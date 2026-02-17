(ns muutos.subscriber
  "Subscribe to a PostgreSQL logical replication stream."
  (:require [cognitect.anomalies :as-alias anomalies]
            [clojure.string :as string]
            [muutos.impl.anomaly :refer [anomaly!]]
            [muutos.impl.client :as client]
            [muutos.impl.connection :as connection]
            [muutos.impl.hook :as hook]
            [muutos.impl.lockable :refer [Lockable]]
            [muutos.impl.subscriber :as impl]
            [muutos.impl.thread :as thread]
            [muutos.sql-client :as sql-client])
  (:import (clojure.lang IDeref IPending)
           (java.lang AutoCloseable)
           (java.net SocketException)
           (java.time Duration)
           (java.util.concurrent.atomic AtomicReference)
           (java.util.concurrent ArrayBlockingQueue BlockingQueue Executors ExecutorService FutureTask TimeUnit)
           (java.util.concurrent.locks ReentrantLock)))

(set! *warn-on-reflection* true)

(defn ^:private lsn->hex-string
  "Given a 64-bit numeric (long) representation of a PostgreSQL log sequence
  number (long), return its hexadecimal string representation."
  [^long n]
  (let [byte-offset (bit-shift-right n 32)
        segment (long n)]
    (format "%X/%X" byte-offset segment)))

(defn ^:private hex-string->lsn
  "Given a hexadecimal string representation of a PostgreSQL log sequence
  number, return its 64-bit numeric (long) representation."
  [^String s]
  (let [[segment byte-offset] (string/split s #"/")
        segment (Long/parseLong segment 16)
        offset (Long/parseLong byte-offset 16)]
    (bit-or (bit-shift-left segment 32) offset)))

(comment
  (lsn->hex-string 0)
  (lsn->hex-string 0x16B3748)
  (hex-string->lsn (lsn->hex-string 23803720))

  (require '[clojure.test.check :as tc])
  (require '[clojure.test.check.generators :as gen])
  (require '[clojure.test.check.properties :as prop])

  (tc/quick-check 1000000
    (prop/for-all [n (gen/large-integer* {:min 0 :max Long/MAX_VALUE})]
      (= n (-> n lsn->hex-string hex-string->lsn))))
  ,,,)

(defn ^:private read-loop
  [connection ^AtomicReference replicating? log ^BlockingQueue recvq lsn-flush-state initial-state handler]
  (loop [state initial-state]
    (let [[recur? state]
          (try
            (let [{:keys [type] :as message} (connection/read connection state)]
              (case type
                :error
                (let [ex (message :ex)]
                  ;; An exception might be thrown either during replication or when running
                  ;; the startup query. In the former case, log the error. In the latter case,
                  ;; put the error into recvq to return it to the caller.
                  (log :error ::server-error {:ex (Throwable->map ex)})
                  (.put recvq message)
                  ;; If we're replicating, immediately throw the error so that dereferencing
                  ;; the subscriber throws.
                  ;;
                  ;; If we're in the middle of processing the startup query, recur to ultimately
                  ;; hand the exception to the caller.
                  (if (AtomicReference/.get replicating?)
                    (throw ex)
                    [true state]))

                :copy-data
                (case (-> message :data :type)
                  :primary-keep-alive-message
                  (do
                    (log :debug ::recv-keep-alive-message {:message message})
                    (when (-> message :data :reply-asap?)
                      (let [lsn (-> lsn-flush-state deref :flushed-lsn)]
                        (log :debug ::reply-to-keep-alive-message {:lsn lsn})
                        (impl/send-status-update connection lsn)))
                    [true state])

                  :wal-data
                  (do
                    (log :trace ::recv-wal-message {:message message})
                    (let [new-state (impl/handle-wal-message state handler (-> message :data :section))]
                      [true new-state]))

                  (do
                    (log :warn ::unknown-copy-data-message {:type type :message message})
                    [true state]))

                (do
                  ;; Progress the event loop initialized by the startup query.
                  (.put recvq message)
                  [true state])))
            (catch SocketException ex
              (when (AtomicReference/.get replicating?)
                (throw ex)))
            (catch Exception ex
              (when-not (= "Closed by interrupt" (ex-message ex))
                (log :error ::read-loop-error {:ex (Throwable->map ex)}))
              (throw ex)))]
      (if recur?
        (recur state)
        :ok))))

(defn flow-controlling-executor
  "Return a single-thread `java.util.concurrent.ExecutorService` that runs off
  a non-fair, bounded queue and exerts backpressure when saturated.

  Options:

  - `:work-queue` (`java.util.concurrent.BlockingQueue`, default: `(ArrayBlockingQueue. 256 false)`)

    A `java.util.concurrent.BlockingQueue` to use as the executor work queue."
  ^ExecutorService [& {:keys [work-queue] :as options}]
  (let [work-queue (or work-queue (ArrayBlockingQueue. 256 false))]
    (thread/executor
      :core-pool-size 1
      :max-pool-size 1
      :rejection-policy (thread/flow-control-policy work-queue (select-keys options [:timeout]))
      :thread-factory (thread/factory "me.flowthing.muutos/handler")
      :work-queue work-queue)))

(defn- default-executor-close-fn [executor]
  (ExecutorService/.close executor))

(defn connect
  "Given the name of a logical replication slot (ident or string) and options,
  subscribe to a PostgreSQL logical replication stream.

  Options:

  - `:publications` (set, default: `#{}`)

    A set of [publication names](https://www.postgresql.org/docs/current/sql-createpublication.html) to subscribe to.

    To create a publication, use e.g. `muutos.sql-client/eq`.

  - `:handler` (fn, default: `(constantly nil)`)

    A function with two arities, a 1-arg and 2-arg arity:

        (fn ([msg] ...) ([msg ack] ...))

    The 1-arg arity receives a Clojure map that describes a PostgreSQL
    logical replication message.

    The 2-arg arity receives the same and a 0-arg fn that you must call to
    acknowledge a database transaction as having been processed.

  - `:executor` (`java.util.concurrent.ExecutorService`, default: `(flow-controlling-executor)`)

    A `java.util.concurrent.ExecutorService` that the subscriber uses to
    execute the handler function (see `:handler`).

  - `:executor-close-fn` (fn, default: see doc)

    A fn of one arg that the subscriber calls after closing, passing it the
    handler function executor (see `:executor`).

    The default implementation shuts down the executor and awaits for
    its termination indefinitely.

    **Note**: If you shut down the executor immediately (using `.shutdownNow`),
    it is possible that the executor shuts down before the handler function can
    inform Muutos that it has successfully processed a transaction. This results
    in PostgreSQL re-sending Muutos that transaction upon resumption.

  - `:start-lsn` (long or string, default: 0)

    The log sequence number to start replicating from. 0 means the oldest
    transaction available in the replication slot.

  - `:protocol-version` (long, default: 2)

     [pgoutput protocol version](https://www.postgresql.org/docs/current/protocol-logical-replication.html).

  - `:log` (fn, default: `(constantly nil)`)

    A logging function with this signature:

        (fn [level event data] ...)

  - `:trust-managers` (coll of `javax.net.ssl.TrustManager`, default: `nil`)

    The trust managers to use when encrypting client/server communication
    using TLS.

    Use `nil` to use the default implementation built into your JVM
    distribution.

    See the `muutos.trust-manager` namespace for a set of pre-defined trust
    managers.

  - `:ack-interval` (`java.time.Duration`, default: `(Duration/ofSeconds 10)`)

    The interval at which to send acknowledgement messages to the PostgreSQL
    server.

    An acknowledgement message contains the last log sequence number (LSN)
    the subscriber has successfully processed. An ack message tells PostgreSQL
    that it can delete entries in its logical replication stream through the
    given LSN.

    The default value (10 seconds) is the same as the default value of the
    PostgreSQL `wal_receiver_status_interval` parameter, which specifies the
    frequency at which a standby PostgreSQL server sends status updates to the
    primary when replicating.

  - `:key-fn` (fn, default: `(fn [_table-oid attr-name] attr-name)`)

    A fn that, given a PostgreSQL table OID (integer) and an attribute name
    (string), returns a transformed attribute name.

    The default implementation returns the attribute name as is.

    The most common use case for this function is to transform attribute names
    into keywords. For example:

        :key-fn (fn [_table-oid attr-name] (keyword attr-name))

  - `:messages?` (boolean, default: `true`)

    If true, tell PostgreSQL to send messages emitted using the
    `pg_logical_emit_message` PostgreSQL function.

  - `:streaming` (boolean or `:parallel`, default: false)

    Whether to support [large transaction streaming](https://www.postgresql.org/docs/current/logicaldecoding-streaming.html).

    Using `:parallel` requires `:protocol-version` 4 or higher."
  ^Thread [slot-name
           & {:as options
              :keys [publications handler executor executor-close-fn start-lsn log ^Duration ack-interval messages?]
              :or {publications #{}
                   handler (constantly nil)
                   executor-close-fn default-executor-close-fn
                   start-lsn 0
                   log (constantly nil)
                   messages? true
                   ack-interval (Duration/ofSeconds 10)}}]
  ;; Allow passing in custom muutos.impl.connection/Connection to simulate network errors.
  (let [{:keys [key-fn] :as options} (merge client/default-options
                                       {:protocol-version 2
                                        :streaming false
                                        :replication :database}
                                       options)
        connection (or (:connection options) (connection/open (:host options) (:port options)))
        reentrant-lock (ReentrantLock.)
        ;; A marker that shows whether we've successfully completed the query
        ;; the starts logical replication.
        replicating? (AtomicReference. false)
        closed? (AtomicReference. false)
        lsn-flush-state (atom {:unflushed-lsn nil :flushed-lsn start-lsn})
        recvq (ArrayBlockingQueue. 1)
        exec-handler (or executor (flow-controlling-executor))
        exec-lsn-flusher (Executors/newSingleThreadScheduledExecutor (thread/factory "me.flowthing.muutos/lsn-flusher"))
        _ (client/start-session connection (assoc options :application_name "Muutos Subscriber"))

        exit-signal (promise)

        flush-lock (Object.)
        close-lock (Object.)

        flush-lsn
        (fn []
          (locking flush-lock
            (let [{:keys [unflushed-lsn flushed-lsn]} @lsn-flush-state
                  ;; If there's no unflushed lsn, send the last flushed LSN
                  ;; (as a keepalive mechanism).
                  lsn (or unflushed-lsn flushed-lsn)]
              (try
                (log :debug ::flush-lsn {:lsn lsn})
                (impl/send-status-update connection lsn)
                (swap! lsn-flush-state assoc :unflushed-lsn nil :flushed-lsn lsn)
                (catch Exception ex
                  (log :error ::flush-lsn-error {:ex (Throwable->map ex)})
                  ;; If the subscriber can't acknowledge the transaction as
                  ;; having been processed, send the exit signal.
                  (deliver exit-signal ex))))))

        sql-client (or (:sql-client options)
                     (sql-client/connect
                       (-> options (assoc :client-type :aux) (dissoc :replication))))

        ^FutureTask task
        (FutureTask.
          (^:once fn* []
           (try
             (let [ret (read-loop connection replicating? log recvq lsn-flush-state
                         {:streaming (:streaming options)
                          :protocol-version (:protocol-version options)
                          :sql-client sql-client
                          :key-fn key-fn
                          :ack-fn (fn [lsn] (swap! lsn-flush-state assoc :unflushed-lsn lsn))}
                         (fn
                           ([msg]
                            (thread/execute exec-handler
                              (try
                                (handler msg)
                                (catch Throwable ex
                                  (deliver exit-signal ex)))))
                           ([msg ack]
                            (thread/execute exec-handler
                              (try
                                (handler msg ack)
                                (catch Throwable ex
                                  (deliver exit-signal ex)))))))]
               (deliver exit-signal ret))
             (catch Throwable ex
               (deliver exit-signal ex)))))

        _
        (doto (Thread/ofVirtual) (.start task))

        client
        (reify
          client/Client
          (options [_] options)

          (log [_ level event-name data]
            (log level event-name data))

          (connection [_] connection)

          (send [_ message]
            (if-not (connection/closed? connection)
              (connection/write connection message)
              (anomaly! "Disconnected from server; can't send" ::anomalies/incorrect {:reason :disconnected})))

          (recv [_] (.take recvq))

          (oid [_ _]
            (anomaly! "Not supported" ::anomalies/unsupported))

          Lockable
          (lock [_] reentrant-lock)

          AutoCloseable
          (close [_]
            (locking close-lock
              (when-not (AtomicReference/.get closed?)
                ;; First, wait for the handler function to complete. If we don't do
                ;; this, we might shut down the handler function before it has the
                ;; chance to request an LSN flush.
                (try
                  (executor-close-fn exec-handler)
                  (catch InterruptedException ex
                    (log :error ::handler-shutdown-interrupted {:ex (Throwable->map ex)})
                    (Thread/.interrupt (Thread/currentThread))))

                ;; If there's a pending scheduled LSN flush, force it.
                (flush-lsn)
                (ExecutorService/.shutdownNow exec-lsn-flusher)

                (log :info ::last-flushed-lsn (lsn->hex-string (-> lsn-flush-state deref :flushed-lsn)))

                (AtomicReference/.set replicating? false)
                (FutureTask/.cancel task true)
                (AutoCloseable/.close connection)
                (AutoCloseable/.close sql-client)

                (when-not (realized? exit-signal)
                  (deliver exit-signal :ok))

                (AtomicReference/.set closed? true))))

          IPending
          (isRealized [_]
            (realized? exit-signal))

          IDeref
          (deref [this]
            (let [ret-or-ex (deref exit-signal)]
              (if (instance? Throwable ret-or-ex)
                (do
                  (AutoCloseable/.close this)
                  (throw ret-or-ex))
                ret-or-ex))))]

    (try
      (impl/run-start-query client
        :slot-name slot-name
        :protocol-version (:protocol-version options)
        :start-lsn (if (number? start-lsn) (lsn->hex-string start-lsn) start-lsn)
        :publication-names publications
        :messages? messages?
        :streaming (:streaming options))

      (AtomicReference/.set replicating? true)

      (.scheduleWithFixedDelay exec-lsn-flusher
        flush-lsn
        (.toNanos ack-interval)
        (.toNanos ack-interval)
        TimeUnit/NANOSECONDS)

      (hook/on-shutdown (AutoCloseable/.close client))

      client

      ;; If there's an error on startup, clean up, then rethrow.
      (catch Exception ex
        (AutoCloseable/.close client)
        (throw ex)))))
