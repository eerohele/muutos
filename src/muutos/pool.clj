(ns muutos.pool
  (:require [cognitect.anomalies :as-alias anomalies]
            [muutos.impl.anomaly :refer [anomaly!]]
            [muutos.impl.client :as client]
            [muutos.impl.connection :as connection :refer [Connection]]
            [muutos.impl.lockable :as lockable :refer [Lockable with-lock]]
            [muutos.impl.type :as type])
  (:import (clojure.lang Ratio)
           (java.lang AutoCloseable)
           (java.time Duration)
           (java.util.concurrent.atomic AtomicBoolean)
           (java.util.concurrent BlockingDeque ConcurrentHashMap LinkedBlockingDeque TimeUnit)
           (java.util.concurrent.locks ReentrantLock)))

(set! *warn-on-reflection* true)

(defn fill
  ^AutoCloseable [& {:keys [oid-fn log capacity lease-timeout validate-after-idle]
                     :or {oid-fn (constantly nil)
                          log (constantly nil)
                          capacity 10
                          lease-timeout (Duration/ofSeconds 10)
                          validate-after-idle (Duration/ofMillis 500)}
                     :as options}]
  (let [options (merge connection/default-options options)
        connection-lock (ReentrantLock.)
        pool-id (random-uuid)
        q (LinkedBlockingDeque. ^int capacity)
        closed? (AtomicBoolean. false)
        ^ConcurrentHashMap access-book (ConcurrentHashMap. (int capacity))
        recently-used? (fn [connection]
                         (let [last-accessed-nanos (ConcurrentHashMap/.getOrDefault access-book connection 0)]
                           (< (- (System/nanoTime) last-accessed-nanos) (Duration/.toNanos validate-after-idle))))
        replace (fn [connection]
                  (AutoCloseable/.close connection)
                  ;; FIXME: Retry with backoff & jitter
                  (connection/open options))]

    ;; Fill up the pool with the requested number of connections.
    (BlockingDeque/.addAll q
      (into []
        (map (fn [_] (connection/open options)))
        (range capacity)))

    ^{:type ::pool}
    (reify

      client/Client
      (id [_] pool-id)
      (options [_] options)
      (log [_ level event-name data] (log level event-name data))
      (connection ^AutoCloseable [_]
        (if (AtomicBoolean/.get closed?)
          (anomaly! "Pool closed" ::anomalies/unavailable)
          (let [candidate (or (BlockingDeque/.pollFirst q (Duration/.toNanos lease-timeout) TimeUnit/NANOSECONDS)
                            (anomaly! "Timed out leasing connection from pool" ::anomalies/unavailable))
                this (if (or (recently-used? candidate) (connection/alive? candidate))
                       candidate
                       (replace candidate))]
            ;; FIXME: Unnecessary allocation just to get with-open?
            (reify
              Connection
              (certificate-hash [_] (connection/certificate-hash this))
              (secure? [_] (connection/secure? this))
              (secure [_ _] (anomaly! "Cannot secure open connection" ::anomalies/incorrect))
              (closed? [_] (connection/closed? this))
              (enqueue [_ msg] (connection/enqueue this msg))
              (read [_ state] (connection/read this state))
              (write [_ message] (connection/write this message))
              (flush [_] (connection/flush this))
              (alive? [_] (connection/alive? this))
              (tx-status [_] (connection/tx-status this))

              AutoCloseable
              (close [_]
                ;; FIXME: What if the queue is full when we attempt to put the
                ;; client back into the queue? Probably kill this connection?
                (ConcurrentHashMap/.put access-book this (System/nanoTime))
                (BlockingDeque/.offerFirst q this))))))
      (oid [_ x] (or (oid-fn x) (type/oid x)))
      (aux [_ f]
        (let [aux-options (select-keys options [:host :port :user :password :database :trust-managers :connect-timeout])]
          ;; FIXME?
          (with-open [pg (fill (assoc aux-options :capacity 1))]
            (f pg))))
      (stats [_]
        {:state (if (AtomicBoolean/.get closed?) :defunct :extant)
         :busy (let [size (BigInteger/valueOf (BlockingDeque/.size q))
                     cap (BigInteger/valueOf capacity)]
                 (Ratio. (BigInteger/.subtract cap size) cap))})

      Lockable
      (lock [_] connection-lock)

      AutoCloseable
      (close [_]
        (when-not (AtomicBoolean/.get closed?)

          (dotimes [_ capacity]
            ;; FIXME: Separate timeout for this?
            (let [connection (BlockingDeque/.pollLast q (Duration/.toNanos lease-timeout) TimeUnit/NANOSECONDS)]
              (some-> connection AutoCloseable/.close)))

          (AtomicBoolean/.set closed? true))))))

(comment
  (with-lock this
    (when-not (= :idle (connection/tx-status this))
      (connection/enqueue this {:type :simple-query
                                :query "ROLLBACK"})

      (connection/flush this)

      ;; FIXME
      (assert (= {:type :command-complete
                  :command "ROLLBACK"}
                (connection/read this {})))

      (assert (= {:type :ready-for-query
                  :tx-status :idle}
                (connection/read this {})))))
  ,,,)

(comment
  (def pg (fill :port 5437)) #_(.close pg)

  (client/stats pg)

  (require '[muutos.sql-client :as sql])

  (sql/sq pg "SELECT 1 AS n")
  (dotimes [_ 10] (sql/eq pg ["SELECT $1 AS n" 1]))

  (def f1 (future (sql/eq pg ["SELECT pg_sleep($1)" 10])))
  (def f2 (future (sql/eq pg ["SELECT pg_sleep($1)" 10])))

  (def q (sql/lq "SELECT $1 || $2 AS s"))

  (into [] (q pg "a" "b"))

  (require '[muutos.test.disruptor :as disruptor])
  (def proxy (disruptor/proxy :bind-port 10080 :connect-port 5437))
  (.close proxy)

  (def pg (fill :port 10080))

  (def f (future
           (sql/eq pg ["SELECT $1 AS n" 1])))

  (deref f)

  (require '[next.jdbc :as jdbc])
  (require '[next.jdbc.connection :as jdbc.connection])
  (import '(com.zaxxer.hikari HikariDataSource))

  (def jdbc-pool
    (jdbc.connection/->pool HikariDataSource
      {:dbtype "postgres"
       :dbname "postgres"
       :username "postgres"
       :password "postgres"
       :port 5437
       :minimumIdle 1
       :maximumPoolSize 1
       :dataSourceProperties {:prepareThreshold 1}}))

  (.close jdbc-pool)

  (dotimes [_ 100] (jdbc/execute! jdbc-pool ["SELECT ? AS n" 42]))

  (stats pool)
  ,,,)

(defmethod print-method ::pool [o ^java.io.Writer w]
  (.write w (format "<<< Pool %s >>>" (client/stats o))))
