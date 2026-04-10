(ns muutos.sql-client
  "SQL client.

  Suitable for diagnostics, debugging, and low-throughput use cases."
  (:require [cognitect.anomalies :as-alias anomalies]
            [clojure.core :as core]
            [muutos.codec.bin :as bin]
            [muutos.error :as-alias error]
            [muutos.impl.anomaly :refer [anomaly!]]
            [muutos.impl.client :as client]
            [muutos.impl.connection :as connection]
            [muutos.impl.data-row :as data-row]
            [muutos.impl.hook :as hook]
            [muutos.impl.lockable :refer [Lockable with-lock]]
            [muutos.impl.type :as type])
  (:import (clojure.lang IFn IReduceInit Seqable)
           (java.lang AutoCloseable)
           (java.util.concurrent.locks ReentrantLock)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def ^:private default-options
  (assoc client/default-options
    :application_name "Muutos SQL Client"
    :key-fn (fn [_table-oid attr-name] attr-name)))

#_{:clj-kondo/ignore [:unused-binding]}
(defn connect
  "Connect to a PostgreSQL database.

  Options:

  - `:host` (string, default: `\"localhost\"`)

    The host name of the PostgreSQL server to connect to.

  - `:port` (long, default: `5432`)

    Port number.

  - `:user` (string, default: `\"postgres\"`)

    PostgreSQL user.

  - `:password` (string, default: `\"postgres\"`)

    PostgreSQL user password.

  - `:database` (string, default: `\"postgres\"`)

    PostgreSQL database.

  - `:replication` (boolean/ident, default: `false`)

    The replication mode to use for the connection. Use `:database` to execute
    replication commands (e.g. `CREATE_REPLICATION_SLOT`).

  - `:oid-fn` (fn, default: `(constantly nil)`)

    A fn that, given `x`, must return the [PostgreSQL data type OID](https://github.com/postgres/postgres/blob/d3d0983169130a9b81e3fe48d5c2ca4931480956/src/include/catalog/pg_type.dat)
    (an integer) associated with `x`, or `nil`.

    Use this option to teach Muutos to tell PostgreSQL the data types of
    extended query parameters. For example, to teach Muutos to tell
    PostgreSQL that it should interpret persistent Clojure collections as
    `jsonb` data:

    ```clojure
    ;; 3802 is the PostgreSQL data type OID for JSONB.
    :oid-fn (fn [x] (when (instance? IPersistentCollection x) 3802))
    ```

    If `oid-fn` returns `nil`, Muutos falls back to the built-in implementation.
    If the built-in implementation does not recognize the class of `x`, Muutos
    uses OID 0 to tell PostgreSQL that the data type is unspecified.

  - `:trust-managers` (coll of `javax.net.ssl.TrustManager`, default: `nil`)

    The trust managers to use when encrypting client/server communication using TLS.

    Use `nil` to use the default implementation built into your JVM
    distribution.

    See the `muutos.trust-manager` namespace for a set of pre-defined trust managers.

  - `:key-fn` (fn, default: `(fn [_table-oid attr-name] attr-name)`)

    A fn that, given a PostgreSQL table OID (`int`) and an attribute name
    (`string`), returns a transformed attribute name.

    The default implementation returns the attribute name as is.

    The most common use case for this function is to transform attribute
    names into keywords. For example:

        :key-fn (fn [_table-oid attr-name] (keyword attr-name))

  - `:socket-timeout` (`java.time.Duration`, default: PT0S)

    The `SO_TIMEOUT` value of the of the socket connection. A zero duration
    means infinite timeout.

  - `:connect-timeout` (`java.time.Duration`, default: PT0S)

    TCP connection timeout value. A zero duration means infinite timeout."
  ^AutoCloseable [& {:keys [^String host ^long port user password database replication log  oid-fn]
                     :or {oid-fn (constantly nil)
                          log (constantly nil)}
                     :as options}]
  (let [options (merge default-options options)

        aux-client (when-not (= :aux (:client-type options))
                     (let [aux-options (-> options
                                         (select-keys [:host :port :user :password :database :trust-managers :connect-timeout])
                                         (assoc :client-type :aux))]
                       (connect aux-options)))

        -lock (ReentrantLock.)
        connection (connection/open options)
        session (client/start-session connection options)

        client
        (reify
          client/Client

          (options [_] options)

          (log [_ level event-name data]
            (log level event-name data))

          (connection [_] connection)

          (enqueue [this message]
            (if-not (connection/closed? connection)
              (connection/write connection message)
              (anomaly! "Disconnected from server; can't send" ::anomalies/incorrect {:reason :disconnected})))

          (flush [this]
            (connection/flush connection))

          (recv [_]
            (connection/read connection {}))

          (oid [_ x]
            (or (oid-fn x) (type/oid x)))

          (aux [_] aux-client)

          (closed? [_]
            (connection/closed? connection))

          Lockable
          (lock [_] -lock)

          AutoCloseable
          (close [this]
            (AutoCloseable/.close connection)
            (some-> aux-client AutoCloseable/.close)))]

    (hook/on-shutdown (AutoCloseable/.close client))

    (with-meta client (assoc session :options (select-keys options [:key-fn])))))

(defn ^:private handle-error! [client ex]
  (if (= ::error/server-error (-> ex ex-data :kind))
    (throw ex)
    (do
      (AutoCloseable/.close client)
      (anomaly! "Fatal error when reading server response; closing client to prevent protocol desynchronization" ::anomalies/fault (ex-data ex) ex))))

(def ^:private unnamed-statement "")
(def ^:private unnamed-portal "")

(defn eq
  "Given a client and any number of query vectors, run an extended query.

  To run a [pipeline](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING) of queries, pass more than one query vector."
  [client & qs]
  (with-lock client
    (let [^long n (loop [n 0 qs qs]
                    (if-some [[q & parameters] (first qs)]
                      (let [oids (mapv (fn [parameter] (client/oid client parameter)) parameters)
                            parameters (mapv bin/encode parameters)]
                        ;; FIXME: What if encoding any of these fail? Especially ones user input can affect?
                        (client/enqueue client {:type :parse :oids oids :statement unnamed-statement :query q})
                        (client/enqueue client {:type :describe :target :statement :name unnamed-statement})
                        (client/enqueue client {:type :bind :statement unnamed-statement :portal unnamed-portal :parameters parameters})
                        (client/enqueue client {:type :execute :portal unnamed-portal :max-rows 0})
                        (recur (inc n) (rest qs)))
                      n))]

      (when (pos? n)
        (client/enqueue client {:type :sync})
        (client/flush client)

        (let [{:keys [key-fn]} (client/options client)

              q-count (count qs)

              data
              (loop [i 0 data (transient [])]
                (if (= q-count i)
                  data
                  (let [datum (try
                                (loop [command-complete {}
                                       row-description {}
                                       data (transient [])
                                       ex nil]
                                  (let [response (client/recv client)
                                        type (response :type)]
                                    (case type
                                      ;; If we get a :ready-for-query response here, that means an :execute
                                      ;; yielded a server error.
                                      ;;
                                      ;; We won't get a :command-complete or :portal-suspended in those cases,
                                      ;; so we must handle :ready-for-query here.
                                      :ready-for-query
                                      (if ex
                                        (throw ex)
                                        (with-meta (persistent! data) (dissoc command-complete :type)))

                                      :read-error
                                      (let [response-ex (:ex response)
                                            ex (ex-info (ex-message response-ex) (ex-data response-ex) ex)]
                                        (throw ex))

                                      :error
                                      (recur command-complete row-description data (:ex response))

                                      :notice
                                      (do
                                        (client/log client :info ::server-notice {:notice response})
                                        (recur command-complete row-description data ex))

                                      :copy-data
                                      (recur command-complete row-description (conj! data (response :data)) ex)

                                      :copy-in
                                      (do
                                        ;; Not implemented; immediately send CopyDone to tell Postgres not to
                                        ;; wait for more data.
                                        (client/enqueue client {:type :copy-done})
                                        (client/flush client)
                                        (anomaly! "Not implemented: COPY ... FROM STDIN" ::anomalies/unsupported {:type type}))

                                      ;; The server sends CopyBoth in response to START_REPLICATION. If
                                      ;; START_REPLICATION is successful, it sends no further messages as a
                                      ;; response to START_REPLICATION. If an error occurs (e.g. a
                                      ;; publication doesn't exist), it sends an additional Error response.
                                      :copy-both
                                      (persistent! (conj! data (dissoc response :type)))

                                      (:command-complete :portal-suspended :empty-query)
                                      (if ex
                                        (throw ex)
                                        (with-meta (persistent! data) (dissoc command-complete :type)))

                                      (:parse-complete :bind-complete :close-complete :parameter-description :copy-out :copy-done :no-data)
                                      (recur command-complete row-description data ex)

                                      ;; e.g. SET TIME ZONE 'Europe/Helsinki'.
                                      :parameter
                                      (let [parameter (response :parameter)]
                                        (recur command-complete row-description (conj! data parameter) ex))

                                      :row-description
                                      (recur command-complete response data ex)

                                      :data-row
                                      (let [attrs (row-description :attrs)
                                            tuples (response :tuple)
                                            data-row (data-row/parse attrs tuples {:query-fn (fn [qvec] (eq (client/aux client) qvec)) :key-fn key-fn :format :bin})]
                                        (recur command-complete row-description (conj! data data-row) ex)))))
                                (catch Throwable ex
                                  ;; If the event loop throws e.g. an OutOfMemoryError, let it crash to
                                  ;; prevent the event loop from getting out of sync.
                                  (handle-error! client ex)))]
                    (recur (inc i) (conj! data datum)))))]
          ;; If everything went well, we'll receive :ready-for-query only after
          ;; reading until :command-complete for every input query.
          (loop [data data]
            (let [response (client/recv client)
                  type (response :type)]
              (case type
                :parameter (recur (conj! data response))
                :ready-for-query (cond-> (persistent! data) (= 1 q-count) peek)
                :error (throw (:ex response))
                :read-error (throw (:ex response))))))))))

(comment
  (def pg (connect))
  (AutoCloseable/.close pg)
  (eq pg ["SELECT $1 AS n" 1])
  (eq pg ["SELECT 1"])
  ,,,)

(defmacro ^:private rf-with [rf acc x]
  `(let [acc# ~acc]
     (if (reduced? acc#)
       acc#
       (~rf acc# ~x))))

(defn ^:private close-statement [client stmt-name]
  (client/enqueue client {:type :close :target :statement :name stmt-name})
  (client/enqueue client {:type :sync})
  (client/flush client)

  (loop [ex nil]
    (let [{:keys [type] :as response} (client/recv client)]
      (case type
        :ready-for-query
        (when ex (throw ex))

        :error
        (recur (:ex response))

        ;; Might get one of these when closing the statement before
        ;; executing it.
        (:command-complete :parameter-description :row-description :bind-complete :data-row)
        (recur ex)

        :close-complete
        (recur ex)))))

(defn ^:private execute [client stmt-name parameters]
  (let [encoded-parameters (mapv bin/encode parameters)]
    (client/enqueue client {:type :bind :statement stmt-name :portal unnamed-portal :parameters encoded-parameters})
    (client/enqueue client {:type :execute :portal unnamed-portal :max-rows 0})
    (client/enqueue client {:type :sync})
    (client/flush client)))

(defprotocol ^:private PreparedStatement
  (^:private attributes [this]))

;; FIXME: Make sure prepare (when using IFn) is compatible with transactions!
(defn prepare
  (^AutoCloseable [client q]
   (prepare client q {}))
  (^AutoCloseable [client q {:keys [name oids]}]
   (let [stmt-name (or (some-> name core/name) (str "ms_" (random-uuid)))
         {:keys [key-fn]} (client/options client)
         query-fn (fn [qvec] (eq (client/aux client) qvec))]
     (with-lock client
       (client/enqueue client {:type :parse :statement stmt-name :query q :oids oids})
       (client/enqueue client {:type :describe :target :statement :name stmt-name})
       (client/enqueue client {:type :sync})
       (client/flush client)

       (let [attrs (loop [data []
                          attrs {}
                          ex nil]
                     (let [{:keys [type] :as response} (client/recv client)]
                       (case type
                         :ready-for-query
                         (if ex
                           (handle-error! client ex)
                           attrs)

                         :row-description
                         (recur data (response :attrs) ex)

                         (:parameter-description :no-data :parse-complete)
                         (recur data attrs ex)

                         :error
                         (recur data attrs (response :ex)))))

             execute
             (fn [parameters]
               (reify
                 ;; FIXME: Add Seqable
                 IReduceInit
                 (reduce [_ rf init]
                   (with-lock client
                     (execute client stmt-name parameters)

                     (loop [attrs attrs
                            data init
                            ex nil]
                       (let [{:keys [type] :as response} (client/recv client)]
                         (case type
                           :ready-for-query
                           (cond
                             ;; If PostgreSQL returns an error that indicates that the cached plan
                             ;; has changed (e.g. because someone has executed ALTER TABLE on a
                             ;; table the prepared statement uses), re-prepare the statement, then
                             ;; retry.
                             (-> ex ex-data :error-code (= "0A000"))
                             (do
                               (close-statement client stmt-name)
                               (let [stmt (prepare client q {:name stmt-name :oids oids})]
                                 (execute client stmt-name parameters)
                                 (recur (attributes stmt) data nil)))

                             ex (throw ex)

                             :else (unreduced data))

                           :error
                           (recur attrs data (:ex response))

                           :notice
                           (do
                             (client/log client :info ::server-notice {:notice response})
                             (recur attrs data ex))

                           :copy-data
                           (recur attrs (rf-with rf data (response :data)) ex)

                           :copy-in
                           (do
                             (client/enqueue client {:type :copy-done})
                             (client/flush client)
                             (anomaly! "Not implemented: COPY ... FROM STDIN" ::anomalies/unsupported {:type type}))

                           (:command-complete :empty-query)
                           (if ex
                             (throw ex)
                             (recur attrs data ex))

                           (:bind-complete :copy-out :copy-done :no-data)
                           (recur attrs data ex)

                           :parameter
                           (let [parameter (response :parameter)]
                             (recur attrs (rf-with rf data parameter) ex))

                           :data-row
                           (let [tuples (response :tuple)
                                 data-row (data-row/parse attrs tuples {:query-fn query-fn :key-fn key-fn :format :bin})]
                             (recur attrs (rf-with rf data data-row) ex)))))))))]

         (reify
           IFn
           (invoke [_]
             (execute []))
           (invoke [_ a1]
             (execute [a1]))
           (invoke [_ a1 a2]
             (execute [a1 a2]))
           (invoke [_ a1 a2 a3]
             (execute [a1 a2 a3]))
           (invoke [_ a1 a2 a3 a4]
             (execute [a1 a2 a3 a4]))
           (invoke [_ a1 a2 a3 a4 a5]
             (execute [a1 a2 a3 a4 a5]))
           (invoke [_ a1 a2 a3 a4 a5 a6]
             (execute [a1 a2 a3 a4 a5 a6]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7]
             (execute [a1 a2 a3 a4 a5 a6 a7]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17 a18]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17 a18]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17 a18 a19]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17 a18 a19]))
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17 a18 a19 a20]
             (execute [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17 a18 a19 a20]))
           ;; FIXME: Is this correct?
           (invoke [_ a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17 a18 a19 a20 args]
             (execute (into [a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17 a18 a19 a20] args)))

           (applyTo [_ arglist]
             (execute arglist))

           PreparedStatement
           (attributes [_] attrs)

           AutoCloseable
           (close [_]
             (with-lock client
               (close-statement client stmt-name)))))))))

#_{:clj-kondo/ignore [:unused-binding]}
(defn sq
  "Given a client and a query string, run a simple query.

  Simple queries do not support parameter placeholders. They can only be used
  with trusted inputs."
  ([client q] (sq client q {}))
  ([client q opts]
   (with-lock client
     (client/enqueue client {:type :simple-query :query q})
     (client/flush client)
     (let [{:keys [key-fn]} (client/options client)]
       (try
         (loop [command-complete {}
                row-description nil
                data (transient [])
                ex nil]
           (let [response (client/recv client)
                 type (response :type)]
             (case type
               :ready-for-query
               (if ex
                 (throw ex)
                 (with-meta (persistent! data) (dissoc command-complete :type)))

               :error
               (recur command-complete row-description data (:ex response))

               :read-error
               (some-> response :ex throw)

               :notice
               (do
                 (client/log client :info ::server-notice {:notice response})
                 (recur command-complete row-description data ex))

               :copy-data
               (recur command-complete row-description (conj! data (response :data)) ex)

               :copy-in
               (do
                 ;; Not implemented; immediately send CopyDone to tell Postgres not to
                 ;; wait for more data.
                 (client/enqueue client {:type :copy-done})
                 (client/flush client)
                 (let [ex (ex-info "Not implemented: COPY ... FROM STDIN"
                            {::anomalies/category ::anomalies/unsupported :type type})]
                   (recur command-complete row-description data ex)))

               ;; The server sends CopyBoth in response to START_REPLICATION. If
               ;; START_REPLICATION is successful, it sends no further messages as a
               ;; response to START_REPLICATION. If an error occurs (e.g. a
               ;; publication doesn't exist), it sends an additional Error response.
               :copy-both
               (persistent! (conj! data (dissoc response :type)))

               :command-complete
               (recur response row-description data ex)

               (:empty-query :copy-out :copy-done :no-data)
               (recur command-complete row-description data ex)

               ;; e.g. SET TIME ZONE 'Europe/Helsinki'.
               :parameter
               (let [parameter (response :parameter)]
                 (recur command-complete row-description (conj! data parameter) ex))

               :row-description
               (recur command-complete response data ex)

               :data-row
               (let [attrs (row-description :attrs)
                     tuples (response :tuple)
                     data-row (data-row/parse attrs tuples {:query-fn (fn [qvec] (eq (client/aux client) qvec)) :key-fn key-fn :format :txt})]
                 (recur command-complete row-description (conj! data data-row) ex)))))
         (catch Throwable ex
           ;; If the event loop throws e.g. an OutOfMemoryError, let it crash to
           ;; prevent the event loop from getting out of sync.
           (handle-error! client ex)))))))

(comment
  (def pg (connect))
  (AutoCloseable/.close pg)
  (sq pg "SELECT 1")

  (eq pg
    ["CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"]
    ["CREATE TABLE person (name text, current_mood mood)"])

  (sq pg "INSERT INTO person VALUES ('Moe', 'happy'::mood)")

  (sq pg "SELECT * FROM person")

  (def pg (connect :user "restricted_user" :password "your_password"))
  (sq pg "SELECT * FROM pg_type")
  ,,,)

(defn begin [client & {:keys [isolation-level access-mode deferrable-mode]
                       :or {isolation-level :serializable
                            access-mode :read-write
                            deferrable-mode :not-deferrable}}]
  (sq client
    (str "START TRANSACTION ISOLATION LEVEL "
      (case isolation-level
        :serializable "SERIALIZABLE"
        :repeatable-read "REPEATABLE READ"
        :read-committed "READ COMMITTED"
        :read-uncommitted "READ UNCOMMITTED")
      ","
      (case access-mode
        :read-only "READ ONLY"
        :read-write "READ WRITE")
      ","
      (case deferrable-mode
        :deferrable "DEFERRABLE"
        :not-deferrable "NOT DEFERRABLE"))))

(defn commit [client]
  (sq client "COMMIT"))

(defn rollback [client]
  (sq client "ROLLBACK"))

(defmacro transact
  "Given a client, an optional options map (which must be a compile-time
  literal), and a body, execute the body inside a transaction.

  If the body throws, roll back the transaction, else commit.

  Options:

    :isolation-level (keyword, default: :serializable)
    :access-mode (keyword, default: :read-write)
    :deferrable-mode (keyword, default: :deferrable)

  See https://www.postgresql.org/docs/current/sql-set-transaction.html for more
  information on the options."
  [client & body]
  (let [head (first body)
        options (if (map? head) head {})
        form (if (map? head) (rest body) body)]
    `(let [client# ~client]
       (begin client# ~options)
       (try
         (let [ret# (do ~@form)]
           (commit client#)
           ret#)
         (catch Throwable ex#
           (rollback client#)
           (throw ex#))))))
(comment
  (def pg (connect))
  (.close pg)

  (sq pg "DROP TABLE IF EXISTS t")

  (transact pg
    (sq pg "CREATE TABLE t (a int)")
    (sq pg "INSERT INTO t VALUES (1)")
    (throw (Exception. "Boom!")))

  (transact pg {:isolation-level :repeatable-read
                :deferrable-mode :deferrable}
    (sq pg "CREATE TABLE t (a int)")
    (sq pg "INSERT INTO t VALUES (1)")
    (throw (Exception. "Boom!")))

  (eq pg ["SELECT * FROM t"])

  (def put-t (prepare pg "INSERT INTO t VALUES ($1)"))
  (def ts-by-a (prepare pg "SELECT * FROM t WHERE a = $1"))

  (transact pg
    (into [] (put-t (int 1)))
    (into [] (ts-by-a (int 1)))
    (throw (Exception. "Boom!")))

  (transact pg
    (eq pg ["CREATE TABLE t (a int)"])
    (eq pg ["INSERT INTO t VALUES ($1)" 1])
    (eq pg ["SELECT * FROM t WHERE a = $1" 1]))
  ,,,)

(defmacro ignoring-dupes
  "Execute body.

  If the body throws a `clojure.lang.ExceptionInfo` that indicates a PostgreSQL
  duplicate object, ignore the exception."
  [& body]
  `(try
     (do ~@body)
     (catch clojure.lang.ExceptionInfo ex#
       (when (not= "42710" (-> ex# ex-data :error-code))
         (throw ex#)))))

(def ^:private q-create-slot
  "SELECT pg_create_logical_replication_slot($1, $2, $3)")

(defn create-slot
  "Given a client and a slot name (string), create a (pgoutput) logical
  replication slot with the given name.

  Options:

  - `:temporary?` (boolean, default: `false`)

    If true, do not persist the slot to disk and release it when the current
    session ends."
  [client slot-name & {:keys [temporary?] :or {temporary? false}}]
  (eq client [q-create-slot slot-name "pgoutput" temporary?]))

(def ^:private q-drop-slot
  "SELECT pg_drop_replication_slot($1)")

(defn drop-slot
  "Given a client and a slot name (string), drop the named logical replication
  slot."
  [client slot-name]
  (eq client [q-drop-slot slot-name]))

(def ^:private q-emit-message
  "SELECT pg_logical_emit_message($1, $2, $3, $4)")

(defn emit-message
  "Given a SQL client, a prefix (string), content (string or bytes), and
  options, emit a [logical replication message](https://www.postgresql.org/docs/current/functions-admin.html#PG-LOGICAL-EMIT-MESSAGE).

  Options:

  - `:transactional?` (boolean, default: `true`)

    Iff true, send message as part of the current transaction.

    A non-transactional logical replication message can be useful e.g. when you
    want to create an audit log entry regardless of whether the SQL statement
    succeeds.

  - `:flush?` (boolean, default: `false`)

     Iff true, immediately flush the message into the write-ahead log.

     Has no effect if `:transactional?` is `true`."
  [client prefix content & {:keys [transactional? flush?] :or {transactional? true flush? false}}]
  (eq client [q-emit-message transactional? prefix content flush?]))
