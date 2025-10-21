(ns muutos.sql-client
  "SQL client.

  Suitable for diagnostics, debugging, and low-throughput use cases."
  (:require [cognitect.anomalies :as-alias anomalies]
            [muutos.codec.bin :as bin]
            [muutos.error :as-alias error]
            [muutos.impl.anomaly :refer [anomaly!]]
            [muutos.impl.client :as client]
            [muutos.impl.connection :as connection]
            [muutos.impl.data-row :as data-row]
            [muutos.impl.hook :as hook]
            [muutos.impl.lockable :refer [Lockable with-lock]]
            [muutos.impl.type :as type])
  (:import (java.lang AutoCloseable)
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

        :key-fn (fn [_table-oid attr-name] (keyword attr-name))"
  ^AutoCloseable [& {:keys [^String host ^long port user password database replication log  oid-fn]
                     :or {oid-fn (constantly nil)
                          log (constantly nil)}
                     :as options}]
  (let [options (merge default-options options)

        aux-client (when-not (= :aux (:client-type options))
                     (let [aux-options (-> options
                                         (select-keys [:host :port :user :password :database :trust-managers])
                                         (assoc :client-type :aux))]
                       (connect aux-options)))

        -lock (ReentrantLock.)
        connection (connection/open (:host options) (:port options))
        session (client/start-session connection options)

        client
        (reify
          client/Client

          (options [_] options)

          (log [_ level event-name data]
            (log level event-name data))

          (connection [_] connection)

          (send [this message]
            (if-not (connection/closed? connection)
              (connection/write connection message)
              (anomaly! "Disconnected from server; can't send" ::anomalies/incorrect {:reason :disconnected})))

          (recv [_]
            (connection/read connection {}))

          (oid [_ x]
            (or (oid-fn x) (type/oid x)))

          (aux [_] aux-client)

          Lockable
          (lock [_] -lock)

          AutoCloseable
          (close [this]
            (AutoCloseable/.close connection)
            (some-> aux-client AutoCloseable/.close)))]

    (hook/on-shutdown (AutoCloseable/.close client))

    (with-meta client (assoc session :options (select-keys options [:key-fn])))))

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
                        (client/send client {:type :parse :oids oids :statement unnamed-statement :query q})
                        (client/send client {:type :describe :target :statement :name unnamed-statement})
                        (client/send client {:type :bind :statement unnamed-statement :portal unnamed-portal :parameters parameters})
                        (client/send client {:type :execute :portal unnamed-portal :max-rows 0})
                        (recur (inc n) (rest qs)))
                      n))]

      (when (pos? n)
        (client/send client {:type :sync})

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
                                        (client/send client {:type :copy-done})
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
                                            data-row (data-row/parse attrs tuples :query-fn (fn [qvec] (eq (client/aux client) qvec)) :key-fn key-fn :format :bin)]
                                        (recur command-complete row-description (conj! data data-row) ex)))))
                                (catch Throwable ex
                                  ;; If the event loop throws e.g. an OutOfMemoryError, let it crash to
                                  ;; prevent the event loop from getting out of sync.
                                  (if (= ::error/server-error (-> ex ex-data :kind))
                                    (throw ex)
                                    (do
                                      (AutoCloseable/.close client)
                                      (anomaly! "Fatal error when reading server response; closing client to prevent protocol desynchronization" ::anomalies/fault (ex-data ex) ex)))))]
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
  ,,,)

#_{:clj-kondo/ignore [:unused-binding]}
(defn sq
  "Given a client and a query string, run a simple query.

  Simple queries do not support parameter placeholders. They can only be used
  with trusted inputs."
  ([client q] (sq client q {}))
  ([client q opts]
   (with-lock client
     (client/send client {:type :simple-query :query q})
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
                 (client/send client {:type :copy-done})
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
                     data-row (data-row/parse attrs tuples :query-fn (fn [qvec] (eq (client/aux client) qvec)) :key-fn key-fn :format :txt)]
                 (recur command-complete row-description (conj! data data-row) ex)))))
         (catch Throwable ex
           ;; If the event loop throws e.g. an OutOfMemoryError, let it crash to
           ;; prevent the event loop from getting out of sync.
           (if (= ::error/server-error (-> ex ex-data :kind))
             (throw ex)
             (do
               (AutoCloseable/.close client)
               (anomaly! "Fatal error when reading server response; closing client to prevent protocol desynchronization" ::anomalies/fault (ex-data ex) ex)))))))))

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
