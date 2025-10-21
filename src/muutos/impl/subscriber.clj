(ns ^:no-doc muutos.impl.subscriber
  (:refer-clojure :exclude [send])
  (:require [clojure.string :as string]
            [cognitect.anomalies :as-alias anomalies]
            [muutos.impl.connection :as connection]
            [muutos.impl.data-row :as data-row]
            [muutos.impl.data-type :as data-type]
            [muutos.impl.time :as time]
            [muutos.sql-client :as sql-client :refer [sq]]))

(set! *warn-on-reflection* true)

(def ^:private start-replication-query
  "START_REPLICATION SLOT \"%s\"
   LOGICAL %s (
    proto_version '%d',
    publication_names '%s',
    streaming '%s',
    binary 'true',
    messages '%b'
  )")

(defn ^:private standby-status-update-message
  [{:keys [written-lsn flushed-lsn applied-lsn reply-asap?]}]
  (let [system-clock (time/microseconds-since-postgres-epoch)
        standby-status-update {:type :standby-status-update
                               :written-lsn written-lsn
                               :flushed-lsn flushed-lsn
                               :applied-lsn applied-lsn
                               :system-clock system-clock
                               :reply-asap? reply-asap?}]
    {:type :copy-data
     :data standby-status-update}))

(defn send-status-update
  [connection lsn & {:keys [reply-asap?] :or {reply-asap? false}}]
  (connection/write connection
    (standby-status-update-message
      {:written-lsn lsn
       :flushed-lsn lsn
       :applied-lsn lsn
       :reply-asap? reply-asap?})))

(defn ^:private resolve-state [state oid]
  (let [keys (-> state :oid->keys (get oid))
        attr-defs (-> state :oid->attributes (get oid))
        schema (-> state :oid->schema (get oid))
        table (-> state :oid->table (get oid))]
    {:keys keys
     :attr-defs attr-defs
     :schema schema
     :table table}))

(defn ^:private attributes->keys [key-fn relation attributes]
  (loop [attributes attributes
         keys (transient [])]
    (if-some [attribute (nth attributes 0)]
      (if (contains? (:flags attribute) :key)
        (recur (next attributes) (conj! keys (key-fn relation (attribute :name))))
        (recur (next attributes) keys))
      (persistent! keys))))

(defmulti handle-wal-message
  (fn [_state _handler section] (:type section)))

(defmethod handle-wal-message :begin [state handler msg]
  (handler msg)
  (assoc state :tx-state :complete))

(defmethod handle-wal-message :type [{:keys [sql-client] :as state} handler {:keys [oid] :as msg}]
  (data-type/install-decoder! (fn [qvec] (sql-client/eq sql-client qvec)) oid)
  (handler msg)
  state)

(defmethod handle-wal-message :relation
  [{:keys [key-fn] :as state} handler {:keys [oid namespace relation attributes] :as msg}]
  (let [new-state (-> state
                    (assoc-in [:oid->schema oid] namespace)
                    (assoc-in [:oid->table oid] relation)
                    (assoc-in [:oid->attributes oid] attributes)
                    (assoc-in [:oid->keys oid] (attributes->keys key-fn relation attributes)))]
    (handler msg)
    new-state))

(defmethod handle-wal-message :truncate
  [{:keys [oid->schema oid->table] :as state} handler {:keys [oids] :as msg}]
  (handler
    (->
      msg
      (dissoc :oids)
      (assoc :targets
        (mapv (fn [oid] {:schema (get oid->schema oid)
                         :table (get oid->table oid)})
          oids))))
  state)

(defmethod handle-wal-message :insert
  [{:keys [key-fn] :as state} handler {:keys [oid new-tuple] :as msg}]
  (let [{:keys [attr-defs schema table]} (resolve-state state oid)]
    (handler
      (cond-> msg
        true (dissoc :new-tuple :oid)
        new-tuple (assoc :new-row (data-row/parse attr-defs new-tuple
                                    :key-fn key-fn
                                    :format :bin))
        schema (assoc :schema schema)
        table (assoc :table table)))
    state))

(defmethod handle-wal-message :update
  [{:keys [key-fn] :as state} handler {:keys [oid old-tuple new-tuple] :as msg}]
  (let [{:keys [keys attr-defs schema table]} (resolve-state state oid)]
    (handler
      (cond-> msg
        true (dissoc :new-tuple :old-tuple :oid)
        keys (assoc :keys keys)
        new-tuple (assoc :new-row (data-row/parse attr-defs new-tuple
                                    :key-fn key-fn
                                    :format :bin))
        old-tuple (assoc :old-row (data-row/parse attr-defs old-tuple
                                    :key-fn key-fn
                                    :format :bin))
        schema (assoc :schema schema)
        table (assoc :table table)))
    state))

(defmethod handle-wal-message :delete
  [{:keys [key-fn] :as state} handler {:keys [oid old-tuple] :as msg}]
  (let [{:keys [keys attr-defs schema table]} (resolve-state state oid)]
    (handler
      (cond-> msg
        true (dissoc :old-tuple :oid)
        keys (assoc :keys keys)
        old-tuple (assoc :old-row
                    (data-row/parse attr-defs old-tuple
                      :key-fn key-fn
                      :format :bin))
        schema (assoc :schema schema)
        table (assoc :table table)))
    state))

(defmethod handle-wal-message :commit
  [{:keys [ack-fn] :as state} handler {:keys [tx-end-lsn] :as msg}]
  (handler msg (partial ack-fn tx-end-lsn))
  (dissoc state :tx-state))

(defmethod handle-wal-message :stream-start
  [state handler msg]
  (handler msg)
  (assoc state :tx-state :in-progress))

(defmethod handle-wal-message :stream-commit
  [{:keys [ack-fn] :as state} handler {:keys [tx-end-lsn] :as msg}]
  (handler msg (partial ack-fn tx-end-lsn))
  (dissoc state :tx-state))

(defmethod handle-wal-message :stream-abort
  [{:keys [ack-fn] :as state} handler {:keys [abort-lsn] :as msg}]
  (if abort-lsn
    (handler msg (partial ack-fn abort-lsn))
    (handler msg))
  (dissoc state :tx-state))

(defmethod handle-wal-message :default
  [state handler msg]
  (handler msg)
  state)

(defn run-start-query
  "Given a client and options, run a PostgreSQL query that starts logical
  replication on the client.

  Options:

    :slot-name
      The name of the logical replication slot to start replication on.

      Must exist before starting replication.

    :start-lsn
      The log sequence number (LSN) to start replication from.

    :protocol-version (long, default: 2)
      pgoutput protocol version.

    :publication-names (set, default: #{})
      A set of PostgreSQL publication[1] names for which to receive changes.

    :streaming (boolean or :parallel, default: true)
      Instruction on streaming in-process transactions.

    :messages? (boolean, default: true)
      Send messages clients send to PostgreSQL using `pg_logical_emit_message`.

   See also: https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS

   [1]: https://www.postgresql.org/docs/current/sql-createpublication.html"
  [client & {:keys [slot-name start-lsn protocol-version publication-names streaming messages?]
             :or {publication-names #{} protocol-version 2}}]
  (let [names (string/join \, (mapv name publication-names))
        slot (name slot-name)
        streaming (if (ident? streaming) (name streaming) streaming)
        q (format start-replication-query slot start-lsn protocol-version names streaming messages?)]
    (sq client q)))
