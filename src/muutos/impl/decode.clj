(ns ^:no-doc muutos.impl.decode
  (:require [clojure.string :as string]
            [cognitect.anomalies :as-alias anomalies]
            [muutos.codec.bin :refer [decode-cstring]]
            [muutos.impl.anomaly :refer [anomaly!]]
            [muutos.impl.bb :as bb :refer [int8 int16 int32 int64]]
            [muutos.impl.error-code :as error-code]
            [muutos.impl.sasl :as sasl]
            [muutos.impl.time :as time]
            [muutos.impl.charset :as charset])
  (:import (clojure.lang MapEntry PersistentVector)
           (java.nio ByteBuffer)
           (java.time.temporal ChronoUnit)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

;; ╔═════════════╗
;; ║  UTILITIES  ║
;; ╚═════════════╝

(defn ^:private decode-timestamp [^ByteBuffer bb]
  (let [micros-since-postgres-epoch (int64 bb)]
    (.plus time/postgres-epoch micros-since-postgres-epoch ChronoUnit/MICROS)))

(defn decode-relation-attributes [^ByteBuffer bb]
  (let [size (int16 bb)
        os (object-array size)]
    (loop [i 0]
      (if (= i size)
        (PersistentVector/adopt os)
        (let [flags (int8 bb)
              name (decode-cstring bb)
              data-type-oid (int32 bb)
              type-modifier (int32 bb)
              attr {:flags (cond-> #{} (pos? (bit-and flags 1)) (conj :key))
                    :name name
                    :data-type-oid data-type-oid
                    :type-modifier type-modifier}]
          (aset os i attr)
          (recur (inc i)))))))

(defn try-decode-byte
  "Given a java.nio.ByteBuffer and a byte, read a byte from the buffer.

  If the read byte equals the given byte, return it. Else, reset the buffer to
  its original position and return false."
  [^ByteBuffer bb b]
  (.mark bb)
  (or (= (int8 bb) b) (do (.reset bb) false)))

;; ╔════════════════════════════════╗
;; ║  LOGICAL REPLICATION MESSAGES  ║
;; ╚════════════════════════════════╝

(defn decode-begin [^ByteBuffer bb]
  (let [lsn (int64 bb)
        commit-timestamp (decode-timestamp bb)
        xid (int32 bb)]
    {:type :begin
     :lsn lsn
     :commit-timestamp commit-timestamp
     :xid xid}))

(defn xid? [state]
  (= :in-progress (:tx-state state)))

(defn decode-message [^ByteBuffer bb state]
  (let [xid (when (xid? state) (int32 bb))
        flags (case (int8 bb)
                0 :none
                1 :transactional
                :unknown)
        lsn (int64 bb)
        prefix (decode-cstring bb)
        len (int32 bb)
        content (let [ba (byte-array len)]
                  (.get bb ba)
                  ba)]
    (cond-> {:type :message
             :flags flags
             :lsn lsn
             :prefix prefix
             :content content}
      xid (assoc :xid xid))))

(defn decode-commit [^ByteBuffer bb]
  ;; Flags for Commit messages are currently unused.
  ;;
  ;; https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-COMMIT
  (let [_flags (int8 bb)
        commit-lsn (int64 bb)
        tx-end-lsn (int64 bb)
        commit-timestamp (decode-timestamp bb)]
    {:type :commit
     :commit-lsn commit-lsn
     :tx-end-lsn tx-end-lsn
     :commit-timestamp commit-timestamp}))

(defn decode-origin [^ByteBuffer bb]
  (let [commit-lsn (int64 bb)
        name (decode-cstring bb)]
    {:type :origin
     :commit-lsn commit-lsn
     :name name}))

(defn decode-relation [^ByteBuffer bb state]
  (let [xid (when (xid? state) (int32 bb))
        oid (int32 bb)
        namespace (decode-cstring bb)
        relation (decode-cstring bb)
        replica-identity (case (int8 bb)
                           100 :primary-key
                           110 :nothing
                           102 :full
                           105 :index
                           :unknown)
        attributes (decode-relation-attributes bb)]
    (cond-> {:type :relation
             :oid oid
             :namespace namespace
             :relation relation
             :replica-identity replica-identity
             :attributes attributes}
      xid (assoc :xid xid))))

(defn decode-type [^ByteBuffer bb state]
  (let [xid (when (xid? state) (int32 bb))
        oid (int32 bb)
        namespace (decode-cstring bb)
        name (decode-cstring bb)]
    (cond-> {:type :type
             :oid oid
             :namespace namespace
             :name name}
      xid (assoc :xid xid))))

(declare decode-tuple-data)

(defn decode-insert [^ByteBuffer bb state]
  (let [xid (when (xid? state) (int32 bb))
        oid (int32 bb)
        _new? (int8 bb)
        tuple (decode-tuple-data bb)]
    (cond-> {:type :insert
             :oid oid
             :new-tuple tuple}
      xid (assoc :xid xid))))

(defn decode-update [^ByteBuffer bb state]
  (let [xid (when (xid? state) (int32 bb))
        oid (int32 bb)
        key? (try-decode-byte bb #_\K 75)
        old? (try-decode-byte bb #_\O 79)
        old-tuple (when (or key? old?) (decode-tuple-data bb))
        _new? (int8 bb)
        new-tuple (decode-tuple-data bb)]
    (cond-> {:type :update
             :oid oid
             :new-tuple new-tuple}
      xid (assoc :xid xid)
      old-tuple (assoc
                  :old-tuple old-tuple
                  :replica-identity (cond
                                      key? :primary-key
                                      old? :full
                                      :else :unknown)))))

(defn decode-delete [^ByteBuffer bb state]
  (let [xid (when (xid? state) (int32 bb))
        oid (int32 bb)
        key? (try-decode-byte bb #_\K 75)
        old? (try-decode-byte bb #_\O 79)
        old-tuple (decode-tuple-data bb)]
    (cond-> {:type :delete
             :oid oid
             :old-tuple old-tuple}
      xid (assoc :xid xid)
      (or key? old?)
      (assoc :replica-identity
        (cond
          key? :primary-key
          old? :full
          :else :unknown)))))

(defn decode-stream-start [^ByteBuffer bb]
  (let [xid (int32 bb)
        segment (int8 bb)]
    {:type :stream-start
     :xid xid
     :segment (case segment
                (byte 1) :first
                :other)}))

(defn decode-stream-stop [_]
  {:type :stream-stop})

(defn decode-stream-commit [^ByteBuffer bb]
  (let [xid (int32 bb)
        _flags (int8 bb)
        commit-lsn (int64 bb)
        tx-end-lsn (int64 bb)
        commit-timestamp (decode-timestamp bb)]
    {:type :stream-commit
     :xid xid
     :commit-lsn commit-lsn
     :tx-end-lsn tx-end-lsn
     :commit-timestamp commit-timestamp}))

(defn- parallel-streaming? [state]
  (and (= :parallel (:streaming state)) (>= ^long (:protocol-version state 0) 4)))

(defn decode-stream-abort [^ByteBuffer bb state]
  (let [xid (int32 bb)
        subtransaction-xid (int32 bb)]
    (cond->
      {:type :stream-abort
       :xid xid
       :subtransaction-xid subtransaction-xid}
      (parallel-streaming? state) (assoc :abort-lsn (int64 bb))
      (parallel-streaming? state) (assoc :tx-timestamp (decode-timestamp bb)))))

(defn decode-truncate [^ByteBuffer bb state]
  (let [xid (when (xid? state) (int32 bb))
        size (int32 bb)
        os (object-array size)
        flags (int8 bb)
        parameters (cond-> #{}
                     (pos? (bit-and flags 1)) (conj :cascade)
                     (pos? (bit-and flags 2)) (conj :restart-identity))
        oids (loop [i 0]
               (if (= i size)
                 (PersistentVector/adopt os)
                 (let [oid (int32 bb)]
                   (aset os i (Integer. oid))
                   (recur (inc i)))))]
    (cond-> {:type :truncate
             :oids oids}
      xid (assoc :xid xid)
      parameters (assoc :parameters parameters))))

;; Not implemented:
;;
;; - https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-BEGIN-PREPARE
;; - https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-PREPARE
;; - https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-COMMIT-PREPARED
;; - https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-ROLLBACK-PREPARED
;; - https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-STREAM-PREPARE

(defn decode-tuple-data [^ByteBuffer bb]
  (let [size (int16 bb)
        os (object-array size)]
    (loop [i 0]
      (if (= i size)
        (PersistentVector/adopt os)
        (let [format (int8 bb)
              attr (case format
                     ;; NULL
                     #_\n 110
                     nil

                     ;; Unchanged TOASTed value (actual value is not sent).
                     ;;
                     ;; See also: https://debezium.io/blog/2019/10/08/handling-unchanged-postgres-toast-values/
                     #_\u 117
                     :unchanged-toasted-value

                     ;; Text-formatted value
                     #_\t 116
                     (let [len (int32 bb)]
                       (charset/string (bb/slice len bb)))

                     ;; Binary-formatted value
                     #_\b 98
                     (let [len (int32 bb)]
                       (bb/slice len bb))

                     (anomaly! "Not implemented" ::anomalies/unsupported {:format (char format)}))]
          (aset os i attr)
          (recur (inc i)))))))

(defn decode-logical-replication-message [^ByteBuffer bb state]
  (let [b (int8 bb)]
    (case b
      #_\A 65 (decode-stream-abort bb state)
      #_\B 66 (decode-begin bb)
      #_\C 67 (decode-commit bb)
      #_\D 68 (decode-delete bb state)
      #_\E 69 (decode-stream-stop bb)
      #_\I 73 (decode-insert bb state)
      #_\M 77 (decode-message bb state)
      #_\O 79 (decode-origin bb)
      #_\R 82 (decode-relation bb state)
      #_\S 83 (decode-stream-start bb)
      #_\T 84 (decode-truncate bb state)
      #_\U 85 (decode-update bb state)
      #_\Y 89 (decode-type bb state)
      #_\c 99 (decode-stream-commit bb)
      (anomaly! "Not implemented" ::anomalies/unsupported {:prefix b}))))

;; ╔════════════╗
;; ║  MESSAGES  ║
;; ╚════════════╝

(defn decode-auth-response [^ByteBuffer bb]
  (let [b (int32 bb)
        type (case b
               0 :auth/ok
               2 :auth/krbv5
               3 :auth/clear-text-password
               5 :auth/md5-password
               7 :auth/gssapi
               8 :auth/gss-continue
               9 :auth/sspi
               10 :auth/sasl
               11 :auth/sasl-continue
               12 :auth/sasl-final)]
    ;; TODO
    (if (#{:auth/krbv5 :auth/clear-text-password :auth/md5-password :auth/gssapi :auth/gss-continue :auth/sspi} type)
      (anomaly! "Not implemented" ::anomalies/unsupported {:type type})
      (cond-> {:type type}
        (= :auth/sasl type)
        (assoc :mechanism (decode-cstring bb))

        (#{:auth/sasl-continue :auth/sasl-final} type)
        (merge (sasl/parse-server-first-message (charset/string (bb/slice (.remaining bb) bb))))))))

(defn decode-parameter-status [^ByteBuffer bb]
  {:type :parameter
   :parameter (MapEntry/create (decode-cstring bb) (decode-cstring bb))})

(defn decode-backend-key-data [^ByteBuffer bb]
  {:type :backend-key-data
   :pid (int32 bb)
   :secret-key (int32 bb)})

(defn decode-ready-for-query [^ByteBuffer bb]
  (let [b (int8 bb)]
    {:type :ready-for-query
     :tx-status (case b
                  #_I 73 :idle
                  #_T 84 :busy
                  #_E 69 :fail
                  (anomaly! "Not implemented" ::anomalies/unsupported {:b b}))}))

(defn decode-row-description [^ByteBuffer bb]
  (let [size (int16 bb)
        os (object-array size)
        attrs (loop [i 0]
                (if (= i size)
                  (PersistentVector/adopt os)
                  (let [name (decode-cstring bb)
                        table-oid (int32 bb)
                        attr-num (int16 bb)
                        data-type-oid (int32 bb)
                        data-type-size (int16 bb)
                        type-modifier (int32 bb)
                        format-code (int16 bb)
                        format (case format-code 0 :txt 1 :bin :unknown)
                        field {:name name
                               :table-oid table-oid
                               :attr-num attr-num
                               :data-type-oid data-type-oid
                               :data-type-size data-type-size
                               :type-modifier type-modifier
                               :format format}]
                    (aset os i field)
                    (recur (inc i)))))]
    {:type :row-description
     :attrs attrs}))

(defn decode-data-row [^ByteBuffer bb]
  (let [tuple (let [size (int16 bb)
                    os (object-array size)]
                (loop [i 0]
                  (if (= i size)
                    (PersistentVector/adopt os)
                    (let [len (int32 bb)
                          v (if (neg? len)
                              nil
                              (bb/slice len bb))]
                      (aset os i v)
                      (recur (inc i))))))]
    {:type :data-row
     :tuple tuple}))

(defn ^:private parse-rows
  [tag n]
  (some-> tag (string/split #"\s") (nth n) (parse-long)))

(defn parse-command-tag
  [tag]
  (cond
    (string/starts-with? tag "INSERT")
    {:command "INSERT"
     :rows (parse-rows tag 2)}

    (string/starts-with? tag "DELETE")
    {:command "DELETE"
     :rows (parse-rows tag 1)}

    (string/starts-with? tag "UPDATE")
    {:command "UPDATE"
     :rows (parse-rows tag 1)}

    (string/starts-with? tag "MERGE")
    {:command "MERGE"
     :rows (parse-rows tag 1)}

    (string/starts-with? tag "SELECT")
    {:command "SELECT"
     :rows (parse-rows tag 1)}

    (string/starts-with? tag "MOVE")
    {:command "MOVE"
     :rows (parse-rows tag 1)}

    (string/starts-with? tag "FETCH")
    {:command "FETCH"
     :rows (parse-rows tag 1)}

    (string/starts-with? tag "COPY")
    {:command "COPY"
     :rows (parse-rows tag 1)}

    :else {:command tag}))

(defn decode-command-complete [^ByteBuffer bb]
  (let [tag (-> bb decode-cstring parse-command-tag)]
    (assoc tag :type :command-complete)))

(defn error-type
  "https://www.postgresql.org/docs/current/protocol-error-fields.html"
  [b]
  (case (char b)
    \S :severity-localized
    \V :severity
    \C :error-code
    \M :message
    \D :detail
    \H :hint
    \P :position
    \p :internal-position
    \q :internal-query
    \W :where
    \s :schema
    \t :table
    \c :column
    \d :data-type
    \n :constraint
    \F :file
    \L :line
    \R :routine
    :unknown))

(defn decode-error-notice-message-fields [^ByteBuffer bb]
  (loop [data (transient {})]
    (let [b (int8 bb)]
      (if (= (byte 0) b)
        (persistent! data)
        (recur
          (let [type (error-type b)
                val (decode-cstring bb)]
            (cond->
              (assoc! data type
                (cond-> val
                  (#{:line :position :internal-position} type) parse-long))
              (= :error-code type) (assoc! :cause (error-code/condition-name val :unknown)))))))))

(defn decode-error-response
  [^ByteBuffer bb]
  (let [{:keys [message] :as data} (decode-error-notice-message-fields bb)]
    {:type :error
     :ex (ex-info message (-> data (assoc :kind :muutos.error/server-error) (dissoc :message)))}))

(defn decode-notice-response
  [^ByteBuffer bb]
  {:type :notice
   :fields (decode-error-notice-message-fields bb)})

(defn decode-copy-both-response [^ByteBuffer bb]
  (let [format (case (int8 bb) 0 :txt 1 :bin)
        n-attrs (int16 bb)
        format-codes (loop [i 0 codes []]
                       (if (= i n-attrs)
                         codes
                         (recur (inc 1) (conj codes (int16 bb)))))]
    {:type :copy-both
     :format format
     :format-codes format-codes}))

(defn decode-wal-data [^ByteBuffer bb state]
  (let [lsn (int64 bb)
        end-lsn (int64 bb)
        system-clock (decode-timestamp bb)]
    {:type :wal-data
     :lsn lsn
     :end-lsn end-lsn
     :system-clock system-clock
     :section (decode-logical-replication-message bb state)}))

(defn decode-primary-keep-alive-message [^ByteBuffer bb]
  (let [lsn (int64 bb)
        system-clock (decode-timestamp bb)
        reply-asap? (case (int8 bb) 1 true false)]
    {:type :primary-keep-alive-message
     :lsn lsn
     :system-clock system-clock
     :reply-asap? reply-asap?}))

(defn decode-copy-data-response [^ByteBuffer bb state]
  ;; https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-START-REPLICATION
  (.mark bb)
  (let [prefix (int8 bb)
        data (case prefix
                      #_\k 107 (decode-primary-keep-alive-message bb)
                      #_\w 119 (decode-wal-data bb state)
                      (charset/string (doto bb .reset)))]
    {:type :copy-data
     :data data}))

(defn decode-copy-in-response [^ByteBuffer bb]
  (let [format (case (int8 bb) 0 :txt 1 :bin)
        len (int16 bb)
        format-codes (into [] (take len) (repeatedly #(int16 bb)))]
    {:type :copy-in
     :format format
     :format-codes format-codes}))

(defn decode-copy-out-response [^ByteBuffer bb]
  (let [format (case (int8 bb) 0 :txt 1 :bin)
        len (int16 bb)
        format-codes (into [] (take len) (repeatedly #(int16 bb)))]
    {:type :copy-out
     :format format
     :format-codes format-codes}))

(defn decode-copy-done-response [_]
  {:type :copy-done})

(defn decode-empty-query-response [_]
  {:type :empty-query})

(defn decode-notification-response [^ByteBuffer bb]
  (let [pid (int32 bb)
        channel (decode-cstring bb)
        payload (decode-cstring bb)]
    {:type :notification
     :pid pid
     :channel channel
     :payload payload}))

(defn decode-parse-complete [_]
  {:type :parse-complete})

(defn decode-bind-complete [_]
  {:type :bind-complete})

(defn decode-close-complete [_]
  {:type :close-complete})

(defn decode-parameter-description [^ByteBuffer bb]
  (let [size (int16 bb)
        os (object-array size)
        parameters (loop [i 0]
                     (if (= i size)
                       (PersistentVector/adopt os)
                       (let [p (int32 bb)]
                         (aset os i (Integer. p))
                         (recur (inc i)))))]
    {:type :parameter-description
     :parameters parameters}))

(defn decode-no-data [_]
  {:type :no-data})

(defn decode-portal-suspended [_]
  {:type :portal-suspended})

(defn decode
  ([bb] (decode bb {}))
  ([^ByteBuffer bb state]
   (let [b (int8 bb)]
     (case b
       #_1 49 (decode-parse-complete bb)
       #_2 50 (decode-bind-complete bb)
       #_3 51 (decode-close-complete bb)
       #_A 65 (decode-notification-response bb)
       #_C 67 (decode-command-complete bb)
       #_D 68 (decode-data-row bb)
       #_E 69 (decode-error-response bb)
       #_G 71 (decode-copy-in-response bb)
       #_H 72 (decode-copy-out-response bb)
       #_I 73 (decode-empty-query-response bb)
       #_K 75 (decode-backend-key-data bb)
       #_N 78 (decode-notice-response bb)
       #_R 82 (decode-auth-response bb)
       #_S 83 (decode-parameter-status bb)
       #_T 84 (decode-row-description bb)
       #_W 87 (decode-copy-both-response bb)
       #_Z 90 (decode-ready-for-query bb)
       #_c 99 (decode-copy-done-response bb)
       #_d 100 (decode-copy-data-response bb state)
       #_n 110 (decode-no-data bb)
       #_s 115 (decode-portal-suspended bb)
       #_t 116 (decode-parameter-description bb)
       (anomaly! "Not implemented" ::anomalies/unsupported {:b b})))))
