(ns ^:no-doc muutos.specs.logical-replication-message
  "Specs for PostgreSQL logical replication messages represented as Clojure
  data."
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as spec]
            [clojure.spec.gen.alpha :as gen]
            [muutos.codec.bin :as bin]
            [muutos.impl.specs.gen :as specs.gen])
  (:import (java.nio ByteBuffer)
           (java.time Instant LocalDate)))

(set! *warn-on-reflection* true)

(spec/def ::type
  #{:begin
    :message
    :commit
    :origin
    :relation
    :type
    :insert
    :update
    :delete
    :truncate
    :stream-start
    :stream-stop
    :stream-commit
    :stream-abort})

(defmulti ^:private msg-type :type)

(spec/def ::lsn
  pos-int?)

(defn ^:private instant? [x]
  (instance? Instant x))

(spec/def ::commit-timestamp
  (spec/with-gen instant? (constantly specs.gen/instant)))

(defn ^:private int32? [x]
  (instance? Integer x))

(defn ^:private uint32? [x]
  (and int32? (pos? x)))

(spec/def ::uint32
  (spec/with-gen uint32? (constantly specs.gen/int)))

(spec/def ::xid ::uint32)

(defmethod msg-type :begin [_]
  (spec/keys :req-un [::type ::lsn ::commit-timestamp ::xid]))

(spec/def :message/flags
  #{:none :transactional})

(spec/def ::string (spec/and string? (fn [s] (not= "\0" s))))

(spec/def ::prefix ::string)

(defn ^:private byte-buffer? [x]
  (instance? ByteBuffer x))

(spec/def ::local-date
  (spec/with-gen (partial instance? LocalDate) (constantly specs.gen/local-date)))

(spec/def ::byte-buffer
  (spec/with-gen byte-buffer?
    #(gen/fmap (fn [x] (-> (bin/encode x) (doto .flip)))
       (gen/one-of
         [(gen/bytes)
          (gen/boolean)
          (gen/char)
          specs.gen/double
          specs.gen/float
          (gen/int)
          (gen/large-integer)
          specs.gen/short
          (gen/uuid)
          (gen/string)
          (gen/keyword)
          (gen/symbol)
          specs.gen/instant
          specs.gen/offset-time
          specs.gen/local-date-time
          (spec/gen ::local-date)
          specs.gen/local-time]))))

(spec/def ::content bytes?)

(defmethod msg-type :message [_]
  (spec/keys
    :req-un [::type :message/flags ::lsn ::prefix ::content]
    :opt-un [::xid]))

(spec/def ::commit-lsn ::lsn)

(spec/def ::tx-end-lsn ::lsn)

(defmethod msg-type :commit [_]
  (spec/and
    (spec/keys :req-un [::type ::commit-lsn ::tx-end-lsn ::commit-timestamp])
    (fn [{:keys [commit-lsn tx-end-lsn]}]
      (> tx-end-lsn commit-lsn))))

(spec/def ::name ::string)

(defmethod msg-type :origin [_]
  (spec/keys :req-un [::type ::commit-lsn ::name]))

(spec/def ::oid
  (spec/with-gen pos-int?
    #(gen/elements
       #{0
         16
         17
         18
         19
         20
         21
         22
         23
         24
         25
         26
         27
         28
         29
         30
         32
         114
         142
         143
         194
         199
         269
         271
         325
         600
         601
         602
         603
         604
         628
         629
         650
         651
         700
         701
         705
         718
         719
         774
         775
         790
         791
         829
         869
         1000
         1001
         1002
         1003
         1005
         1006
         1007
         1008
         1009
         1010
         1011
         1012
         1013
         1014
         1015
         1016
         1017
         1018
         1019
         1020
         1021
         1022
         1027
         1028
         1033
         1034
         1040
         1041
         1042
         1043
         1082
         1083
         1114
         1115
         1182
         1183
         1184
         1185
         1186
         1187
         1231
         1263
         1266
         1270
         1560
         1561
         1562
         1563
         1700
         1790
         2201
         2202
         2203
         2204
         2205
         2206
         2207
         2208
         2209
         2210
         2211
         2249
         2275
         2276
         2277
         2278
         2279
         2280
         2281
         2283
         2287
         2776
         2949
         2950
         2951
         2970
         3115
         3220
         3221
         3310
         3361
         3402
         3500
         3614
         3615
         3642
         3643
         3644
         3645
         3734
         3735
         3769
         3770
         3802
         3807
         3831
         3838
         3904
         3905
         3906
         3907
         3908
         3909
         3910
         3911
         3912
         3913
         3926
         3927
         4072
         4073
         4089
         4090
         4096
         4097
         4191
         4192
         4537
         4538
         4600
         4601
         5017
         5038
         5039
         5069
         5077
         5078
         5079
         5080})))

(spec/def ::namespace ::string)

(spec/def ::relation ::string)

(spec/def ::replica-identity
  #{:primary-key :nothing :full :index})

(spec/def ::flags
  (spec/with-gen
    (fn [s] (set/subset? s #{:key}))
    #(gen/frequency [[90 (gen/return #{})]
                     [10 (gen/return #{:key})]])))

(spec/def ::name ::string)

(spec/def ::data-type-oid ::oid)

(spec/def ::type-modifier
  (spec/with-gen int32? #(gen/fmap int (gen/int))))

(spec/def ::attribute
  (spec/keys :req-un [::flags ::name ::data-type-oid ::type-modifier]))

(spec/def ::attributes
  (spec/coll-of ::attribute :kind? vector?))

(defmethod msg-type :relation [_]
  (spec/keys
    :req-un [::type ::oid ::namespace ::relation ::replica-identity ::attributes]
    :opt-un [::xid]))

(defmethod msg-type :type [_]
  (spec/keys
    :req-un [::type ::oid ::namespace ::name]
    :opt-un [::xid]))

(spec/def ::tuple-value
  (spec/with-gen
    (spec/nilable
      (spec/or
        :unchanged-toasted-value #{:unchanged-toasted-value}
        :text-formatted-value ::string
        :binary-formatted-value ::byte-buffer))
    #(gen/frequency [[45 (spec/gen ::byte-buffer)]
                     [45 (spec/gen ::string)]
                     [8 (gen/return nil)]
                     [2 (gen/return :unchanged-toasted-value)]])))

(spec/def ::tuple
  (spec/coll-of ::tuple-value :kind vector?))

(spec/def ::new-tuple ::tuple)

(defmethod msg-type :insert [_]
  (spec/keys
    :req-un [::type ::oid ::new-tuple]
    :opt-un [::xid]))

(spec/def ::old-tuple ::tuple)

(spec/def :update/replica-identity
  #{:primary-key :full})

(defmethod msg-type :update [_]
  (spec/and
    (spec/keys
      :req-un [::type ::oid ::new-tuple]
      :opt-un [::xid ::old-tuple :update/replica-identity])
    (fn [message]
      (or
        (and (contains? message :replica-identity) (contains? message :old-tuple))
        (and (not (contains? message :replica-identity)) (not (contains? message :old-tuple)))))))

(spec/def :delete/replica-identity
  #{:primary-key :full})

(defmethod msg-type :delete [_]
  (spec/keys
    :req-un [::type ::oid ::old-tuple]
    :opt-un [::xid :delete/replica-identity]))

(spec/def ::segment #{:first :other})

(defmethod msg-type :stream-start [_]
  (spec/keys
    :req-un [::type ::xid ::segment]))

(defmethod msg-type :stream-stop [_]
  (spec/keys
    :req-un [::type]))

(defmethod msg-type :stream-commit [_]
  (spec/keys
    :req-un [::type ::xid ::commit-lsn ::tx-end-lsn ::commit-timestamp]))

(spec/def ::subtransaction-xid ::xid)
(spec/def ::abort-lsn ::lsn)
(spec/def ::tx-timestamp ::commit-timestamp)

(defmethod msg-type :stream-abort [_]
  (spec/and
    (spec/keys
      :req-un [::type ::xid ::subtransaction-xid]
      :opt-un [::abort-lsn ::tx-timestamp])
    (fn [{:keys [abort-lsn tx-timestamp]}]
      (or (and (nil? abort-lsn) (nil? tx-timestamp))
        (and abort-lsn tx-timestamp)))))

(spec/def ::oids
  (spec/coll-of ::oid :kind vector?))

(spec/def :truncate/parameters
  (spec/with-gen
    (fn [s] (set/subset? s #{:cascade :restart-identity}))
    #(gen/frequency [[70 (gen/return #{})]
                     [25 (gen/return #{:cascade})]
                     [5 (gen/return #{:cascade :restart-identity})]])))

(defmethod msg-type :truncate [_]
  (spec/keys
    :req-un [::type ::oids :truncate/parameters]
    :opt-un [::xid]))

(spec/def ::message
  (spec/multi-spec msg-type :type))

(comment
  (gen/sample (spec/gen ::message))
  ,,,)
