(ns ^{:clj-kondo/ignore #{:unresolved-symbol}} muutos.codec.txt
  "Turn string representations of Postgres data types into Java data types."
  (:require [cognitect.anomalies :as-alias anomalies]
            [clojure.string :as string]
            [muutos.impl.anomaly :refer [anomaly!]]
            [muutos.error :as-alias error])
  (:import (java.net InetAddress)
           (java.time LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime)
           (java.time.format DateTimeFormatter)
           (java.util UUID)
           (muutos.type LogSequenceNumber)))

(set! *warn-on-reflection* true)

(defmulti ^{:arglists '([oid s])} decode
  "Given a PostgreSQL data type OID (`int`) and a string representing that data
  type, parse the string into a Java data type."
  (fn [oid _s] oid))

(defmethod decode :default [oid _]
  (anomaly! (format "Unknown data type: OID %d" oid) ::anomalies/unsupported
    {:kind ::error/unknown-data-type :oid oid}))

(defmacro ^:private of-txt
  "See muutos.codec.bin/of-bin."
  [oid & body]
  (let [fnn (symbol (format "decode-%s" oid))
        docstring (format "Decode a string representation of a PostgreSQL data type OID %d into its corresponding Java type." oid)
        s (vary-meta 's assoc :tag `String)
        attrs {:private true}]
    `(do
       (defn ~fnn ~docstring ~attrs [~s] (do ~@body))
       (defmethod decode (int ~oid) [_oid# ~s] (~fnn ~s)))))

(of-txt 16 (case s "t" true "f" false)) ; boolean
(of-txt 18 (.charAt s 0)) ; char
(of-txt 19 s) ; name
(of-txt 20 (parse-long s)) ; int8
(of-txt 21 (Short/parseShort s)) ; int2
(of-txt 23 (Integer/parseInt s)) ; int4
(of-txt 24 s) ; regproc
(of-txt 25 s) ; text
(of-txt 26 (Integer/parseInt s)) ; oid
(of-txt 28 (Integer/parseInt s)) ; xid
(of-txt 114 s) ; json
(of-txt 194 s) ; pg_node_tree
(of-txt 700 (Float/parseFloat s)) ; float4
(of-txt 701 (Double/parseDouble s)) ; float8
(of-txt 705 nil) ; unknown
(of-txt 774 s) ; macaddr8
(of-txt 790 (some-> s (subs 1) (BigDecimal.))) ; money
(of-txt 829 s) ; macaddr
(of-txt 869 (InetAddress/getByName s)) ; inet
(of-txt 1042 s) ; bpchar
(of-txt 1043 s) ; varchar
(of-txt 1082 (LocalDate/parse s)) ; date
(of-txt 1083 (LocalTime/parse s)) ; time

(def ^:private fmt-timestamp
  (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))

(of-txt 1114 (LocalDateTime/parse s fmt-timestamp)) ; timestamp

(def ^:private fmt-timestamptz
  (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss[.[SSSSSS][SSSSS][SSSS][SSS][SS][S]][[XXX][XX][X]]"))

(of-txt 1184 (OffsetDateTime/parse s fmt-timestamptz)) ; timestamptz

(def ^:private fmt-timetz
  (DateTimeFormatter/ofPattern "HH:mm:ss[[XXX][XX][X]]"))

(of-txt 1266 (OffsetTime/parse s fmt-timetz)) ; timetz
(of-txt 1560 s) ; bit
(of-txt 1562 s) ; varbit
(of-txt 1700 (BigDecimal. s)) ; numeric
(of-txt 2278 nil) ; void
(of-txt 2950 (UUID/fromString s)) ; uuid

(of-txt 3220
  (let [[segment byte-offset] (some-> s (string/split #"/"))]
    (LogSequenceNumber.
      (Long/parseLong segment 16)
      (Integer/parseUnsignedInt byte-offset 16)))) ; pg_lsn

(comment
  (decode 3220 "0/0")
  (decode 3220 "0/14E5348")
  ,,,)

(of-txt 3802 s) ; jsonb

(comment
  (decode 16 "t")
  (decode 16 "f")
  (decode 18 "A")
  (decode 1560 "101")
  (decode 790 "$1.0")
  (decode 2950 "5b603992-cc37-4cfc-9da7-d19213c1c7bc")
  ,,,)
