(ns muutos.codec.bin-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [muutos.codec.bin :as bin]
            [muutos.impl.type :as type]
            [muutos.impl.specs.gen :as specs.gen]
            [muutos.test.array :as array])
  (:import (java.net Inet4Address Inet6Address)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (muutos.type Box Circle Inet Line LineSegment LogSequenceNumber Path Point Polygon Range)
           (org.postgresql.util ByteConverter)))

(set! *warn-on-reflection* true)

(def gen-inet4-address
  (gen/fmap (comp Inet4Address/getByAddress byte-array) (gen/vector gen/byte 4)))

(def gen-inet6-address
  (gen/fmap
    (fn [byte-vec] (Inet6Address/getByAddress ^bytes (byte-array byte-vec)))
    (gen/vector gen/byte 16)))

(comment
  (gen/generate gen-inet4-address)
  (gen/generate gen-inet6-address)
  ,,,)

(def gen-inet
  (gen/fmap (fn [[address type netmask]] (Inet. address type netmask))
    (gen/tuple (gen/one-of [gen-inet4-address gen-inet6-address]) (gen/elements #{:inet :cidr}) gen/byte)))

(def gen-log-sequence-number
  (gen/fmap (fn [[segment byte-offset]]
              (LogSequenceNumber. segment byte-offset))
    (gen/tuple gen/small-integer gen/small-integer)))

(def gen-bigdecimal
  (gen/fmap (fn [[^clojure.lang.BigInt bi ^int i]] (BigDecimal/new ^BigInteger (.toBigInteger bi) i))
    (gen/tuple (gen/fmap bigint gen/size-bounded-bigint) (gen/fmap int gen/small-integer))))

(def gen-finite-double
  (gen/double* {:infinite? false :NaN? false}))

(def gen-point
  (gen/fmap (fn [[x y]] (Point. x y))
    (gen/tuple gen-finite-double gen-finite-double)))

(declare gen-record)

(defn gen-range [gen]
  (gen/fmap (fn [[lower-bound
                  lower-bound-inclusive?
                  upper-bound
                  upper-bound-inclusive?
                  contain-empty?]]
              (Range. lower-bound lower-bound-inclusive? upper-bound upper-bound-inclusive? contain-empty?))
    (gen/tuple gen gen/boolean gen gen/boolean gen/boolean)))

(defn gen-for
  [^long oid]
  (case oid
    16 gen/boolean
    17 gen/bytes
    18 gen/char-ascii
    19 gen/string
    20 gen/large-integer
    21 (gen/fmap short (gen/choose Short/MIN_VALUE Short/MAX_VALUE))
    23 (gen/fmap int (gen/choose Integer/MIN_VALUE Integer/MAX_VALUE))
    25 gen/string
    114 (gen/one-of [(gen/map gen/any gen/any) (gen/vector gen/any)])
    600 gen-point
    601 (gen/fmap (fn [[point-1 point-2]] (LineSegment. point-1 point-2)) (gen/tuple gen-point gen-point))
    602 (gen/fmap (fn [[open? points]] (Path. open? points)) (gen/tuple gen/boolean (gen/vector gen-point)))
    603 (gen/fmap (fn [[upper-right lower-left]] (Box. upper-right lower-left)) (gen/tuple gen-point gen-point))
    604 (gen/fmap (fn [points] (Polygon. points)) (gen/vector gen-point))
    628 (gen/fmap (fn [[ax by c]] (Line. ax by c)) (gen/tuple gen-finite-double gen-finite-double gen-finite-double))
    ;; FIXME: NaN?
    790 (gen/fmap BigInteger/valueOf gen/large-integer)
    869 gen-inet
    700 specs.gen/float
    701 specs.gen/double
    718 (gen/fmap (fn [[x y r]] (Circle. x y r)) (gen/tuple gen-finite-double gen-finite-double gen-finite-double))
    1042 gen/string
    1043 gen/string
    1082 specs.gen/local-date
    1186 specs.gen/duration-or-period
    1083 specs.gen/local-time
    1266 specs.gen/offset-time
    1114 specs.gen/local-date-time
    1184 specs.gen/instant
    3220 gen-log-sequence-number
    2950 gen/uuid
    2249 gen-record
    1000 (gen/fmap boolean-array (gen/vector (gen-for 16)))
    1001 (gen/fmap char-array (gen/vector (gen-for 18)))
    1016 (gen/fmap long-array (gen/vector (gen-for 20)))
    1005 (gen/fmap short-array (gen/vector (gen-for 21)))
    1007 (gen/fmap int-array (gen/vector (gen-for 23)))
    1009 (gen/fmap (fn [s] (into-array String s)) (gen/vector gen/string))
    1021 (gen/fmap float-array (gen/vector (gen-for 700)))
    1022 (gen/fmap float-array (gen/vector (gen-for 701)))
    1700 gen-bigdecimal
    3904 (gen-range (gen-for 23))
    3906 (gen-range (gen-for 1700))
    3908 (gen-range (gen-for 1114))
    3910 (gen-range (gen-for 1184))
    3912 (gen-range (gen-for 1082))
    3926 (gen-range (gen-for 20))))

(def gen-record
  (let [gen-record-kv (gen/one-of [(gen-for 16)
                                   (gen-for 18)
                                   (gen-for 21)
                                   (gen-for 23)
                                   (gen-for 20)
                                   (gen-for 25)
                                   (gen-for 700)
                                   (gen-for 701)
                                   (gen-for 1082)
                                   (gen-for 1083)
                                   (gen-for 1114)
                                   (gen-for 1184)
                                   (gen-for 1186)
                                   (gen-for 1700)
                                   (gen-for 2950)
                                   #_(gen-for 2249) ; FIXME: Support nested records?
                                   (gen-for 3220)])]
    (gen/map gen-record-kv gen-record-kv)))

(comment
  (bin/decode 18 (bin/encode \A))
  ,,,)

(extend-protocol bin/Parameter
  BigDecimal
  (encode [this]
    (ByteBuffer/wrap (ByteConverter/numeric this))))

(defn ^:private encode-record [m]
  (let [cnt (count m)
        kvs (into []
              (comp
                cat
                (map
                  (fn [x]
                    (let [oid (type/oid x)
                          ^ByteBuffer bb (bin/encode x)]
                      {:oid oid
                       :len (.limit bb)
                       :bb bb}))))
              m)
        len (reduce (fn [^long n {:keys [^long len]}] (+ n 4 4 len)) 4 kvs)
        record-bb (.. (ByteBuffer/allocate len)
                    (putInt (unchecked-multiply-int cnt 2)))]
    (run! (fn [{:keys [oid len ^ByteBuffer bb]}]
            (.putInt record-bb oid)
            (.putInt record-bb len)
            (.put record-bb bb))
      kvs)
    (doto record-bb .flip )))

(defmacro round-trip-prop
  ([oid] `(round-trip-prop ~oid nil))
  ([oid meta]
   `(let [m# ~meta]
      (prop/for-all [i# (gen-for ~oid)]
        (let [ret# (bin/decode ~oid (bin/encode (cond-> i# m# (with-meta m#))))]
          (if (array/array? i#)
            (array/equals? i# ret#)
            (= i# ret#)))))))

(defspec round-trip-bool 100 (round-trip-prop 16))
(defspec round-trip-bytea 10000 (round-trip-prop 17))
(defspec round-trip-char 10000 (round-trip-prop 18))
(defspec round-trip-int8 10000 (round-trip-prop 20))
(defspec round-trip-short2 10000 (round-trip-prop 21))
(defspec round-trip-int4 10000 (round-trip-prop 23))
(defspec round-trip-text 1000 (round-trip-prop 25))
(defspec round-trip-float4 10000 (round-trip-prop 700))
(defspec round-trip-float8 10000 (round-trip-prop 701))

(deftest float8-special
  (is (= ##Inf (bin/decode (int 701) (bin/encode ##Inf))))
  (is (= ##-Inf (bin/decode (int 701) (bin/encode ##-Inf))))
  (is (NaN? (bin/decode (int 701) (bin/encode ##NaN)))))

(defspec round-trip-money 10000 (round-trip-prop 790))
(defspec round-trip-inet 10000 (round-trip-prop 869))
(defspec round-trip-bpchar 1000 (round-trip-prop 1042))
(defspec round-trip-varchar 1000 (round-trip-prop 1043))
(defspec round-trip-date 10000 (round-trip-prop 1082))
(defspec round-trip-time 10000 (round-trip-prop 1083))
(defspec round-trip-timestamp 10000 (round-trip-prop 1114))
(defspec round-trip-timestamptz 10000 (round-trip-prop 1184))
(defspec round-trip-interval 10000 (round-trip-prop 1186))
(defspec round-trip-timetz 10000 (round-trip-prop 1266))

(deftest period-zero
  (is
    (= #time/duration "PT0S"
      (bin/decode (int 1186) (bin/encode #time/period "P0D"))))

  (is
    (= #time/duration "PT0S"
      (bin/decode (int 1186) (bin/encode #time/duration "PT0S")))))

(defspec round-trip-uuid 10000 (round-trip-prop 2950))
(defspec round-trip-lsn 10000 (round-trip-prop 3220))
(defspec round-trip-record 1000 (round-trip-prop 2249 {`bin/encode encode-record}))

(defspec round-trip-bool-array 100 (round-trip-prop 1000))
(defspec round-trip-char-array 10000 (round-trip-prop 1001))
(defspec round-trip-int2-array 1000 (round-trip-prop 1005))
(defspec round-trip-int8-array 1000 (round-trip-prop 1016))
(defspec round-trip-int4-array 1000 (round-trip-prop 1007))
(defspec round-trip-text-array 500 (round-trip-prop 1009))
(defspec round-trip-float4-array 1000 (round-trip-prop 1021))
(defspec round-trip-float8-array 1000 (round-trip-prop 1022))

(comment
  (bin/decode 1009 (bin/encode (into-array String [])))
  (bin/decode 1009 (bin/encode (into-array String ["a"])))
  ,,,)

(defspec round-trip-numeric 100000
  (prop/for-all [bd gen-bigdecimal]
    (= bd (bin/decode (int 1700) (bin/encode bd)))))

(defn ^:private utf8-string->cstring
  [^String s]
  (let [bytes (.getBytes s StandardCharsets/UTF_8)
        buffer (ByteBuffer/allocate (inc (count bytes)))]
    (doto buffer
      (.put bytes)
      (.put (byte 0))
      (.flip))))

(defspec round-trip-cstring 50000
  (prop/for-all [s gen/string-alphanumeric]
    (let [cs (utf8-string->cstring s)]
      (= s (bin/decode-cstring cs)))))

(defn ^:private decode-cstring-oracle [^ByteBuffer bb]
  (let [sb (StringBuilder.)]
    (loop []
      (when (.hasRemaining bb)
        (let [b (.get bb)]
          (when (not= 0x00 b)
            (.append sb (char b))
            (recur)))))
    (str sb)))

(comment
  (def s (gen/generate gen/string-alphanumeric 1000))

  (require '[criterium.core :refer [quick-bench]])

  (quick-bench (bin/decode-cstring (utf8-string->cstring s)))
  (quick-bench (decode-cstring-oracle (utf8-string->cstring s)))
  ,,,)

(defspec cstring-oracle 50000
  (prop/for-all [s gen/string-alphanumeric]
    (let [bb1 (utf8-string->cstring s)
          bb2 (utf8-string->cstring s)]
      (= (decode-cstring-oracle bb1) (bin/decode-cstring bb2)))))

;; Geometric types

(defspec round-trip-point 100000 (round-trip-prop 600))
(defspec round-trip-line-segment 100000 (round-trip-prop 601))
(defspec round-trip-path 10000 (round-trip-prop 602))
(defspec round-trip-box 10000 (round-trip-prop 603))
(defspec round-trip-polygon 10000 (round-trip-prop 604))
(defspec round-trip-line 100000 (round-trip-prop 628))
(defspec round-trip-circle 100000 (round-trip-prop 718))

(defspec round-trip-int4range 100000 (round-trip-prop 3904))
#_(defspec round-trip-numrange 100 (round-trip-prop 3906)) ; FIXME
(defspec round-trip-tsrange 100000 (round-trip-prop 3908))
(defspec round-trip-tstzrange 100000 (round-trip-prop 3910))
(defspec round-trip-daterange 100000 (round-trip-prop 3912))
(defspec round-trip-int8range 100000 (round-trip-prop 3926))
