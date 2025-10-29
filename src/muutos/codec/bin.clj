(ns ^{:clj-kondo/ignore #{:unresolved-symbol}} muutos.codec.bin
  "Turn Postgres binary data into Java data types and vice versa."
  (:refer-clojure :exclude [parse-uuid])
  (:require [cognitect.anomalies :as-alias anomalies]
            [muutos.error :as-alias error]
            [muutos.impl.anomaly :refer [anomaly!]]
            [muutos.impl.bb :as bb :refer [float4 float8 int8 int16 int32 int64]]
            [muutos.impl.time :as time]
            [muutos.impl.charset :as charset]
            [muutos.type :as type])
  (:import (clojure.lang Named PersistentVector)
           (java.math RoundingMode)
           (java.net InetAddress Inet4Address Inet6Address)
           (java.nio ByteBuffer)
           (java.nio.charset Charset StandardCharsets)
           (java.time Duration LocalDate LocalDateTime LocalTime Instant OffsetTime Period ZoneOffset)
           (java.time.temporal ChronoUnit)
           (java.util UUID)
           (muutos.type Box Circle Document Inet Lexeme LogSequenceNumber Path Point Polygon Line LineSegment Range)))

;; https://www.postgresql.org/docs/current/catalog-pg-type.html

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn decode-cstring
  "Given a `java.nio.ByteBuffer`, decode a null-terminated string (aka C
  string) from the buffer."
  [^ByteBuffer bb]
  (let [pos (.position bb)
        ^long limit (loop [i 0]
                      (when (.hasRemaining bb)
                        (let [b (.get bb)]
                          (if (== 0x00 b)
                            i
                            (recur (inc i))))))
        slice (.slice bb pos limit)]
    (str (Charset/.decode StandardCharsets/UTF_8 slice))))

;; ╔══════════╗
;; ║  DECODE  ║
;; ╚══════════╝

(defmulti ^{:arglists '([oid bb])} decode
  "Given a PostgreSQL data type OID (`int`) and a `java.nio.ByteBuffer`, decode
  the PostgreSQL value (of the given data type) in the byte buffer into a Java
  data type."
  (fn [oid _bb] oid))

(defmethod decode :default [oid ^ByteBuffer _]
  (anomaly! (format "Unknown data type: OID %d" oid) ::anomalies/unsupported
    {:kind ::error/unknown-data-type :oid oid}))

;; The raison d'être of this macro is to automatically add array support to
;; every decoder with :array-oid.
;;
;; This is probably a very bad idea, but at least it's pain I'll only be
;; inflicting on myself. Users don't need to know that this macro exists --
;; they can just extend the "decode" multimethod.
(defmacro ^:private of-bin
  "Given the name of a Postgres data type (string or ident), a defn attribute
  map, and a body, define a function and a multimethod that decodes the given
  data type into a Java type."
  [oid attrs & body]
  (let [fnn (symbol (format "decode-%s" oid))
        docstring (format "Decode an instance of PostgreSQL data type OID %s into its corresponding Java type." oid)
        bb (vary-meta 'bb assoc :tag `ByteBuffer)
        attrs (assoc attrs :private true :oid oid)]
    `(do
       (defn ~fnn ~docstring ~attrs [~bb] (do ~@body))
       (defmethod decode (int ~oid) [_name# ~bb] (~fnn ~bb)))))

(defn ^:private oid->type [^long oid]
  (case oid
    16 Boolean/TYPE
    18 Character/TYPE
    20 Long/TYPE
    21 Short/TYPE
    23 Integer/TYPE
    25 String
    700 Float/TYPE
    701 Double/TYPE
    Object))

(defn ^:private aset-oid [^long oid ary idx val]
  (case oid
    16 (aset ^booleans ary idx ^boolean val)
    18 (aset ^chars ary idx ^char val)
    20 (aset ^longs ary idx ^long val)
    21 (aset ^shorts ary idx ^short val)
    23 (aset ^ints ary idx ^int val)
    25 (aset ^String/1 ary idx val)
    700 (aset ^floats ary idx ^float val)
    701 (aset ^doubles ary idx ^double val)
    (aset ^Object/1 ary idx ^Object val)))

(defn ^:private decode-array [bb]
  (let [dimensions (int32 bb)
        _has-nulls? (int32 bb)
        oid (int32 bb)
        data-type (oid->type oid)]
    (if (zero? dimensions)
      (make-array data-type 0)
      (let [dim-length (int32 bb)
            ary (make-array data-type dim-length)
            _lower-bound (int32 bb)]
        (loop [i 0]
          (if (= i dim-length)
            ary
            (let [len (int32 bb)]
              (if (= len -1)
                (do
                  (aset-oid oid ary i nil)
                  (recur (inc i)))
                (let [item (decode (int oid) (bb/slice len bb))]
                  (aset-oid oid ary i item)
                  (recur (inc i)))))))))))

(comment
  (macroexpand-1
    '(of-bin 16 {:array-oid 1000} (case (int8 bb) 1 true 0 false)))
  ,,,)

(of-bin 16 {:name "boolean" :array-oid 1000}
  (case (int8 bb) 1 true 0 false))

(comment (meta #'decode-bool) ,,,)

(of-bin 17 {:name "bytea" :array-oid 1001}
  (let [ba (byte-array (.remaining bb))]
    (.get (.slice bb) ba)
    ba))

(of-bin 18 {:name "char" :array-oid 1002}
  (char (int8 bb)))

(of-bin 19 {:name "name" :array-oid 1003} (charset/string bb))
(of-bin 20 {:name "int8" :array-oid 1016} (int64 bb))
(of-bin 21 {:name "int2" :array-oid 1005} (int16 bb))
(of-bin 22 {:name "int2vector" :array-oid 1006} (decode-array bb))
(of-bin 23 {:name "int4" :array-oid 1007} (int32 bb))
(of-bin 24 {:name "regproc" :array-oid 1008} (decode (int 26) bb))
(of-bin 25 {:name "text" :array-oid 1009} (charset/string bb))
(of-bin 26 {:name "oid" :array-oid 1028} (int32 bb))
(of-bin 28 {:name "xid" :array-oid 1011} (int32 bb))
(of-bin 30 {:name "oidvector" :array-oid 1013} (decode-array bb))

(of-bin 114 {:name "json" :array-oid 199}
  (let [ba (byte-array (.remaining bb))]
    (.get bb ba)
    ba))

(of-bin 194 {:name "pg_node_tree"} (charset/string bb))

(of-bin 600 {:name "point"}
  (let [x (float8 bb)
        y (float8 bb)]
    (Point. x y)))

(of-bin 601 {:name "lseg"}
  (let [x1 (float8 bb)
        y1 (float8 bb)
        x2 (float8 bb)
        y2 (float8 bb)]
    (LineSegment. (Point. x1 y1) (Point. x2 y2))))

(of-bin 602 {:name "path"}
  (let [open? (case (int8 bb) 0 true false)
        n (int32 bb)
        points (vec (repeatedly n #(decode (int 600) bb)))]
    (Path. open? points)))

(of-bin 603 {:name "box"}
  (let [x1 (float8 bb)
        y1 (float8 bb)
        x2 (float8 bb)
        y2 (float8 bb)]
    (Box. (Point. x1 y1) (Point. x2 y2))))

(of-bin 604 {:name "polygon"}
  (let [n (int32 bb)
        points (vec (repeatedly n #(decode (int 600) bb)))]
    (Polygon. points)))

(of-bin 628 {:name "line"}
  (let [ax (float8 bb)
        by (float8 bb)
        c (float8 bb)]
    (Line. ax by c)))

(of-bin 700 {:name "float4" :array-oid 1021} (float4 bb))
(of-bin 701 {:name "float8" :array-oid 1022} (float8 bb))

(of-bin 705 {:name "unknown"} nil)

(of-bin 718 {:name "circle"}
  (let [x (float8 bb)
        y (float8 bb)
        r (float8 bb)]
    (Circle. x y r)))

(of-bin 790 {:name "money" :array-oid 791} (BigInteger/valueOf (int64 bb)))

(of-bin 869 {:name "inet" :array-oid 1041}
  (let [_family (int8 bb)
        netmask (int8 bb)
        type (case (int8 bb) 0 :inet 1 :cidr)
        addr-len (int8 bb)
        addr-bytes (byte-array addr-len)]
    (.get bb addr-bytes)
    (Inet. (InetAddress/getByAddress addr-bytes) type netmask)))

(of-bin 1042 {:name "bpchar" :array-oid 1014} (charset/string bb))
(of-bin 1043 {:name "varchar" :array-oid 1015} (charset/string bb))

(of-bin 1082 {:name "date" :array-oid 1182}
  (let [days-since-postgres-epoch (int32 bb)]
    (.plus time/postgres-epoch-date days-since-postgres-epoch ChronoUnit/DAYS)))

(of-bin 1184 {:name "timestamptz" :array-oid 1185}
  (let [ms (int64 bb)]
    (.plus time/postgres-epoch ms ChronoUnit/MICROS)))

(of-bin 1083 {:name "time" :array-oid 1183}
  (let [ms (int64 bb)]
    (.plus time/postgres-epoch-time ms ChronoUnit/MICROS)))

(of-bin 1114 {:name "timestamp" :array-oid 1115}
  (let [ms (int64 bb)]
    (.plus time/postgres-epoch-date-time ms ChronoUnit/MICROS)))

(of-bin 1186 {:name "interval" :array-oid 1187}
  (let [micros (int64 bb)
        millis (/ micros 1000)
        day (int32 bb)
        month (int32 bb)]
    (cond
      (and (= 0 millis) (= 0 day) (= 0 month))
      Duration/ZERO

      (or (not (= 0 month)) (not (= 0 day)))
      (.. (Period/ofMonths month) (plusDays day))

      :else (Duration/ofMillis millis))))

(of-bin 1266 {:name "timetz" :array-oid 1277}
  (let [microseconds-past-midnight (int64 bb)
        second-of-day (quot microseconds-past-midnight 1000000)
        offset (int32 bb)]
    (OffsetTime/of (LocalTime/ofSecondOfDay second-of-day)
      (ZoneOffset/ofTotalSeconds (- offset)))))

(def ^:private ^BigInteger ten-thousand (BigInteger/valueOf 10000))

(of-bin 1700 {:name "numeric" :array-oid 1231}
  ;; Treat number of digits as unsigned.
  ;;
  ;; Per https://github.com/pgjdbc/pgjdbc/blob/0a88ea425e86dce691a96d6aa7023c20ac887b98/pgjdbc/src/main/java/org/postgresql/util/ByteConverter.java#L134C48-L134C54.
  (let [len (bit-and (int16 bb) 0xFFFF)]
    (if (zero? len)
      BigDecimal/ZERO
      (let [first-digit-weight (int16 bb)
            sign (int16 bb)
            decimal-scale (int16 bb)
            exponent (* (- (inc first-digit-weight) len) 4)

            ;; TODO: Operating on a BigInteger in a hot loop is very slow. To
            ;; optimize, stay with longs until it's necessary to go BigInteger.
            ^BigInteger num (loop [i 0 num BigInteger/ZERO]
                              (if (= i len)
                                (cond-> num (= 0x4000 sign) (.negate))
                                (let [base-10000-digit (int16 bb)
                                      digit (BigInteger/valueOf base-10000-digit)
                                      base (.pow ten-thousand (- len i 1))]
                                  (recur (inc i) (.add num (.multiply base digit))))))]
        (.. (BigDecimal. num)
          (scaleByPowerOfTen exponent)
          (setScale decimal-scale RoundingMode/DOWN))))))

(defn ^:private decode-column [^ByteBuffer bb]
  (let [oid (int32 bb)
        len (int32 bb)]
    (when (nat-int? len)
      (decode oid (bb/slice len bb)))))

(of-bin 2249 {:name "record"}
  (let [fields (int32 bb)]
    (loop [m (transient {}) i 0]
      (if (= i fields)
        (persistent! m)
        (let [k (decode-column bb)
              v (decode-column bb)]
          (recur (assoc! m k v) (+ i 2)))))))

(of-bin 2278 {:name "void"} nil)

(of-bin 2950 {:name "uuid" :array-oid 2951}
  (let [high-bits (int64 bb)
        low-bits (int64 bb)]
    (UUID. high-bits low-bits)))

(of-bin 3220 {:name "pg_lsn" :array-oid 3221}
  (let [segment (int32 bb)
        byte-offset (int32 bb)]
    (LogSequenceNumber. segment byte-offset)))

(comment
  (decode 3220 (.. (ByteBuffer/allocate 8) (putInt 22) (putInt -1284188088) (flip)))
  ,,,)

(of-bin 3614 {:name "tsvector" :array-oid 3643}
  (let [size (int32 bb)
        os (object-array size)]
    (loop [i 0]
      (if (= i size)
        (Document. (PersistentVector/adopt os))
        (let [text (decode-cstring bb)
              n-pos (int16 bb)
              w (object-array n-pos)
              entries (loop [j 0]
                        (if (= j n-pos)
                          (PersistentVector/adopt w)
                          (let [word-entry-pos (int16 bb)
                                weight (bit-and (bit-shift-right word-entry-pos 14) 3)
                                weight (case weight
                                         3 \A
                                         2 \B
                                         1 \C
                                         0 \D
                                         \?)
                                position (bit-and word-entry-pos 0x3FFF)]
                            (aset w j {:position position :weight weight})
                            (recur (inc j)))))]
          (aset os i (Lexeme. text entries))
          (recur (inc i)))))))

(of-bin 3802 {:name "jsonb" :array-oid 3807}
  ;; Skip version byte
  (assert (= 1 (int8 bb)) "Unsupported JSONB version")
  (let [ba (byte-array (.remaining bb))]
    (.get bb ba)
    ba))

;; Ranges

(defn ^:private decode-range [oid ^ByteBuffer bb]
  (let [flag-byte (int8 bb)
        empty? (bit-test flag-byte 0)
        lb-inc? (bit-test flag-byte 1)
        ub-inc? (bit-test flag-byte 2)
        lb-inf? (bit-test flag-byte 3)
        ub-inf? (bit-test flag-byte 4)
        contain-empty? (bit-test flag-byte 7)
        lb (if (or empty? lb-inf?)
             nil
             (do
               (int32 bb)
               (decode oid bb)))
        ub (if (or empty? ub-inf?)
             nil
             (do
               (int32 bb)
               (decode oid bb)))]
    (Range. lb lb-inc? ub ub-inc? contain-empty?)))

(of-bin 3904 {:name "int4range" :array-oid 3905} (decode-range (int 23) bb))
(of-bin 3906 {:name "numrange" :array-oid 3907} (decode-range (int 1700) bb))
(of-bin 3908 {:name "tsrange" :array-oid 3909} (decode-range (int 1114) bb))
(of-bin 3910 {:name "tstzrange" :array-oid 3911} (decode-range (int 1184) bb))
(of-bin 3912 {:name "daterange" :array-oid 3913} (decode-range (int 1082) bb))
(of-bin 3926 {:name "int8range" :array-oid 3927} (decode-range (int 20) bb))

;; Arrays

(defn ^:private array-type-oids []
  (into (sorted-set)
    (comp
      (map val)
      (keep (comp :array-oid meta)))
    (ns-interns *ns*)))

(doseq [oid (array-type-oids)]
  (defmethod decode oid [_ bb] (decode-array bb)))

;; ╔══════════╗
;; ║  ENCODE  ║
;; ╚══════════╝

(declare encode-array)
(declare encode-string-array)

(defprotocol Parameter
  "A Clojure/Java type that can be encoded into a `java.nio.ByteBuffer` for use
  as a PostgreSQL query parameter."
  :extend-via-metadata true
  (encode ^ByteBuffer [this]
    "Encode a parameter into a `java.nio.ByteBuffer`."))

(extend-protocol Parameter
  nil
  ;; Special case; the PostgreSQL binary representation of NULL is a length of
  ;; -1 with no value. muutos.impl.encode/bind :encode handles this, but I'm
  ;; not sure if that's a good idea. Maybe this should return a ByteBuffer too,
  ;; and every encode impl here should prefix the ByteBuffer it generates with
  ;; its length?
  (encode [_] nil)

  byte/1
  (encode [this]
    (ByteBuffer/wrap this))

  Boolean
  (encode [this]
    (.. (ByteBuffer/allocate 1)
      (put (byte (if this #_\t 1 #_\f 0)))
      (flip)))

  Byte
  (encode [this]
    (.. (ByteBuffer/allocate 1) (put this) (flip)))

  Character
  (encode [this]
    (.. (ByteBuffer/allocate 1) (put ^byte (.charValue this)) (flip)))

  Double
  (encode [this]
    (.. (ByteBuffer/allocate 8) (putDouble this) (flip)))

  Float
  (encode [this]
    (.. (ByteBuffer/allocate 4) (putFloat this) (flip)))

  Integer
  (encode [this]
    (.. (ByteBuffer/allocate 4) (putInt this) (flip)))

  Long
  (encode [this]
    (.. (ByteBuffer/allocate 8) (putLong this) (flip)))

  Short
  (encode [this]
    (.. (ByteBuffer/allocate 2) (putShort this) (flip)))

  UUID
  (encode [this]
    (.. (ByteBuffer/allocate 16)
      (putLong (.getMostSignificantBits this))
      (putLong (.getLeastSignificantBits this))
      (flip)))

  String
  (encode [this]
    (let [ba (.getBytes this StandardCharsets/UTF_8)]
      (.. (ByteBuffer/allocate (alength ba))
        (put ^bytes ba)
        (flip))))

  Named
  (encode [this]
    (encode (name this)))

  ;; Primitive arrays & string arrays
  boolean/1 (encode [this] (encode-array this))
  char/1 (encode [this] (encode-array this))
  short/1 (encode [this] (encode-array this))
  int/1 (encode [this] (encode-array this))
  long/1 (encode [this] (encode-array this))
  float/1 (encode [this] (encode-array this))
  double/1 (encode [this] (encode-array this))
  String/1 (encode [this] (encode-string-array this))

  Instant
  (encode [this]
    (let [micros (.between ChronoUnit/MICROS time/postgres-epoch this)]
      (.. (ByteBuffer/allocate 8) (putLong (long micros)) (flip))))

  OffsetTime
  (encode [this]
    (let [micros-since-midnight (.between ChronoUnit/MICROS time/postgres-epoch-time this)]
      (.. (ByteBuffer/allocate 12)
        (putLong micros-since-midnight)
        (putInt (- (-> this .getOffset .getTotalSeconds)))
        (flip))))

  LocalDate
  (encode [this]
    (let [days (.between ChronoUnit/DAYS time/postgres-epoch-date this)]
      (.. (ByteBuffer/allocate 4) (putInt days) (flip))))

  LocalDateTime
  (encode [this]
    (let [micros (.between ChronoUnit/MICROS time/postgres-epoch-date-time this)]
      (.. (ByteBuffer/allocate 8) (putLong micros) (flip))))

  LocalTime
  (encode [this]
    (let [micros-since-midnight (.between ChronoUnit/MICROS time/postgres-epoch-time this)]
      (.. (ByteBuffer/allocate 8) (putLong micros-since-midnight) (flip))))

  Duration
  (encode [this]
    (let [seconds (.getSeconds this)]
      (.. (ByteBuffer/allocate (+ 8 4 4))
        (putLong (* 1000000 seconds))
        (putInt 0)
        (putInt 0)
        (flip))))

  Period
  (encode [this]
    (let [years (.getYears this)
          days (.getDays this)
          months (.getMonths this)]
      (.. (ByteBuffer/allocate (+ 8 4 4))
        (putLong 0)
        (putInt days)
        (putInt (+ months (* 12 years)))
        (flip))))

  BigInteger
  (encode [this]
    (.. (ByteBuffer/allocate 8)
      (putLong (.longValue this))
      (flip)))

  BigDecimal
  (encode [this]
    ;; TODO
    (anomaly! (format "Not implemented: %s" (-> this class .getName)) ::anomalies/unsupported
      {:class (class this)}))

  Inet
  (encode [{:keys [address type netmask]}]
    (let [family (cond
                   (instance? Inet4Address address) 2
                   (instance? Inet6Address address) 3)
          bs (.getAddress ^InetAddress address)
          len (alength bs)]
      (.. (ByteBuffer/allocate (+ 1 1 1 1 len))
        (put (byte family))
        (put (byte netmask))
        (put (byte (case type :inet 0 :cidr 1)))
        (put (byte len))
        (put ^bytes bs)
        (flip))))

  LogSequenceNumber
  (encode [{:keys [segment byte-offset]}]
    (.. (ByteBuffer/allocate 8)
      (putInt segment)
      (putInt byte-offset)
      (flip)))

  Point
  (encode [{:keys [x y]}]
    (.. (ByteBuffer/allocate (* 8 2))
      (putDouble x)
      (putDouble y)
      (flip)))

  LineSegment
  (encode [{:keys [point-1 point-2]}]
    (.. (ByteBuffer/allocate (* 8 4))
      (putDouble (:x point-1))
      (putDouble (:y point-1))
      (putDouble (:x point-2))
      (putDouble (:y point-2))
      (flip)))

  Path
  (encode [{:keys [open? points]}]
    (let [open-bit (if open? 0 1)
          len (count points)
          bb (ByteBuffer/allocate (+ 1 4 (* len 16)))]
      (.put bb (byte open-bit))
      (.putInt bb len)
      (run! (fn [{:keys [x y]}]
              (.putDouble bb x)
              (.putDouble bb y))
        points)
      (doto bb .flip)))

  Box
  (encode [{:keys [upper-right lower-left]}]
    (.. (ByteBuffer/allocate (* 8 4))
      (putDouble (:x upper-right))
      (putDouble (:y upper-right))
      (putDouble (:x lower-left))
      (putDouble (:y lower-left))
      (flip)))

  Polygon
  (encode [{:keys [points]}]
    (let [len (count points)
          bb (ByteBuffer/allocate (+ 4 (* len 16)))]
      (.putInt bb len)
      (run! (fn [{:keys [x y]}]
              (.putDouble bb x)
              (.putDouble bb y))
        points)
      (doto bb .flip)))

  Line
  (encode [{:keys [ax by c]}]
    (.. (ByteBuffer/allocate (* 8 3))
      (putDouble ax)
      (putDouble by)
      (putDouble c)
      (flip)))

  Circle
  (encode [{:keys [x y r]}]
    (.. (ByteBuffer/allocate (* 8 3))
      (putDouble x)
      (putDouble y)
      (putDouble r)
      (flip)))

  Range
  (encode [{:keys [lower-bound
                   lower-bound-inclusive?
                   upper-bound
                   upper-bound-inclusive?
                   contain-empty?]}]
    (let [flag-byte (byte (cond-> (byte 0)
                            (and (nil? lower-bound) (nil? upper-bound)) (bit-set 0)
                            lower-bound-inclusive? (bit-set 1)
                            upper-bound-inclusive? (bit-set 2)
                            (nil? lower-bound) (bit-set 3)
                            (nil? upper-bound) (bit-set 4)
                            contain-empty? (bit-set 7)))
          lower-bound-bb (encode lower-bound)
          lower-bound-len (.limit lower-bound-bb)
          upper-bound-bb (encode upper-bound)
          upper-bound-len (.limit upper-bound-bb)
          bb (ByteBuffer/allocate (+ 1 4 lower-bound-len 4 upper-bound-len))]
      (.. bb
        (put flag-byte)
        (putInt lower-bound-len)
        (put lower-bound-bb)
        (putInt upper-bound-len)
        (put upper-bound-bb)
        (flip))))

  Object
  (encode [this]
    (anomaly! (format "Cannot encode %s" (class this)) ::anomalies/unsupported)))

(defn ^:private size
  "Given the PostgreSQL data type OID of a data type that maps to a Java
  primitive array, return the number of bytes it occupies."
  ^long [oid]
  (condp = oid
    16 1 #_bool
    18 1 #_char
    20 8 #_int8
    21 2 #_int
    23 4 #_int4
    700 4 #_float4
    701 8 #_float8))

(defn ^:private array-oid [x]
  (condp = (class x)
    boolean/1 16
    char/1 18
    long/1 20
    short/1 21
    int/1 23
    String/1 25
    float/1 700
    double/1 701
    :else 0))

(defn ^:private byte-length [s]
  (-> s charset/bytes alength))

(defn ^:private encode-string-array [^String/1 ary]
  (let [dimensions 1
        has-nulls? 1
        oid (array-oid ary)
        dim-length (alength ary)
        lower-bound 1
        ^long len (transduce (map byte-length) + 0 ary)
        bb (ByteBuffer/allocate (+ 4 4 4 (* dimensions (+ 4 4)) (* 4 dim-length) len))]
    (.putInt bb dimensions)
    (.putInt bb has-nulls?)
    (.putInt bb oid)

    (dotimes [_ dimensions]
      (.putInt bb dim-length)
      (.putInt bb lower-bound)
      (run! (fn [x]
              (.putInt bb (byte-length x))
              (.put bb ^ByteBuffer (encode x)))
        ary))

    (doto bb .flip)))

(defn ^:private encode-array [ary]
  ;; FIXME: Support multi-dimensional arrays.
  (let [dimensions 1
        has-nulls? 0 ; FIXME?
        oid (array-oid ary)
        ;; FIXME: Not sure count is the most efficient approach here.
        dim-length (count ary)
        lower-bound 1 ; FIXME?
        size (size oid)
        bb (ByteBuffer/allocate (+ 4 4 4 (* dimensions (+ 4 4)) (* 4 dim-length) (* size dim-length)))]
    (.putInt bb dimensions)
    (.putInt bb has-nulls?)
    (.putInt bb oid)

    (dotimes [_ dimensions]
      (.putInt bb dim-length)
      (.putInt bb lower-bound)
      (run! (fn [x]
              (.putInt bb size)
              (.put bb ^ByteBuffer (encode x)))
        ary))

    (doto bb .flip)))
