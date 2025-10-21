(ns muutos.sql-client.eq-test
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [cognitect.anomalies :as-alias anomalies]
            [matcher-combinators.matchers :as matchers]
            [matcher-combinators.test]
            [muutos.sql-client :refer [connect eq] :as sut]
            [muutos.codec.bin :as bin]
            [muutos.error :as-alias error]
            [muutos.test.array :as array]
            #_[muutos.test.fray :as fray]
            [muutos.test.server :as server :refer [host port]]
            [muutos.type])
  (:import (clojure.lang ExceptionInfo)
           (java.lang AutoCloseable)
           (java.net InetAddress)
           (java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetTime Period)
           (java.util Random)
           (muutos.type Box Circle Document Inet Lexeme Line LineSegment LogSequenceNumber Path Point Polygon Range)))

(def container-opts
  {:env-vars {"POSTGRES_PASSWORD" "postgres"
              "POSTGRES_DB" "test"}
   :exposed-ports [5432]
   :image-name "postgres:17"})

(defonce server
  (delay
    (server/start container-opts)))

(comment (.close @server) ,,,)

(use-fixtures :each
  (fn [f]
    (with-open [client (connect :host (host @server) :port (port @server))]
      (eq client ["DROP DATABASE test WITH (FORCE)"])
      (eq client ["CREATE DATABASE test"]))

    (f)))

(defmacro same? [a b]
  `(zero? (.compareTo ~a ~b)))

(defn connect-test [& {:as opts}]
  (connect (merge {:database "test" :host (host @server) :port (port @server)} opts)))

(deftest ^:integration read-error
  (with-open [server (server/start container-opts)
              pg (connect {:host (host server) :port (port server)})]
    (is (= [{"?column?" 1}] (eq pg ["SELECT $1" 1])))
    (AutoCloseable/.close server)

    (Thread/sleep 1000)

    ;; FIXME: Make more specific/deterministic. This at least checks the event
    ;; loop won't hang, though.
    (is (thrown? Exception (eq pg ["SELECT $1" 1])))
    (is (thrown? Exception (eq pg ["SELECT $1" 1])))))

(comment
  (def server (server/start container-opts))
  (def pg (connect {:host (host server) :port (port server)}))
  #_(AutoCloseable/.close pg)

  (is (= [{"?column?" 1}] (eq pg ["SELECT $1" 1])))
  (AutoCloseable/.close server)

  (eq pg ["SELECT $1" 1])
  ,,,)

(deftest ^:integration extended-query
  (with-open [pg (connect-test)]
    (is (nil? (eq pg nil)) "nil")

    (is (= [] (eq pg []))
      "Empty query vector")

    (is (= [] (eq pg [""]))
      "Empty query string")

    (is (= [] (eq pg ["SELECT FROM pg_type WHERE FALSE"]))
      "Empty result")

    (is (thrown-with-msg? ExceptionInfo #"syntax error at or near \"E'a'\""
          (eq pg ["E'a'::bytea"]))
      "Very bad query")

    (is (= [{}] (eq pg ["SELECT NULL"]))
      "NULL")

    (is (= [{}] (eq pg ["SELECT $1" nil]))
      "NULL")

    (is (= [{"a" 1}] (eq pg ["SELECT 1 AS a"]))
      "Basic select")

    (is (= [[{"a" 1}] [{"b" 2}]] (eq pg ["SELECT 1 AS a"] ["SELECT 2 AS b"]))
      "Multiple queries")

    (is (thrown-with-msg? ExceptionInfo #"relation \"a\" does not exist"
          (eq pg ["SELECT 1 AS a"] ["SELECT * FROM a"] ["SELECT 3 AS c"]))
      "Good query, bad query, good query")

    (is (= [{"n" 1}] (eq pg ["SELECT $1 AS n" 1 2]))
      "Extra parameters")))

(defn eq1 [pg attr-name qvec]
  (-> (eq pg qvec) first (get attr-name)))

(deftest ^:integration data-types
  (with-open [pg (connect-test)]
    (is (same? 9223372036854775807 (eq1 pg "int8" ["SELECT $1 AS int8" 9223372036854775807]))
      "int8")

    (is (= [{"bool" true}] (eq pg ["SELECT $1 AS bool" true]))
      "boolean (true)")

    (is (= [{"bool" false}] (eq pg ["SELECT $1 AS bool" false]))
      "boolean (false)")

    (is (= [{"char" \A}] (eq pg ["SELECT $1 AS char" \A]))
      "char")

    (is (= [{"text" "Hello, world!"}] (eq pg ["SELECT $1 AS text" "Hello, world!"]))
      "text")

    (let [date (LocalDate/now)]
      (is (= [{"date" date}] (eq pg ["SELECT $1 AS date" date]))
        "date"))

    (is (same? (float 3.14)
          (eq1 pg "float4" ["SELECT $1 AS float4" (float 3.14)]))
      "single precision")

    (is (same? (double 2.718281828459045)
          (eq1 pg "float8" ["SELECT $1 AS float8" (double 2.718281828459045)]))
      "double precision")

    (is (NaN? (eq1 pg "float8" ["SELECT 'NaN'::float8"])) "NaN")

    (is (= [{"float8" ##Inf}] (eq pg ["SELECT 'infinity'::float8"])))
    (is (= [{"float8" ##-Inf}] (eq pg ["SELECT '-infinity'::float8"])))

    (is (same? (biginteger 1)
          (eq1 pg "biginteger" ["SELECT $1 AS biginteger" (biginteger 1)]))
      "biginteger")

    (is (same? (biginteger 1234568) (eq1 pg "money" ["SELECT 12345.6789::money AS money;"])))

    (is (= [{"n" 1} {"n" 2} {"n" 3}] (eq pg ["SELECT generate_series(1, 3) AS n"])))

    (is (= "bpchar"
          (eq1 pg "bpchar" ["SELECT 'bpchar'::bpchar AS bpchar"]))
      "bpchar")

    (is (= "varchar"
          (eq1 pg "varchar" ["SELECT 'varchar'::varchar AS varchar"]))
      "varchar")

    (is (= {42
            "hello"
            true
            123.45M}
          (eq1 pg "record" ["SELECT ROW(42, 'hello'::text, true, 123.45::numeric) AS record"])))

    (let [uuid (random-uuid)]
      (is (= uuid (eq1 pg "uuid" ["SELECT $1 AS uuid" uuid]))))

    (let [lsn (LogSequenceNumber. 1 3427832)]
      (is (= lsn (eq1 pg "lsn" ["SELECT $1 AS lsn" lsn]))))

    (is (= (Document.
             [(Lexeme. "a" [])
              (Lexeme. "cat" [])
              (Lexeme. "fat" [])])
            (eq1 pg "tsvector" ["SELECT 'a fat cat'::tsvector"])))

    (let [tsvector (Document.
                     [(Lexeme. "a" [{:position 1 :weight \A}])
                      (Lexeme. "cat" [{:position 5 :weight \D}])
                      (Lexeme. "fat" [{:position 2 :weight \B}
                                      {:position 4 :weight \C}])])]
      (is (= tsvector
            (eq1 pg "tsvector" ["SELECT 'a:1A fat:2B,4C cat:5D'::tsvector"]))))

    (let [address (Inet. (InetAddress/getByName "10.1.0.0") :inet 0)]
      (is (= [{"inet" address}]
            (eq pg ["SELECT $1 AS inet" address]))
        "inet (v4)"))

    (let [address (Inet. (InetAddress/getByName "::1") :inet 1)]
      (is (= [{"inet" address}]
            (eq pg ["SELECT $1 AS inet" address]))
        "inet (v6)"))

    (is (same? (int 2147483647)
          (eq1 pg "int4" ["SELECT $1 AS int4" (int 2147483647)]))
      "int4")

    (let [instant (Instant/parse "2025-10-19T10:15:45.401966Z")]
      (is (= [{"timestamptz" instant}]
            (eq pg ["SELECT $1 AS timestamptz" instant]))))

    (is (= [{"timestamptz" (Instant/parse "+294277-01-09T04:00:54.775807Z")}]
          (eq pg ["SELECT 'infinity'::timestamptz"])))
    (is (= [{"timestamptz" (Instant/parse "-290278-12-22T19:59:05.224192Z")}]
          (eq pg ["SELECT '-infinity'::timestamptz"])))

    (let [local-date-time (LocalDateTime/parse "2025-10-19T10:15:45.527728")]
      (is (= [{"timestamp" local-date-time}]
            (eq pg ["SELECT $1 AS timestamp" local-date-time]))))

    (let [local-date (LocalDate/parse "2025-10-19")]
      (is (= [{"date" local-date}]
            (eq pg ["SELECT $1 AS date" local-date]))))

    (let [local-time (LocalTime/parse "10:15:45.611794")]
      (is (= [{"time" local-time}]
            (eq pg ["SELECT $1 AS time" local-time]))))

    (let [offset-time (OffsetTime/parse "08:20:55+03:00")]
      (is (= [{"timetz" offset-time}]
            (eq pg ["SELECT $1 AS timetz" offset-time]))))

    (let [interval-period (Period/parse "P10M")]
      (is (= [{"interval" interval-period}]
            (eq pg ["SELECT $1 AS interval" interval-period]))))

    (let [interval-duration (Duration/parse "PT10M")]
      (is (= [{"interval" interval-duration}]
            (eq pg ["SELECT $1 AS interval" interval-duration]))))

    (let [interval-duration (Duration/parse "PT0S")]
      (is (= [{"interval" interval-duration}]
            (eq pg ["SELECT $1 AS interval" interval-duration]))))

    (let [interval-period (Period/parse "P178000000Y")]
      (is (= interval-period
            (->
              (eq1 pg "interval" ["SELECT $1 AS interval" interval-period])
              (Period/.normalized)))))

    (with-open [reader (io/reader (eq1 pg "json" ["SELECT '{\"a\": 1}'::json"]))]
      (is (= {"a" 1} (json/read reader)) "json"))

    (with-open [reader (io/reader (eq1 pg "jsonb" ["SELECT '{\"a\": 1}'::jsonb"]))]
      (is (= {"a" 1} (json/read reader)) "jsonb"))))

(defn rand-bigdec [^long precision ^long scale]
  (let [rnd (Random.)
        digits (StringBuilder.)]
    (StringBuilder/.append digits (char (+ 49 (Random/.nextInt rnd 9))))
    (dotimes [_ (dec precision)]
      (StringBuilder/.append digits (char (+ 48 (Random/.nextInt rnd 10)))))
    (let [unscaled (BigInteger. (.toString digits))]
      (cond-> (BigDecimal. unscaled (int scale)) (Random/.nextBoolean rnd) .negate))))

(comment (rand-bigdec 1000 1000) ,,,)

(deftest ^:integration numeric
  (with-open [pg (connect-test)]
    (is (same? 123M
          (eq1 pg "n" ["SELECT 123::numeric AS n"])))

    (is (same? 123.45M
          (eq1 pg "n" ["SELECT 123.45::numeric AS n"])))

    (is (same? 0M
          (eq1 pg "n" ["SELECT 0::numeric AS n"])))

    (is (same? -42.007M
          (eq1 pg "n" ["SELECT -42.007::numeric AS n"])))

    (is (same? 99999999999999999999999999999.99999999999999999999999999999M
          (eq1 pg "n" ["SELECT 99999999999999999999999999999.99999999999999999999999999999::numeric AS n"])))

    (is (same? 0.000000000000000000000000001M
          (eq1 pg "n" ["SELECT 0.000000000000000000000000001::numeric AS n"])))

    (is (same? -0M
          (eq1 pg "n" ["SELECT -0::numeric AS n"])))

    (is (same? 1e10M
          (eq1 pg "n" ["SELECT 1e10::numeric AS n"])))

    (is (same? -12345678901234567890.1234567890123456789M
          (eq1 pg "n" ["SELECT -12345678901234567890.1234567890123456789::numeric AS n"])))

    (is (same? 1.23000M
          (eq1 pg "n" ["SELECT 1.23000::numeric AS n"])))

    (is (same? 42.000M
          (eq1 pg "n" ["SELECT 42.000::numeric AS n"])))

    (is (same? 0.123M
          (eq1 pg "n" ["SELECT .123::numeric AS n"])))

    (let [bd (rand-bigdec 1000 1000)]
      (is (same? bd
            (eq1 pg "n" [(format "SELECT %s::numeric(1000, 1000) AS n" bd)]))))

    ;; FIXME: This test is very slow because BigDecimal decoding performs poorly.
    (let [bd (rand-bigdec 131072 16383)]
      (is (same? bd
            (eq1 pg "n" [(format "SELECT %s::numeric AS n" bd)]))))))

(comment
  (deftest ^:integration join-test
    ;; FIXME: ought to be able to join such that synonymous columns are in result.
    ;;
    ;; :row-description has :table-oid, but name would be nice
    (eq pg ["DROP TABLE u"] ["DROP TABLE t"])
    (eq pg ["CREATE TABLE t (id INT NOT NULL, v TEXT)"])
    (eq pg ["CREATE TABLE u (id INT NOT NULL, v TEXT)"])
    (eq pg ["INSERT INTO t (id, v) VALUES (1, 'T')"])
    (eq pg ["INSERT INTO u (id, v) VALUES (1, 'U')"])
    (eq pg ["SELECT * FROM t JOIN u ON t.id = u.id"]))
  ,,,)

(comment
  ;; ^^
  ;; could maybe have the client do this in the background:
  ;;
  (reduce (fn [oid->table {:strs [oid schema table]}]
            (assoc oid->table oid {:schema schema :table table}))
    {}
    (eq pg ["SELECT c.relnamespace AS oid, n.nspname AS schema, c.relname AS table FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid"]))

  ;; if oid is not in cache, repopulate the cache, and if it still isn't, abort

  ,,,)

(def ^:private re-fatal-error
  #"Fatal error when reading server response; closing client to prevent protocol desynchronization")

(deftest ^:integration disconnected
  (let [pg (connect-test)]
    (AutoCloseable/.close pg)
    (is (thrown-with-msg? ExceptionInfo #"Disconnected from server; can't send"
          (eq pg ["SELECT $1" 1])))))

(comment
  (def pg (connect))
  (.close pg)
  (eq pg nil)
  (eq pg [])
  ,,,)

(deftest ^:integration event-loop-desync
  (with-open [pg (connect-test)]
    (is (= [{"a" 1}] (eq pg ["SELECT 1 AS a"])))

    ;; By default, Muutos cannot encode a Clojure map into a Postgres parameter.
    (is
      (thrown-with-msg? ExceptionInfo #"Cannot encode class clojure.lang.PersistentArrayMap"
        (eq pg ["SELECT $1" {}])))

    ;; Event loop remains in sync.
    (is (= [{"b" 2}] (eq pg ["SELECT 2 AS b"])))

    ;; Simulate a faulty user encoding function.
    (is
      (thrown-with-msg? ExceptionInfo #"Boom!"
        (eq pg ["SELECT $1" (with-meta [] {`bin/encode (fn [_] (throw (ex-info "Boom!" {})))})])))

    (is (= [{"c" 3}] (eq pg ["SELECT 3 AS c"]))))

  ;; Make a faulty :key-fn function.
  ;;
  ;; Muutos calls :key-fn when decoding messages PostgreSQL sends it. If
  ;; :key-fn throws, Muutos must close the SQL client. Else, the event loop
  ;; will get out of sync.
  (with-open [pg (connect-test :key-fn (fn [_] (throw (ex-info "Boom!" {}))))]
    (is (thrown-with-msg? ExceptionInfo re-fatal-error (eq pg ["SELECT 1"])))
    (is (thrown-with-msg? ExceptionInfo #"Disconnected from server; can't send" (eq pg ["SELECT 4 AS d"]))))

  ;; Simulate a faulty user decoding function.
  ;;
  ;; Muutos calls bin/decode when decoding messages PostgreSQL sends it. If
  ;; If bin/decode throws, Muutos must close the SQL client. Else, the event
  ;; loop will get out of sync.
  (let [decoder (get-method bin/decode (int 3802))]
    (try
      (defmethod bin/decode (int 3802) [_ _] (throw (ex-info "Boom!" {})))

      (with-open [pg (connect-test)]
        (is (thrown-with-msg? ExceptionInfo re-fatal-error (eq pg ["SELECT '{\"a\": 1}'::jsonb"])))
        (is (thrown-with-msg? ExceptionInfo #"Disconnected from server; can't send" (eq pg ["SELECT 4 AS d"]))))

      (finally
        (defmethod bin/decode (int 3802) [oid bb] (decoder oid bb))))))

(def ^:private q-raise-exception
  "CREATE OR REPLACE FUNCTION raise_exception(code TEXT, msg TEXT)
   RETURNS VOID AS $$
   BEGIN
     RAISE EXCEPTION '%', msg USING ERRCODE = code;
   END; $$ LANGUAGE plpgsql")

(deftest ^:integration raise-exception
  (with-open [pg (connect-test)]
    (eq pg [q-raise-exception])
    (is (thrown-with-msg? ExceptionInfo #"Division by zero"
          (eq pg ["SELECT raise_exception($1, $2)" "22012" "Division by zero"])))))

(deftest ^:integration copy
  (with-open [pg (connect-test)]
    (testing "COPY ... TO STDOUT"
      (eq pg ["CREATE TABLE t (a int, b int, c int)"])
      (eq pg ["INSERT INTO t VALUES ($1, $2, $3)" 1 2 3])
      (is (= ["1,2,3\n"] (eq pg ["COPY t TO STDOUT (DELIMITER ',')"]))))

    ;; FIXME: This is not the kind of error that necessarily needs to close the client.
    (testing "COPY ... FROM STDIN"
      (is (thrown-with-msg? ExceptionInfo re-fatal-error
            (eq pg ["COPY t (a) FROM STDIN"]))))))

(defmacro ^:private array-round-trips?
  [pg ary]
  `(let [a# ~ary]
     (array/equals? a# (-> (eq ~pg ["SELECT $1 AS ary" a#]) first (get "ary")))))

(deftest ^:integration array-round-trip
  (with-open [pg (connect-test)]
    (is (array-round-trips? pg (byte-array [])))
    (is (array-round-trips? pg (byte-array [1])))
    (is (array-round-trips? pg (byte-array [Byte/MIN_VALUE Byte/MAX_VALUE])))
    (is (array-round-trips? pg (boolean-array 1)))
    (is (array-round-trips? pg (boolean-array [])))
    (is (array-round-trips? pg (boolean-array [true])))
    (is (array-round-trips? pg (boolean-array [true false])))
    (is (array-round-trips? pg (char-array 1)))
    (is (array-round-trips? pg (char-array [])))
    (is (array-round-trips? pg (char-array [\a])))
    (is (array-round-trips? pg (char-array [Character/MIN_VALUE Character/MAX_VALUE])))
    (is (array-round-trips? pg (long-array 1)))
    (is (array-round-trips? pg (long-array [])))
    (is (array-round-trips? pg (long-array [1])))
    (is (array-round-trips? pg (long-array [Long/MIN_VALUE Long/MAX_VALUE])))
    (is (array-round-trips? pg (short-array 1)))
    (is (array-round-trips? pg (short-array [])))
    (is (array-round-trips? pg (short-array [1])))
    (is (array-round-trips? pg (short-array [Short/MIN_VALUE Short/MAX_VALUE])))
    (is (array-round-trips? pg (int-array 1)))
    (is (array-round-trips? pg (int-array [])))
    (is (array-round-trips? pg (int-array [1])))
    (is (array-round-trips? pg (int-array [Integer/MIN_VALUE Integer/MAX_VALUE])))
    (is (array-round-trips? pg (into-array String [])))
    (is (array-round-trips? pg (into-array String ["a"])))
    (is (array-round-trips? pg (into-array String ["a" "b"])))
    (is (array-round-trips? pg (float-array 1)))
    (is (array-round-trips? pg (float-array [])))
    (is (array-round-trips? pg (float-array [1.0])))
    (is (array-round-trips? pg (float-array [Float/MIN_VALUE Float/MAX_VALUE])))
    (is (array-round-trips? pg (double-array 1)))
    (is (array-round-trips? pg (double-array [])))
    (is (array-round-trips? pg (double-array [1.0])))
    (is (array-round-trips? pg (double-array [Double/MIN_VALUE Double/MAX_VALUE])))))

(deftest ^:integration array
  (testing "pass Java array as argument to ANY"
    (with-open [pg (connect-test {:key-fn (fn [_table-oid attr-name] (keyword attr-name))})]
      (is (set/subset? #{16 18 194 3361 3402 4600 4601 5017}
            (into (sorted-set)
              (map :oid)
              (eq pg ["SELECT oid FROM pg_type WHERE typcategory = ANY($1)" (char-array [\B \Z])])))))))

(comment
  (def pg (connect))
  #_(.close pg)

  (eq pg ["SELECT $1 AS ary" (int-array [1 2])])
  (eq pg ["SELECT $1 AS ary" (float-array [1.0 2.0])])
  ,,,)

(deftest ^:integration data-type-enum
  (with-open [pg (connect-test)]
    (eq pg
      ["CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"]
      ["CREATE TABLE person (name text, current_mood mood)"])

    (eq pg ["INSERT INTO person VALUES ($1, $2::mood)" "Moe" "happy"])

    (is (= [{"current_mood" "happy" "name" "Moe"}] (eq pg ["SELECT * FROM person"])))))

(deftest ^:integration data-type-base
  (with-open [pg (connect-test)]
    (eq pg
      ["CREATE DOMAIN us_postal_code AS TEXT CHECK(VALUE ~ '^\\d{5}$' OR VALUE ~ '^\\d{5}-\\d{4}$')"]
      ["CREATE TABLE us_snail_addy (
          address_id SERIAL PRIMARY KEY,
          city TEXT NOT NULL,
          postal us_postal_code NOT NULL
        )"])

    (eq pg ["INSERT INTO us_snail_addy (city, postal) VALUES ($1, $2::us_postal_code)" "Cornish" "03745"])

    (is (= [{"address_id" 1 "city" "Cornish" "postal" "03745"}] (eq pg ["SELECT * FROM us_snail_addy"])))))

(deftest ^:integration data-type-unknown
  (with-open [pg (connect-test)]
    (eq pg ["DROP EXTENSION IF EXISTS citext"])
    (eq pg ["CREATE EXTENSION citext"])
    (eq pg ["CREATE TABLE u (username citext PRIMARY KEY)"])
    (eq pg ["INSERT INTO u VALUES ('Alice')"])

    (is (thrown-match? ExceptionInfo
          {:kind ::error/unknown-data-type
           :oid int?
           ::anomalies/category ::anomalies/fault}
          (eq pg ["SELECT * FROM u"])))))

(deftest ^:integration geometric-types
  (with-open [pg (connect-test)]
    (let [point (Point. 1.0 2.0)]
      (is (= [{"point" point}] (eq pg ["SELECT $1 AS point" point])) "point"))

    (let [lseg (LineSegment. (Point. 1.0 2.0) (Point. 3.0 4.0))]
      (is (= [{"lseg" lseg}] (eq pg ["SELECT $1 AS lseg" lseg])) "lseg"))

    (let [path (Path. true [(Point. 1.0 2.0)])]
      (is (= [{"path" path}] (eq pg ["SELECT $1 AS path" path])) "path"))

    (let [box (Box. (Point. 3.0 4.0) (Point. 1.0 2.0))]
      (is (= [{"box" box}] (eq pg ["SELECT $1 AS box" box])) "box"))

    (let [polygon (Polygon. [(Point. 1.0 2.0)])]
      (is (= [{"polygon" polygon}] (eq pg ["SELECT $1 AS polygon" polygon])) "polygon"))

    (let [line (Line. 1.0 2.0 3.0)]
      (is (= [{"line" line}] (eq pg ["SELECT $1 AS line" line])) "line"))

    (let [circle (Circle. 1.0 2.0 3.0)]
      (is (= [{"circle" circle}] (eq pg ["SELECT $1 AS circle" circle])) "circle"))))

(deftest ^:integration ranges
  (with-open [pg (connect-test)]
    (testing "empty"
      (is (= [{"int4range" (Range. nil false nil false false)}]
            (eq pg ["SELECT '(,)'::int4range"])))

      (is (= [{"int4range" (Range. nil false nil false false)}]
            (eq pg ["SELECT '[,)'::int4range"])))

      (is (= [{"int4range" (Range. nil false nil false false)}]
            (eq pg ["SELECT '(,]'::int4range"])))

      (is (= [{"int4range" (Range. nil false nil false false)}]
            (eq pg ["SELECT '[,]'::int4range"]))))

    (testing "no lower bound"
      (is (= [{"int4range" (Range. nil false 20 false false)}]
            (eq pg ["SELECT '[,20)'::int4range"]))))

    (testing "no upper bound"
      (is (= [{"int4range" (Range. 10 true nil false false)}]
            (eq pg ["SELECT '[10,)'::int4range"]))))

    (testing "inclusive / exclusive"
      (is (= [{"int4range" (Range. 11 true 20 false false)}]
            (eq pg ["SELECT '(10,20)'::int4range"])))

      (is (= [{"int4range" (Range. 11 true 21 false false)}]
            (eq pg ["SELECT '(10,20]'::int4range"])))

      (is (= [{"int4range" (Range. 10 true 21 false false)}]
            (eq pg ["SELECT '[10,20]'::int4range"])))

      (is (= [{"int4range" (Range. 10 true 20 false false)}]
            (eq pg ["SELECT '[10,20)'::int4range"]))))

    (testing "round-trip inclusive / exclusive"
      (is (= [{"int4range" (Range. 10 true 20 false false)}]
            (eq pg ["SELECT $1 AS int4range" (Range. 10 true 20 false false)]))))

    (is (= [{"int4range" (Range. 10 true 21 false false)}]
          (eq pg ["SELECT $1 AS int4range" (Range. 10 true 20 true false)])))

    (testing "numrange"
      (is (= [{"numrange" (Range. 10.0M true 20.0M false false)}]
            (eq pg ["SELECT numrange(10.0::numeric, 20.0::numeric)"])))

      (is (= [{"numrange" (Range. 0M true 0M false false)}]
            (eq pg ["SELECT numrange(0::numeric, 1::numeric)"])))

      (is (= [{"numrange" (Range. 0M true 0M true false)}]
            (eq pg ["SELECT numrange(0::numeric, 1::numeric, '[]')"])))

      (is (= [{"numrange" (Range. 0M false 0M true false)}]
            (eq pg ["SELECT numrange(0::numeric, 1::numeric, '(]')"])))

      (is (= [{"numrange" (Range. 0M true 0M false false)}]
            (eq pg ["SELECT numrange(0::numeric, 1::numeric, '[)')"])))

      (is (= [{"numrange" (Range. 0M false 0M false false)}]
            (eq pg ["SELECT numrange(0::numeric, 1::numeric, '()')"]))))

    (let [tsrange (Range.
                    (LocalDateTime/parse "2010-01-01T14:30")
                    true
                    (LocalDateTime/parse "2010-01-01T15:30")
                    false
                    false)]
      (is (= [{"tsrange" tsrange}]
            (eq pg ["SELECT $1 AS tsrange" tsrange]))))

    (let [tstzrange (Range.
                      (Instant/parse "2023-01-01T00:00:00.000000Z")
                      true
                      (Instant/parse "2023-12-31T23:59:59.999999Z")
                      false
                      false)]
      (is (= [{"tstzrange" tstzrange}]
            (eq pg ["SELECT $1 AS tstzrange" tstzrange]))))

    (let [daterange (Range.
                           (LocalDate/parse "2023-01-01")
                           true
                           (LocalDate/parse "2023-12-31")
                           false
                           false)]
      (is (= [{"daterange" daterange}]
            (eq pg ["SELECT $1 AS daterange" daterange]))))

    (let [int8range (Range. 10 true 20 false false)]
      (is (= [{"int8range" int8range}]
            (eq pg ["SELECT $1 AS int8range" int8range]))))))

(deftest ^:integration set-parameter
  (with-open [pg (connect-test)]
    (is (= [] (eq pg ["SET search_path TO public"])))
    (is (= {:parameter ["TimeZone" "Europe/Helsinki"] :type :parameter}
          (eq pg ["SET TIME ZONE 'Europe/Helsinki'"])))))

(deftest ^:integration system-catalogs
  (with-open [pg (connect-test)]
    (is (seq (into []
               (map (fn [rel] (eq pg [(format "SELECT * FROM pg_catalog.%s" rel)])))
               ;; The relations outside this set have e.g. aclitem columns, which don't
               ;; have a binary representation.
               ;;
               ;; TODO: Add support for OID 2277 (anyarray)?
               #{"pg_aggregate"
                 "pg_am"
                 "pg_amop"
                 "pg_amproc"
                 "pg_attrdef"
                 "pg_auth_members"
                 "pg_authid"
                 "pg_cast"
                 "pg_collation"
                 "pg_constraint"
                 "pg_conversion"
                 "pg_db_role_setting"
                 "pg_default_acl"
                 "pg_depend"
                 "pg_description"
                 "pg_enum"
                 "pg_event_trigger"
                 "pg_extension"
                 "pg_foreign_data_wrapper"
                 "pg_foreign_server"
                 "pg_foreign_table"
                 "pg_index"
                 "pg_inherits"
                 "pg_language"
                 "pg_largeobject"
                 "pg_largeobject_metadata"
                 "pg_opclass"
                 "pg_operator"
                 "pg_opfamily"
                 "pg_parameter_acl"
                 "pg_partitioned_table"
                 "pg_policy"
                 "pg_publication"
                 "pg_publication_namespace"
                 "pg_publication_rel"
                 "pg_range"
                 "pg_replication_origin"
                 "pg_rewrite"
                 "pg_seclabel"
                 "pg_sequence"
                 "pg_shdepend"
                 "pg_shdescription"
                 "pg_shseclabel"
                 "pg_statistic_ext"
                 "pg_statistic_ext_data"
                 "pg_subscription"
                 "pg_subscription_rel"
                 "pg_tablespace"
                 "pg_transform"
                 "pg_trigger"
                 "pg_ts_config"
                 "pg_ts_config_map"
                 "pg_ts_dict"
                 "pg_ts_parser"
                 "pg_ts_template"
                 "pg_type"
                 "pg_user_mapping"})))))

(deftest nil-val-strategy
  (let [uuid #uuid "8c109ffb-8cd0-456b-8c65-35fe143bd49c"]
    (with-open [pg (connect-test)]
      (eq pg ["create table t (id integer primary key generated always as identity, t text null, rid uuid)"])

      (is (match? [{"id" 1 "t" matchers/absent "rid" uuid}]
            (eq pg ["insert into t (t, rid) values ($1, $2) returning *" nil uuid])))

      (eq pg ["update t set t = $2 where id = $1" 1 "v"])

      (is (match? [{"id" 1 "t" matchers/absent "rid" uuid}]
            (eq pg ["update t set t = $2 where id = $1 returning *" 1 nil])))

      (is (match? [{"id" 1 "t" matchers/absent "rid" uuid}]
            (eq pg ["select * FROM t"]))))))
