(ns muutos.sql-client.sq-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [cognitect.anomalies :as-alias anomalies]
            [matcher-combinators.test]
            [muutos.error :as-alias error]
            [muutos.sql-client :refer [connect eq sq] :as sut]
            #_[muutos.test.fray :as fray]
            [muutos.test.server :as server :refer [host port]]
            [muutos.type])
  (:import (clojure.lang ExceptionInfo)
           (java.net InetAddress)
           (muutos.type LogSequenceNumber)))

(def container-opts
  {:env-vars {"POSTGRES_PASSWORD" "postgres"
              "POSTGRES_DB" "test"}
   :exposed-ports [5432]
   :image-name "postgres:17"})

(defonce server
  (delay (server/start container-opts)))

(comment (.close @server) ,,,)

(use-fixtures :each
  (fn [f]
    (with-open [client (connect :host (host @server) :port (port @server))]
      (sq client "DROP DATABASE test WITH (FORCE)")
      (sq client "CREATE DATABASE test"))

    (f)))

(defmacro same? [a b]
  `(zero? (.compareTo ~a ~b)))

(defn connect-test [& {:as opts}]
  (connect (merge {:database "test" :host (host @server) :port (port @server)} opts)))

(comment
  (with-open [pg (connect-test)]
    (sq pg "SELECT 3.14::float4"))

  (def pg (connect-test))
  ,,,)

(defn sq1 [pg k q]
  (-> (sq pg q) (first) (get k)))

(deftest ^:integration simple-query
  (with-open [pg (connect-test)]
    (is (thrown-with-msg? ExceptionInfo #"relation \"a\" does not exist"
          (sq pg "SELECT * FROM a"))
      "Bad query")

    (is (thrown-with-msg? ExceptionInfo #"syntax error at or near \"E'a'\""
          (sq pg "E'a'::bytea"))
      "Very bad query")

    (is (= [] (sq pg ""))
      "Empty query string")

    (is (= [{}] (sq pg "SELECT NULL"))
      "NULL")

    (is (= [{"a" 1}] (sq pg "SELECT 1 AS a"))
      "Basic select")

    (is (= [{"a" 1} {"b" 2}] (sq pg "SELECT 1 AS a; SELECT 2 AS b"))
      "Multiple queries")

    (is (thrown-with-msg? ExceptionInfo #"relation \"a\" does not exist"
          (sq pg "SELECT 1 AS a; SELECT * FROM a; SELECT 3 AS c"))
      "Good query, bad query, good query")

    (is (= [] (sq pg "CREATE TABLE t (a int)")))
    (is (= [] (sq pg "INSERT INTO t VALUES (1)")) "INSERT")
    (is (= [{"a" 1}] (sq pg "SELECT * FROM t")))
    (is (= [] (sq pg "UPDATE t SET a = 2")) "UPDATE")
    (is (= [{"a" 2}] (sq pg "SELECT * FROM t")))
    (is (= [{"a" 3}] (sq pg "INSERT INTO t VALUES (3) RETURNING *")) "INSERT ... RETURNING")
    (is (= [] (sq pg "DELETE FROM t")) "DELETE")
    (is (= [] (sq pg "SELECT * FROM t")))
    (is (= [] (sq pg "DROP TABLE t")))))

;; https://www.postgresql.org/docs/current/datatype.html#DATATYPE-TABLE
(deftest ^:integration data-types
  (with-open [pg (connect-test)]
    (is (same? (long 9223372036854775807)
          (sq1 pg "int8" "SELECT 9223372036854775807::int8"))
      "int8")

    #_bigserial

    (is (= "101" (sq1 pg "bit" "SELECT '101'::bit(3)"))
      "bit")

    (is (= "010" (sq1 pg "varbit" "SELECT '010'::varbit(3)"))
      "bit varying")

    (is (= [{"bool" true}] (sq pg "SELECT TRUE::bool"))
      "boolean (true)")

    (is (= [{"bool" false}] (sq pg "SELECT FALSE::bool"))
      "boolean (false)")

    #_box

    (is (= [{"char" \A}] (sq pg "SELECT 'A'::\"char\""))
      "char")

    (is (= [{"bpchar" "A"}] (sq pg "SELECT 'A'::char(1)"))
      "character")

    (is (= [{"varchar" "A"}] (sq pg "SELECT 'A'::varchar(256)"))
      "character varying")

    #_cidr
    #_circle

    (is (= [{"date" #time/date "2025-01-01"}] (sq pg "SELECT '2025-01-01'::date"))
      "date")

    (is (same? (double 2.718281828459045)
          (sq1 pg "float8" "SELECT 2.718281828459045::float8"))
      "double precision")

    (is (= [{"inet" (InetAddress/getByName "10.1.0.0")}]
          (sq pg "SELECT '10.1.0.0'::inet"))
      "inet (v4)")

    (is (= [{"inet" (InetAddress/getByName "::1")}]
          (sq pg "SELECT '::1'::inet"))
      "inet (v6)")

    (is (same? (int 2147483647)
          (sq1 pg "int4" "SELECT 2147483647::int4"))
      "integer")

    #_interval

    (is (= [{"json" "{}"}] (sq pg "SELECT '{}'::json"))
      "json")

    (is (= [{"jsonb" "{}"}] (sq pg "SELECT '{}'::jsonb"))
      "jsonb")

    #_line
    #_lseg

    (is (= [{"macaddr" "aa:bb:cc:dd:ee:ff"}] (sq pg "SELECT 'AA:BB:CC:DD:EE:FF'::macaddr"))
      "macaddr")

    (is (= [{"macaddr8" "aa:bb:cc:ff:fe:dd:ee:ff"}] (sq pg "SELECT 'AA:BB:CC:DD:EE:FF'::macaddr8"))
      "macaddr8")

    (is (same? 1.0M (sq1 pg "money" "SELECT 1.0::money"))
      "money")

    (is (= [{"numeric" 12345.6789M}] (sq pg "SELECT 12345.6789::numeric"))
      "numeric")

    #_path

    (is (= [{"pg_lsn" (LogSequenceNumber. 22 -1284188088)}]
          (sq pg "SELECT '16/B374D848'::pg_lsn"))
      "pg_lsn")

    #_pg_snapshot

    #_point
    #_polygon

    (is (same? (float 3.14) (sq1 pg "float4" "SELECT 3.14::float4"))
      "real")

    (is (same? (short 32767) (sq1 pg "int2" "SELECT 32767::int2"))
      "smallint")

    #_smallserial
    #_serial

    (is (= [{"text" "text"}] (sq pg "SELECT 'text'::text"))
      "text")

    (is (= [{"time" #time/time "15:30:45"}] (sq pg "SELECT '15:30:45'::time"))
      "time")

    (is (= [{"timetz" #time/offset-time "15:30:45+02:00"}] (sq pg "SELECT '15:30:45+02'::timetz"))
      "timetz")

    (is (= [{"timestamp" #time/date-time "2025-01-01T15:30:45"}]
          (sq pg "SELECT '2025-01-01 15:30:45'::timestamp"))
      "timestamp")

    (is (= [{"timestamptz" #time/offset-date-time "2004-10-19T08:23:54Z"}]
          (sq pg "SELECT '2004-10-19 10:23:54+02'::timestamptz"))
      "timestamptz")

    #_tsquery
    #_tsvector
    #_txid_snapshot

    (is (= [{"uuid" #uuid "5b603992-cc37-4cfc-9da7-d19213c1c7bc"}]
          (sq pg "SELECT '5b603992-cc37-4cfc-9da7-d19213c1c7bc'::uuid"))
      "uuid")

    #_xml))

(deftest ^:integration copy
  (with-open [pg (connect-test)]
    (testing "COPY ... TO STDOUT"
      (sq pg "CREATE TABLE t (a int, b int, c int)")
      (sq pg "INSERT INTO t VALUES (1, 2, 3)")
      (is (= ["1,2,3\n"] (sq pg "COPY t TO STDOUT (DELIMITER ',')"))))

    (testing "COPY ... FROM STDIN"
      (is (thrown-with-msg? ExceptionInfo #"Fatal error when reading server response; closing client to prevent protocol desynchronization"
            (sq pg "COPY t (a) FROM STDIN"))))))

;; I'm just learning to use Fray here, not sure this test makes sense.
;;
;; I'm not really sure, but it seems to be Fray wipes locals between iterations.
#_(deftest ^:integration fray
    (fray/run {:iterations 100}
      (with-open [pg (connect-test)]
        (let [uuid (random-uuid)]
          (is (= [{"uuid" uuid}] (eq pg "SELECT $1 AS uuid" [uuid])))))))

(deftest ^:integration event-loop-desync
  ;; Make a faulty :key-fn function.
  ;;
  ;; Muutos calls :key-fn when decoding messages PostgreSQL sends it. If
  ;; :key-fn throws, Muutos must close the SQL client. Else, the event loop
  ;; will get out of sync.
  (with-open [pg (connect-test :key-fn (fn [_] (throw (ex-info "Boom!" {}))))]
    (is (thrown-with-msg? ExceptionInfo #"Fatal error when reading server response; closing client to prevent protocol desynchronization" (sq pg "SELECT 1")))
    (is (thrown-with-msg? ExceptionInfo #"Disconnected from server; can't send" (sq pg "SELECT 4 AS d")))))

(deftest ^:integration data-type-enum
  (with-open [pg (connect-test)]
    (eq pg
      ["CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"]
      ["CREATE TABLE person (name text, current_mood mood)"])

    (sq pg "INSERT INTO person VALUES ('Moe', 'happy'::mood)")

    (is (= [{"current_mood" "happy" "name" "Moe"}] (sq pg "SELECT * FROM person")))))

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

    (is (= [{"address_id" 1 "city" "Cornish" "postal" "03745"}] (sq pg "SELECT * FROM us_snail_addy")))))

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
          (sq pg "SELECT * FROM u")))))

(deftest ^:integration set-parameter
  (with-open [pg (connect-test)]
    (is (= [] (sq pg "SET search_path TO public")))))
