(ns muutos.sql-client.lq-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is use-fixtures]]
            [cognitect.anomalies :as-alias anomalies]
            [matcher-combinators.test]
            [muutos.impl.statement :as stmt]
            [muutos.sql-client :refer [connect eq oid] :as sql]
            [muutos.test.concurrency :refer [concurrently]]
            [muutos.error :as-alias error]
            [muutos.type])
  (:import (clojure.lang ExceptionInfo IReduceInit)
           (java.lang AutoCloseable)
           (java.util.concurrent ArrayBlockingQueue)))

(set! *warn-on-reflection* true)

(use-fixtures :each
  (fn [f]
    (with-open [client (connect :port 5432)]
      (eq client ["DROP DATABASE IF EXISTS test WITH (FORCE)"])
      (eq client ["CREATE DATABASE test"]))

    (f)))

(defn ^:private key-fn [_ attr-name] (keyword attr-name))

(defn $ ^AutoCloseable [& {:as opts}]
  (connect
    (merge {:database "test" :key-fn key-fn :port 5432} opts)))

(deftest infer
  (with-open [pg ($)]
    ;; Without second parameter, Muutos passes 0, telling PostgreSQL to infer
    ;; parameter types.
    (let [oid-by-category (sql/lq "SELECT oid FROM pg_type WHERE typcategory = ANY($1)")]
      (is (set/subset? #{{:oid 16}
                         {:oid 18}
                         {:oid 194}
                         {:oid 3361}
                         {:oid 3402}
                         {:oid 5017}
                         {:oid 4600}
                         {:oid 4601}}
            (into #{} (oid-by-category pg (char-array [\B \Z]))))))))

(deftest multiple-clients
  (with-open [pg-1 ($)
              pg-2 ($)]
    (let [catenate (sql/lq "SELECT $1||$2 AS s")]
      (is (= [{:s "ab"}] (into [] (catenate pg-1 "a" "b"))))
      (is (= [{:s "ba"}] (into [] (catenate pg-2 "b" "a")))))))

(deftest explicit-oids
  (with-open [pg ($)]
    (let [sum (sql/lq "SELECT $1 + $2 AS n" {:oids [(oid :int8) (oid :int8)]})]
      (is (= [{:n 3}] (into [] (sum pg 1 2)))))))

(deftest xform
  (with-open [pg ($)]
    (let [oid-by-category (sql/lq "SELECT oid FROM pg_type WHERE typcategory = ANY($1)")]
      (is (set/subset? #{32 36 388}
            (into #{}
              (comp
                (filter (fn [{:keys [oid]}] (< oid 1000)))
                (map (fn [{:keys [oid]}] (* 2 oid))))
              (oid-by-category pg (char-array [\B \Z]))))))))

(deftest xform-reducible
  (with-open [pg ($)]
    (let [oid-by-category (sql/lq "SELECT oid FROM pg_type WHERE typcategory = ANY($1)")]
      (is (= {:oid 3361}
            (transduce
              (halt-when (fn [{:keys [oid]}] (> oid 1000)))
              conj
              {}
              (oid-by-category pg (char-array [\B \Z]))))))))

(deftest xform-throw
  (with-open [pg ($)]
    (let [sum (sql/lq "SELECT $1 + $1" {:oids [(oid :int8)]})]
      (is (thrown? Exception (into [] (map (fn [_] (throw (Exception. "Boom!")))) (sum pg 1 2))))

      ;; No protocol desynchronization
      (is (= [{:n 1}] (eq pg ["SELECT $1 AS n" 1]))))))


;; TODO: Add test for multiple clients (interlace)

(deftest close-by-name
  (with-open [pg ($)]
    (let [sum (sql/lq "SELECT $1 + $2 AS n" {:name 'sum :oids [(oid :int8)]} )]
      ;; Call the latent query to force Muutos to send it to PostgreSQL to
      ;; parse and describe.
      (is (= [{:n 3}] (into [] (sum pg 1 2))))

      ;; Close the statement.
      (stmt/close pg 'sum)

      ;; Prepared statement is closed, attempting to call it throws.
      (is (thrown-match? ExceptionInfo {:cause :ERRCODE-UNDEFINED-PSTATEMENT
                                        :error-code "26000"
                                        :kind ::error/server-error
                                        :severity "ERROR"}
            (into [] (sum pg 3 4)))))))

(deftest close-before-execute
  (with-open [pg ($)]
    (let [sum (sql/lq "SELECT $1 + $2 AS n" {:name 'sum :oids [(oid :int8)]})]
      (stmt/close pg 'sum)
      ;; Closing a prepared statement that hasn't been reduced doesn't throw.
      (is (instance? IReduceInit (sum pg 1 2))))))

(deftest parse-error
  (with-open [pg ($)]
    (let [bad (sql/lq "SELECT bad")]
      (is (thrown-match? ExceptionInfo {:cause :undefined-column
                                        :error-code "42703"
                                        :kind ::error/server-error
                                        :severity "ERROR"}
            (into [] (bad pg)))))

    ;; No protocol desynchronization
    (is (= [{:n 1}] (eq pg ["SELECT $1 AS n" 1])))

    (let [sum (sql/lq "SELECT $1 + $2 AS n" {:oids [(oid :int8)]})]
      (is (= #{{:n 3}} (into #{} (sum pg 1 2)))))))

(deftest no-parameter
  (with-open [pg ($)]
    (let [no-param (sql/lq "SELECT 1 AS n")]
      (is (= [{:n 1}] (into [] (no-param pg)))))))

(deftest returning
  (with-open [pg ($)]
    (eq pg ["CREATE TABLE t (a int8, b text)"])

    (let [put-t (sql/lq "INSERT INTO t (a, b) VALUES ($1, $2) RETURNING a, b")]
      (is (= [{:a 1 :b "c"}] (into [] (put-t pg 1 "c")))))))

(deftest bad-parameter
  (with-open [pg ($)]
    (let [sum (sql/lq "SELECT $1 + $2 AS n" {:oids [(oid :int8)]})]
      (is (thrown-match? ExceptionInfo {::anomalies/category
                                        ::anomalies/unsupported}
            (into #{} (sum pg [1 2]))))

      (is (= #{{:n 3}} (into #{} (sum pg 1 2)))))))

(deftest already-exists
  (with-open [pg ($)]
    (let [sum (sql/lq "SELECT $1 + $2 AS n" {:name 'sum :oids [(oid :int8)]})
          sum-dupe (sql/lq "SELECT $1 + $2 AS n" {:name 'sum :oids [(oid :int8)]})]
      (is (= #{{:n 3}} (into #{} (sum pg 1 2))))

      (is (thrown-match? ExceptionInfo {:cause :duplicate-prepared-statement
                                        :error-code "42P05"
                                        :kind ::error/server-error
                                        :severity "ERROR"}
            (into #{} (sum-dupe pg 1 2))))

      (is (= #{{:n 7}} (into #{} (sum pg 3 4)))))))

(deftest protocol-violation
  (with-open [pg ($)]
    (let [sum (sql/lq "SELECT $1 + $2 AS n" {:oids [(oid :int8)]})]
      (is (thrown-match? {:cause :protocol-violation
                          :error-code "08P01"
                          :kind ::error/server-error
                          :severity "ERROR"}
            (into [] (sum pg 1))))

      ;; No protocol desynchronization
      (is (= [{:n 3}] (into [] (sum pg 1 2)))))))

(def q-raise-notice
  "CREATE OR REPLACE FUNCTION pg_temp.raise_notice(msg TEXT) RETURNS void AS $$
   BEGIN
     RAISE NOTICE '%', msg;
   END;
   $$ LANGUAGE plpgsql")

(deftest notice
  (with-open [pg ($)]
    (eq pg [q-raise-notice])

    (let [notice (sql/lq "SELECT pg_temp.raise_notice($1)")]
      ;; Not quite sure only logging is the best way to handle them. Could
      ;; maybe allow users to pass a callback that gets called on notices.
      (is (= [{}] (into [] (notice pg "Hello, world!")))))))

(deftest empty-query
  (with-open [pg ($)]
    (eq pg [q-raise-notice])

    (let [void (sql/lq "")]
      (is (= [] (into [] (void pg)))))))

(deftest parameter
  (with-open [pg ($)]
    (let [set-time-zone (sql/lq "SET TIME ZONE 'Pacific/Midway'")]
      (is (= [["TimeZone" "Pacific/Midway"]] (into [] (set-time-zone pg))))
      (is (= [{:n 1}] (eq pg ["SELECT 1 AS n"]))))))

(deftest copy-data
  (with-open [pg ($)]
    (let [copy-data (sql/lq "COPY (SELECT 1) TO STDOUT")]
      (is (= ["1\n"] (into [] (copy-data pg))))
      (is (= [{:n 1}] (eq pg ["SELECT 1 AS n"]))))))

(deftest no-data
  (with-open [pg ($)]
    (let [no-data (sql/lq "SELECT FROM pg_type WHERE FALSE")]
      (is (= [] (into [] (no-data pg)))))))

(def ^:private q-t-by-ids
  "SELECT * FROM t WHERE id = ANY($1) ORDER BY a ASC")

(deftest table
  (with-open [pg ($)]
    (eq pg
      ["CREATE TABLE t (id int PRIMARY KEY, a int)"]
      ["INSERT INTO t (id, a) VALUES (1, 10), (2, 20), (3, 30)"])

    (let [t-by-ids (sql/lq q-t-by-ids)]
      (is (= [{:id 1 :a 10}
              {:id 2 :a 20}
              {:id 3 :a 30}]
            (into [] (t-by-ids pg (int-array [1 2 3]))))))))

(deftest interlace
  (with-open [pg ($)]
    ;; The order in which you execute the prepared statements does not matter.
    ;;
    ;; If we (incorrectly) did Bind -> Execute -> Sync outside of reduce, it would.
    (let [sum (sql/lq "SELECT $1 + $2 AS n" {:oids [(oid :int8)]})
          product (sql/lq "SELECT $1 * $2 AS n" {:oids [(oid :int8)]})]
      (let [s (sum pg 1 2)
            p (product pg 3 4)]
        (is (= [{:n 3}] (into [] s)))
        (is (= [{:n 12}] (into [] p))))

      (let [p (product pg 3 4)
            s (sum pg 1 2)]
        (is (= [{:n 3}] (into [] s)))
        (is (= [{:n 12}] (into [] p)))))))

(deftest alter-table
  (with-open [pg ($)]
    (eq pg
      ["CREATE TABLE t (id int PRIMARY KEY, a int)"]
      ["INSERT INTO t (id, a) VALUES (1, 10)"])

    (let [t-by-ids (sql/lq q-t-by-ids)]
      (is (= [{:id 1 :a 10}] (into [] (t-by-ids pg (int-array [1])))))

      (eq pg
        ["ALTER TABLE t ADD COLUMN b int"]
        ["UPDATE t SET b = 100 WHERE id = 1"]
        ["INSERT INTO t (id, a, b) VALUES (2, 20, 200)"])

      ;; Statement that works after ALTER
      (is (= [{:id 1 :a 10 :b 100}
              {:id 2 :a 20 :b 200}]
            (into [] (t-by-ids pg (int-array [1 2])))
            (eq pg [q-t-by-ids (int-array [1 2])])))

      (eq pg ["ALTER TABLE t DROP COLUMN a"])

      (is (thrown-match? ExceptionInfo {:cause :undefined-column
                                        :error-code "42703"
                                        :kind ::error/server-error
                                        :severity "ERROR"}
            (into [] (t-by-ids pg (int-array [1 2])))))

      ;; No protocol desynchronization
      (is (= [{:b 100 :id 1} {:b 200 :id 2}] (eq pg ["SELECT * FROM t"]))))))

(deftest contention
  (let [n 1000
        q (ArrayBlockingQueue. n)]
    (with-open [pg-1 ($) pg-2 ($) pg-3 ($) pg-4 ($) pg-5 ($)]
      (let [uuid-id (sql/lq "SELECT $1 AS uuid" {:oids [(oid :uuid)]})]
        (concurrently {:threads n}
          (let [uuid (random-uuid)]
            (.put q (= [{:uuid uuid}] (into [] (uuid-id (rand-nth [pg-1 pg-2 pg-3 pg-4 pg-5]) uuid))))))

        (let [v (volatile! [])]
          (dotimes [_ n]
            (vswap! v conj (ArrayBlockingQueue/.take q)))

          (is (= n (count @v)))
          (is (every? true? @v)))))))

(deftest enum
  (with-open [pg ($)]
    (eq pg ["CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed')"])

    (let [echo-type (sql/lq "SELECT $1::bug_status")]
      (is (= [{:bug_status "open"}] (into [] (echo-type pg "open")))))))

(deftest apply-fn
  (with-open [pg ($)]
    (let [sum (sql/lq "SELECT $1 + $2 AS n" {:oids [(oid :int8) (oid :int8)]})]
      (is (= [{:n 3}] (into [] (apply sum [pg 1 2])))))))

(deftest twenty-one-args
  (with-open [pg ($)]
    (let [sum (sql/lq "SELECT $1||$2||$3||$4||$5||$6||$7||$8||$9||$10||$11||$12||$13||$14||$15||$16||$17||$18||$19||$20||$21 AS s")]
      (is (= [{:s "aaaaaaaaaaaaaaaaaaaaa"}]
            (into [] (sum pg "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" "a" )))))))
