(ns muutos.sql-client.prepare-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is use-fixtures]]
            [cognitect.anomalies :as-alias anomalies]
            [matcher-combinators.test]
            [muutos.sql-client :refer [connect eq] :as sql]
            [muutos.error :as-alias error]
            [muutos.type])
  (:import (clojure.lang ExceptionInfo IReduceInit)
           (java.lang AutoCloseable)))

(set! *warn-on-reflection* true)

(use-fixtures :each
  (fn [f]
    (with-open [client (connect :port 5432)]
      (eq client ["DROP DATABASE test WITH (FORCE)"])
      (eq client ["CREATE DATABASE test"]))

    (f)))

(defn ^:private key-fn [_ attr-name] (keyword attr-name))

(defn $ ^AutoCloseable [& {:as opts}]
  (connect
    (merge {:database "test" :key-fn key-fn :port 5432} opts)))

(deftest infer
  (with-open [pg ($)
              ;; Without :oids, Muutos passes 0, telling PostgreSQL to infer parameter types.
              oid-by-category (sql/prepare pg "SELECT oid FROM pg_type WHERE typcategory = ANY($1)")]
    (is (set/subset? #{{:oid 16}
                       {:oid 18}
                       {:oid 194}
                       {:oid 3361}
                       {:oid 3402}
                       {:oid 5017}
                       {:oid 4600}
                       {:oid 4601}}
          (into #{} (oid-by-category (char-array [\B \Z])))))))

(deftest explicit-oids
  (with-open [pg ($)
              sum (sql/prepare pg "SELECT $1 + $2 AS n" {:oids [(int 20) (int 20)]})]
    (is (= [{:n 3}] (into [] (sum 1 2))))))

(deftest xform
  (with-open [pg ($)
              oid-by-category (sql/prepare pg "SELECT oid FROM pg_type WHERE typcategory = ANY($1)" {:name 'oid-by-category})]
    (is (set/subset? #{32 36 388}
          (into #{}
            (comp
              (filter (fn [{:keys [oid]}] (< oid 1000)))
              (map (fn [{:keys [oid]}] (* 2 oid))))
            (oid-by-category (char-array [\B \Z])))))))

(deftest xform-reducible
  (with-open [pg ($)
              oid-by-category (sql/prepare pg "SELECT oid FROM pg_type WHERE typcategory = ANY($1)" {:name 'oid-by-category})]
    (is (= {:oid 3361}
          (transduce
            (halt-when (fn [{:keys [oid]}] (> oid 1000)))
            conj
            {}
            (oid-by-category (char-array [\B \Z])))))))

(deftest xform-throw
  (with-open [pg ($)
              sum (sql/prepare pg "SELECT $1 + $1" {:oids [(int 20) (int 20)]})]
    (is (thrown? Exception (into [] (map (fn [_] (throw (Exception. "Boom!")))) (sum 1 2))))

    ;; No protocol desynchronization
    (is (= [{:n 1}] (eq pg ["SELECT $1 AS n" 1])))))

(deftest close-by-name
  (with-open [pg ($)]
    (let [sum (sql/prepare pg "SELECT $1 + $2 AS n" {:name 'sum :oids [(int 20) (int 20)]})]
      (.close sum)

      ;; Prepared statement is closed, attempting to call it throws.
      (is (thrown-match? ExceptionInfo {:cause :ERRCODE-UNDEFINED-PSTATEMENT
                                        :error-code "26000"
                                        :kind ::error/server-error
                                        :severity "ERROR"}
            (into [] (sum 1 2)))))))

(deftest close-fn
  (with-open [pg ($)]
    (let [sum (sql/prepare pg "SELECT $1 + $2 AS n" {:name 'sum :oids [(int 20) (int 20)]})]
      (AutoCloseable/.close sum)
      (is (thrown-match? ExceptionInfo {:cause :ERRCODE-UNDEFINED-PSTATEMENT
                                        :error-code "26000"
                                        :kind ::error/server-error
                                        :severity "ERROR"}
            (into [] (sum 1 2)))))))

(deftest close-before-execute
  (with-open [pg ($)
              sum (sql/prepare pg "SELECT $1 + $2 AS n" {:oids [(int 20) (int 20)]})]
    ;; Closing a prepared statement that hasn't been reduced doesn't throw.
    (is (instance? IReduceInit (sum 1 2)))))

(deftest parse-error
  (with-open [pg ($)]
    (is (thrown-match? ExceptionInfo {:cause :undefined-column
                                      :error-code "42703"
                                      :kind ::error/server-error
                                      :severity "ERROR"}
          (sql/prepare pg "SELECT bad")))

    ;; No protocol desynchronization
    (is (= [{:n 1}] (eq pg ["SELECT $1 AS n" 1])))

    (with-open [sum (sql/prepare pg "SELECT $1 + $2 AS n" {:oids [(int 20) (int 20)]})]
      (is (= #{{:n 3}} (into #{} (sum 1 2)))))))

(deftest no-parameter
  (with-open [pg ($)
              no-param (sql/prepare pg "SELECT 1 AS n")]
    (is (= [{:n 1}] (into [] (no-param))))))

(deftest returning
  (with-open [pg ($)]
    (eq pg ["CREATE TABLE t (a int8, b text)"])

    (with-open [put-t (sql/prepare pg "INSERT INTO t (a, b) VALUES ($1, $2) RETURNING a, b")]
      (is (= [{:a 1 :b "c"}] (into [] (put-t 1 "c")))))))

(deftest bad-parameter
  (with-open [pg ($)
              sum (sql/prepare pg "SELECT $1 + $2 AS n" {:oids [(int 20) (int 20)]})]
    (is (thrown-match? ExceptionInfo {::anomalies/category
                                      ::anomalies/unsupported}
          (into #{} (sum [1 2]))))

    (is (= #{{:n 3}} (into #{} (sum 1 2))))))

(deftest already-exists
  (with-open [pg ($)
              sum (sql/prepare pg "SELECT $1 + $2 AS n" {:name 'sum :oids [(int 20) (int 20)]})]
    (is (thrown-match? {:cause :duplicate-prepared-statement
                        :error-code "42P05"
                        :kind ::error/server-error
                        :severity "ERROR"}
          (sql/prepare pg "SELECT $1 + $2 AS n" {:name 'sum :oids [(int 20) (int 20)]})))

    (is (= #{{:n 3}} (into #{} (sum 1 2))))))

(deftest protocol-violation
  (with-open [pg ($)
              sum (sql/prepare pg "SELECT $1 + $2 AS n" {:oids [(int 20) (int 20)]})]
    (is (thrown-match? {:cause :protocol-violation
                        :error-code "08P01"
                        :kind ::error/server-error
                        :severity "ERROR"}
          (into [] (sum 1))))

    ;; No protocol desynchronization
    (is (= [{:n 3}] (into [] (sum 1 2))))))

(def q-raise-notice
  "CREATE OR REPLACE FUNCTION pg_temp.raise_notice(msg TEXT) RETURNS void AS $$
   BEGIN
     RAISE NOTICE '%', msg;
   END;
   $$ LANGUAGE plpgsql")

(deftest notice
  (with-open [pg ($)]
    (eq pg [q-raise-notice])

    (let [notice (sql/prepare pg "SELECT pg_temp.raise_notice($1)")]
      ;; Not quite sure only logging is the best way to handle them. Could
      ;; maybe allow users to pass a callback that gets called on notices.
      (is (= [{}] (into [] (notice "Hello, world!")))))))

(deftest empty-query
  (with-open [pg ($)]
    (eq pg [q-raise-notice])

    (let [void (sql/prepare pg "")]
      (is (= [] (into [] (void)))))))

(deftest parameter
  (with-open [pg ($)
              set-time-zone (sql/prepare pg "SET TIME ZONE 'Pacific/Midway'")]
    (is (= [["TimeZone" "Pacific/Midway"]] (into [] (set-time-zone))))
    (is (= [{:n 1}] (eq pg ["SELECT 1 AS n"])))))

(deftest copy-data
  (with-open [pg ($)
              copy-data (sql/prepare pg "COPY (SELECT 1) TO STDOUT")]
    (is (= ["1\n"] (into [] (copy-data))))
    (is (= [{:n 1}] (eq pg ["SELECT 1 AS n"])))))

(deftest no-data
  (with-open [pg ($)
              no-data (sql/prepare pg "SELECT FROM pg_type WHERE FALSE")]
    (is (= [] (into [] (no-data))))))

(def ^:private q-t-by-ids
  "SELECT * FROM t WHERE id = ANY($1) ORDER BY a ASC")

(deftest table
  (with-open [pg ($)]
    (eq pg
      ["CREATE TABLE t (id int PRIMARY KEY, a int)"]
      ["INSERT INTO t (id, a) VALUES (1, 10), (2, 20), (3, 30)"])

    (with-open [t-by-ids (sql/prepare pg q-t-by-ids)]
      (is (= [{:id 1 :a 10}
              {:id 2 :a 20}
              {:id 3 :a 30}]
            (into [] (t-by-ids (int-array [1 2 3]))))))))

(deftest interlace
  (with-open [pg ($)
              sum (sql/prepare pg "SELECT $1 + $2 AS n" {:oids [(int 20) (int 20)]})
              product (sql/prepare pg "SELECT $1 * $2 AS n" {:oids [(int 20) (int 20)]})]
    ;; The order in which you execute the prepared statements does not matter.
    ;;
    ;; If we (incorrectly) did Bind -> Execute -> Sync outside of reduce, it would.
    (let [s (sum 1 2)
          p (product 3 4)]
      (is (= [{:n 3}] (into [] s)))
      (is (= [{:n 12}] (into [] p))))

    (let [p (product 3 4)
          s (sum 1 2)]
      (is (= [{:n 3}] (into [] s)))
      (is (= [{:n 12}] (into [] p))))))

(deftest alter-table
  (with-open [pg ($)]
    (eq pg
      ["CREATE TABLE t (id int PRIMARY KEY, a int)"]
      ["INSERT INTO t (id, a) VALUES (1, 10)"])

    (with-open [t-by-ids (sql/prepare pg q-t-by-ids)]
      (is (= [{:id 1 :a 10}] (into [] (t-by-ids (int-array [1])))))

      (eq pg
        ["ALTER TABLE t ADD COLUMN b int"]
        ["UPDATE t SET b = 100 WHERE id = 1"]
        ["INSERT INTO t (id, a, b) VALUES (2, 20, 200)"])

      ;; Statement that works after ALTER
      (is (= [{:id 1 :a 10 :b 100}
              {:id 2 :a 20 :b 200}]
            (into [] (t-by-ids (int-array [1 2])))
            (eq pg [q-t-by-ids (int-array [1 2])])))

      (eq pg ["ALTER TABLE t DROP COLUMN a"])

      (is (thrown-match? ExceptionInfo {:cause :undefined-column
                                        :error-code "42703"
                                        :kind ::error/server-error
                                        :severity "ERROR"}
            (into [] (t-by-ids (int-array [1 2])))))

      ;; No protocol desynchronization
      (is (= [{:b 100 :id 1} {:b 200 :id 2}] (eq pg ["SELECT * FROM t"]))))))
