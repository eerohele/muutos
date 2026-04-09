(ns muutos.sql-client.prepare-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is use-fixtures]]
            [cognitect.anomalies :as-alias anomalies]
            [matcher-combinators.test]
            [muutos.sql-client :refer [connect eq] :as sql]
            [muutos.error :as-alias error]
            [muutos.test.server :as server :refer [host port]]
            [muutos.type])
  (:import (clojure.lang ExceptionInfo IReduceInit)
           (java.lang AutoCloseable)))

(set! *warn-on-reflection* true)

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

(defn ^:private key-fn [_ attr-name] (keyword attr-name))

(defn $ ^AutoCloseable [& {:as opts}]
  (connect (merge {:database "test" :key-fn key-fn :host (host @server) :port (port @server)} opts)))

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
              _ (sql/prepare pg "SELECT oid FROM pg_type WHERE typcategory = ANY($1)" {:name 'oid-by-category})]
    (is (set/subset? #{32 36 388}
          (into #{}
            (comp
              (filter (fn [{:keys [oid]}] (< oid 1000)))
              (map (fn [{:keys [oid]}] (* 2 oid))))
            (sql/execute pg 'oid-by-category (char-array [\B \Z])))))))

(deftest xform-reducible
  (with-open [pg ($)
              oid-by-category (sql/prepare pg "SELECT oid FROM pg_type WHERE typcategory = ANY($1)" {:name 'oid-by-category})]
    (is (= {:oid 3361}
          (transduce
            (halt-when (fn [{:keys [oid]}] (> oid 1000)))
            conj
            {}
            (oid-by-category (char-array [\B \Z])))))))

(deftest close-by-name
  (with-open [pg ($)]
    (with-open [sum (sql/prepare pg "SELECT $1 + $2 AS n" {:name 'sum :oids [(int 20) (int 20)]})]
      (is (= [{:n 3}] (into [] (sum 1 2))))
      (is (= [{:n 7}] (into [] (sql/execute pg 'sum 3 4)))))

    ;; Prepared statement is closed, attempting to call it (by name) throws.
    (is (thrown-match? ExceptionInfo {:cause :ERRCODE-UNDEFINED-PSTATEMENT
                                      :error-code "26000"
                                      :kind ::error/server-error
                                      :severity "ERROR"}
          (prn (into [] (sql/execute pg 'sum 1 2)))))))

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
    ;; Closing a prepared statement that hasn't been executed doesn't throw.
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

    #_(with-open [sum (sql/prepare pg "SELECT $1 + $2 AS n" {:oids [(int 20) (int 20)]})]
      (is (= #{{:n 3}} (into #{} (sum 1 2)))))))

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
              set-time-zone (sql/prepare pg "SET TIME ZONE 'Europe/Helsinki'")]
    (is (= [["TimeZone" "Europe/Helsinki"]] (into [] (set-time-zone))))
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
