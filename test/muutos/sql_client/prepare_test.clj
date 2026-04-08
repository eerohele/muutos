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

(defn connect-test ^AutoCloseable [& {:as opts}]
  (connect (merge {:database "test" :key-fn key-fn :host (host @server) :port (port @server)} opts)))

(deftest infer
  (with-open [pg (connect-test)
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
  (with-open [pg (connect-test)
              sum (sql/prepare pg "SELECT $1 + $2 AS n" {:oids [(int 20) (int 20)]})]
    (is (= [{:n 3}] (into [] (sum 1 2))))))

(deftest xform
  (with-open [pg (connect-test)
              _ (sql/prepare pg "SELECT oid FROM pg_type WHERE typcategory = ANY($1)" {:name 'oid-by-category})]
    (is (set/subset? #{32 36 388}
          (into #{}
            (comp
              (filter (fn [{:keys [oid]}] (< oid 1000)))
              (map (fn [{:keys [oid]}] (* 2 oid))))
            (sql/execute pg 'oid-by-category (char-array [\B \Z])))))))

(deftest xform-reducible
  (with-open [pg (connect-test)
              oid-by-category (sql/prepare pg "SELECT oid FROM pg_type WHERE typcategory = ANY($1)" {:name 'oid-by-category})]
    (is (= {:oid 3361}
          (transduce
            (halt-when (fn [{:keys [oid]}] (> oid 1000)))
            conj
            {}
            (oid-by-category (char-array [\B \Z])))))))

(deftest close
  (with-open [pg (connect-test)]
    (with-open [sum (sql/prepare pg "SELECT $1 + $2 AS n" {:name 'sum :oids [(int 20) (int 20)]})]
      (is (= [{:n 3}] (into [] (sum 1 2))))
      (is (= [{:n 7}] (into [] (sql/execute pg 'sum 3 4)))))

    ;; Prepared statement is closed, attempting to call it (by name) throws.
    (is (thrown-match? ExceptionInfo {:cause :ERRCODE-UNDEFINED-PSTATEMENT
                                      :error-code "26000"
                                      :kind ::error/server-error
                                      :severity "ERROR"}
          (into [] (sql/execute pg 'sum 1 2))))))

(deftest close-before-execute
  (with-open [pg (connect-test)
              sum (sql/prepare pg "SELECT $1 + $2 AS n" {:oids [(int 20) (int 20)]})]
    ;; Closing a prepared statement that hasn't been executed doesn't throw.
    (is (instance? IReduceInit (sum 1 2)))))
