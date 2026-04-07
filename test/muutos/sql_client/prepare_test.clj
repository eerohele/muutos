(ns muutos.sql-client.eq-test
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [cognitect.anomalies :as-alias anomalies]
            [matcher-combinators.matchers :as matchers]
            [matcher-combinators.test]
            [muutos.sql-client :refer [connect eq prepare execute] :as sut]
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

(defn connect-test ^AutoCloseable [& {:as opts}]
  (connect (merge {:database "test" :host (host @server) :port (port @server)} opts)))

(deftest infer
  (with-open [pg (connect-test :key-fn (fn [_ attr-name] (keyword attr-name)))
              ;; Without :oids, Muutos passes 0, telling PostgreSQL to infer parameter types.
              oid-by-category (prepare pg "SELECT oid FROM pg_type WHERE typcategory = ANY($1)")]
    (is (set/subset? #{{:oid 16}
                       {:oid 18}
                       {:oid 194}
                       {:oid 3361}
                       {:oid 3402}
                       {:oid 5017}
                       {:oid 4600}
                       {:oid 4601}}
          (into #{} (oid-by-category (char-array [\B \Z])))))))

(deftest xform
  (with-open [pg (connect-test :key-fn (fn [_ attr-name] (keyword attr-name)))
              oid-by-category (prepare pg "SELECT oid FROM pg_type WHERE typcategory = ANY($1)" {:name 'oid-by-category})]
    (is (set/subset? #{{:oid 16}
                       {:oid 18}
                       {:oid 194}}
          (into #{} (filter (fn [{:keys [oid]}] (< oid 1000)))
            (execute pg 'oid-by-category [(char-array [\B \Z])]))))))

(deftest close
  (with-open [pg (connect-test)]
    (with-open [sum (prepare pg "SELECT $1 + $2 AS n" {:name 'sum :oids [(int 20) (int 20)]})]
      (is (= [{"n" 3}] (into [] (sum 1 2))))
      (is (= [{"n" 7}] (into [] (sut/execute pg 'sum [3 4])))))

    ;; Prepared statement is closed, attempting to call it (by name) throws.
    (is (thrown-match? ExceptionInfo {:cause :ERRCODE-UNDEFINED-PSTATEMENT
                                      :error-code "26000"
                                      :kind ::error/server-error
                                      :severity "ERROR"}
          (into [] (sut/execute pg 'sum [1 2]))))))

(deftest close-before-execute
  (with-open [pg (connect-test)
              sum (prepare pg "SELECT $1 + $2 AS n" {:name 'sum :oids [(int 20) (int 20)]})]
    ;; Closing a prepared statement that hasn't been executed doesn't throw.
    (is (instance? clojure.lang.IReduceInit (sum 1 2)))))
