(ns muutos.sql-client.authentication-test
  (:require [clojure.test :refer [deftest is]]
            [muutos.sql-client :refer [connect sq]]))

;; TODO: scram-sha-256-plus

(deftest ^:integration scram-sha-256
  (with-open [pg (connect)]
    (is (= [{"a" 1}] (sq pg "SELECT 1 AS a")))))

(deftest ^:integration trust
  (with-open [pg (connect :port 5437)]
    (is (= [{"a" 1}] (sq pg "SELECT 1 AS a")))))
