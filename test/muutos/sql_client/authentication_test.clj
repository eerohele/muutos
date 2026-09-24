(ns muutos.sql-client.authentication-test
  (:require [clojure.test :refer [deftest is]]
            [muutos.sql-client :refer [connect sq]]))

(deftest ^:integration scram-sha-256-plus
  (with-open [pg (connect)]
    (is (= [{"a" 1}] (sq pg "SELECT 1 AS a")))
    (is (= [{"ssl" true
             "cipher" "TLS_AES_256_GCM_SHA384"
             "version" "TLSv1.3"
             "bits" 256}]
          (sq pg "SELECT ssl, cipher, version, bits FROM pg_stat_ssl WHERE pid = pg_backend_pid()")))))

(deftest ^:integration trust
  (with-open [pg (connect :port 5437)]
    (is (= [{"a" 1}] (sq pg "SELECT 1 AS a")))))
