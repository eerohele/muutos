(ns muutos.sql-client.authentication-test
  (:require [clojure.test :refer [deftest is]]
            [muutos.test.container :as container]
            [muutos.test.server :refer [host port]]
            [muutos.sql-client :refer [connect sq]]))

;; TODO: scram-sha-256-plus

(deftest ^:integration scram-sha-256
  (with-open [server (container/start (container/create container/default-opts))
              pg (connect :host (host server) :port (port server))]
    (is (= [{"a" 1}] (sq pg "SELECT 1 AS a")))))

(deftest ^:integration trust
  (let [opts (assoc-in container/default-opts [:env-vars "POSTGRES_HOST_AUTH_METHOD"] "trust")]
    (with-open [server (container/start (container/create opts))
                pg (connect :host (host server) :port (port server))]
      (is (= [{"a" 1}] (sq pg "SELECT 1 AS a"))))))
