(ns muutos.sql-client.client-test
  (:require [clojure.test :refer [deftest is]]
            [muutos.test.container :as container]
            [muutos.test.server :refer [host port]]
            [muutos.sql-client :refer [connect sq]]))

(defn ^:private count-clients [pg]
  (->
    (sq pg "SELECT COUNT(1) AS clients
            FROM pg_stat_activity
            WHERE application_name = 'Muutos SQL Client'")
    (first)
    (get "clients")))

(deftest ^:integration close-clients
  (with-open [server (container/start (container/create container/default-opts))
              pg (connect :host (host server) :port (port server))]
    (is (= 2 (count-clients pg)))
    (with-open [_ (connect :host (host server) :port (port server))])
    (Thread/sleep 1000)
    (is (= 2 (count-clients pg)))))
