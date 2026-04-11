(ns muutos.sql-client.client-test
  (:require [clojure.test :refer [deftest is]]
            [muutos.test.concurrency :refer [concurrently]]
            [muutos.sql-client :refer [connect sq]])
  (:import (java.util.concurrent ArrayBlockingQueue)))

(set! *warn-on-reflection* true)

(defn ^:private count-clients [pg]
  (->
    (sq pg "SELECT COUNT(1) AS clients
            FROM pg_stat_activity
            WHERE application_name = 'Muutos SQL Client'")
    (first)
    (get "clients")))

(deftest ^:integration close-clients
  (with-open [pg (connect)]
    (is (= 2 (count-clients pg)))
    (with-open [_ (connect)])
    (^[long] Thread/sleep 1000)
    (is (= 2 (count-clients pg)))))

(deftest ^:integration concurrent-connect
  (let [n 50
        q (ArrayBlockingQueue. n)]
    (concurrently {:threads n}
      (with-open [pg (connect :port 5432)]
        (ArrayBlockingQueue/.put q (sq pg "SELECT 1 AS n"))))

    (dotimes [_ n]
      (is (= [{"n" 1}] (ArrayBlockingQueue/.take q))))))
