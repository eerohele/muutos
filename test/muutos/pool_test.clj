(ns muutos.pool-test
  (:require [clojure.test :refer [deftest is]]
            [cognitect.anomalies :as-alias anomalies]
            [matcher-combinators.test]
            [muutos.pool :as pool]
            [muutos.sql-client :as sql])
  (:import (clojure.lang ExceptionInfo)
           (java.time Duration)))

(set! *warn-on-reflection* true)

(deftest lease-available
  (with-open [pg (pool/fill :capacity 1)]
    (is (= [{"n" 42}] (sql/eq pg ["SELECT $1 AS n" 42])))))

(deftest lease-wait
  (with-open [pg (pool/fill :capacity 1 :lease-timeout (Duration/ofSeconds 2))]
    (future (sql/eq pg ["SELECT pg_sleep($1)" 1]))

    (is (= [{"n" 42}] (sql/eq pg ["SELECT $1 AS n" 42])))))

(deftest lease-throw-on-timeout
  (with-open [pg (pool/fill :capacity 1 :lease-timeout (Duration/ofSeconds 1))]
    (future (sql/eq pg ["SELECT pg_sleep($1)" 2]))

    (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/unavailable}
          (sql/eq pg ["SELECT $1 AS n" 42])))))
