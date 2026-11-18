(ns muutos.pool-test
  (:require [clojure.test :refer [deftest is]]
            [cognitect.anomalies :as-alias anomalies]
            [matcher-combinators.test]
            [muutos.impl.thread :as thread]
            [muutos.pool :as pool]
            [muutos.sql-client :as sql]
            [muutos.test.tcp-crusher :as tcp-crusher])
  (:import (clojure.lang ExceptionInfo)
           (java.time Duration)
           (java.util.concurrent Executors)
           (org.netcrusher.tcp TcpCrusher)))

(set! *warn-on-reflection* true)

(defmacro ^:private unwrap-ex
  "Given a body that throws a java.lang.Exception with a cause, throw the cause
  exception."
  [& body]
  `(try
     (do ~@body)
     (catch Exception ex#
       (throw (ex-cause ex#)))))

(deftest lease-pool-closed
  (let [pg (pool/fill :capacity 1)]
    (.close pg)

    (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/unavailable}
          (sql/eq pg ["SELECT $1 AS n" 42])))))

(deftest lease-reopen-connect-timeout
  (with-open [reactor (tcp-crusher/make-reactor)
              crusher (tcp-crusher/make-crusher reactor :bind-port 10080 :connect-port 5433)
              pg (pool/fill
                   :port 10080
                   :capacity 1
                   :connect-timeout (Duration/ofSeconds 1)
                   :validate-after-idle (Duration/ofNanos 0))]
    (is (= [{"n" 1}] (sql/eq pg ["SELECT $1 AS n" 1])))
    (TcpCrusher/.reopen crusher)
    (is (= [{"n" 2}] (sql/eq pg ["SELECT $1 AS n" 2])))))

(deftest lease-retry
  (with-open [reactor (tcp-crusher/make-reactor)
              crusher (tcp-crusher/make-crusher reactor :bind-port 10080 :connect-port 5433)
              pg (pool/fill
                   :port 10080
                   :capacity 1
                   :connect-timeout (Duration/ofMillis 500)
                   :validate-after-idle (Duration/ofNanos 0))]
    (is (= [{"n" 42}] (sql/eq pg ["SELECT $1 AS n" 42])))
    (TcpCrusher/.close crusher)
    (let [p (promise)]
      (future (deliver p (sql/eq pg ["SELECT $1 AS n" 42])))
      (Thread/sleep (Duration/ofSeconds 1))
      (TcpCrusher/.open crusher)
      (is (= [{"n" 42}] (deref p 5000 ::timeout))))))

(deftest lease-available
  (with-open [pg (pool/fill :capacity 1)]
    (is (= [{"n" 42}] (sql/eq pg ["SELECT $1 AS n" 42])))))

(deftest lease-wait
  (with-open [executor (Executors/newFixedThreadPool 2)
              pg (pool/fill :capacity 1 :lease-timeout (Duration/ofSeconds 2))]
    (thread/execute executor (sql/eq pg ["SELECT pg_sleep($1)" 1]))
    (is (= [{"n" 42}] @(thread/submit executor (sql/eq pg ["SELECT $1 AS n" 42]))))))

(deftest lease-throw-on-timeout
  (with-open [executor (Executors/newFixedThreadPool 2)
              pg (pool/fill :capacity 1 :lease-timeout (Duration/ofSeconds 1))]
    (thread/execute executor (sql/eq pg ["SELECT pg_sleep($1)" 2]))

    (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/unavailable}
          (unwrap-ex @(thread/submit executor (sql/eq pg ["SELECT $1 AS n" 42])))))))

;; Eh, maybe not?
#_(deftest return-busy
  (with-open [pg (pool/fill :capacity 1)]
    (sql/eq pg ["BEGIN"] ["CREATE TEMPORARY TABLE t (n int)"] ["INSERT INTO t (n) VALUES (1)"])

    (is (thrown-match? ExceptionInfo
          {:cause :undefined-table
           :error-code "42P01"
           :severity "ERROR"}
          (sql/eq pg ["SELECT * FROM t"])))))
