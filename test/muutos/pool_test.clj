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

(deftest lease-available
  (with-open [pg (pool/fill :capacity 1)]
    (is (= [{"n" 42}] (sql/eq pg ["SELECT $1 AS n" 42])))))

(deftest lease-wait
  (with-open [executor (Executors/newFixedThreadPool 2)
              ;; Make a pool with a single available connection.
              pg (pool/fill :capacity 1 :lease-timeout (Duration/ofSeconds 2))]
    ;; Tie up the sole connection.
    (thread/execute executor (sql/eq pg ["SELECT pg_sleep($1)" 1]))
    ;; When the tied up connection finishes its work, it returns the connection
    ;; into the pool, making it available to the next caller.
    (is (= [{"n" 42}] @(thread/submit executor (sql/eq pg ["SELECT $1 AS n" 42]))))))

(deftest lease-throw-on-timeout
  (with-open [executor (Executors/newFixedThreadPool 2)
              pg (pool/fill :capacity 1 :lease-timeout (Duration/ofSeconds 1))]
    ;; Tie up the sole connection.
    (thread/execute executor (sql/eq pg ["SELECT pg_sleep($1)" 2]))

    ;; The pool cannot supply a connection in time; throw.
    (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/unavailable}
          (unwrap-ex @(thread/submit executor (sql/eq pg ["SELECT $1 AS n" 42])))))))

(deftest lease-pool-closed
  (let [pg (pool/fill :capacity 1)]
    (.close pg)

    ;; Attempting to lease a connection from a closed pool throws.
    (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/incorrect}
          (sql/eq pg ["SELECT $1 AS n" 42])))))

(deftest lease-reopen
  (with-open [reactor (tcp-crusher/make-reactor)
              crusher (tcp-crusher/make-crusher reactor :bind-port 10080 :connect-port 5433)
              pg (pool/fill
                   :port 10080
                   :capacity 1
                   :connect-timeout (Duration/ofSeconds 5)
                   :validate-after-idle (Duration/ofNanos 0))]

    (is (= [{"n" 1}] (sql/eq pg ["SELECT $1 AS n" 1])))
    ;; Emulate a network blip.
    (TcpCrusher/.reopen crusher)
    (is (= [{"m" 2}] (sql/eq pg ["SELECT $1 AS m" 2])))))

;; FIXME: This test is pretty slow.
#_(deftest lease-reopen-timeout
    (with-open [reactor (tcp-crusher/make-reactor)
                crusher (tcp-crusher/make-crusher reactor :bind-port 10080 :connect-port 5433)
                pg (pool/fill
                     :port 10080
                     :capacity 1
                     :connect-timeout (Duration/ofMillis 1)
                     :validate-after-idle (Duration/ofNanos 0))]

      (is (= [{"n" 1}] (sql/eq pg ["SELECT $1 AS n" 1])))

      (TcpCrusher/.close crusher)

      (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/unavailable}
            (sql/eq pg ["SELECT $1 AS m" 2])))))

(deftest lease-retry
  (with-open [executor (Executors/newFixedThreadPool 2)
              reactor (tcp-crusher/make-reactor)
              crusher (tcp-crusher/make-crusher reactor :bind-port 10080 :connect-port 5433)
              pg (pool/fill
                   :port 10080
                   :capacity 1
                   :connect-timeout (Duration/ofSeconds 5)
                   :validate-after-idle (Duration/ofNanos 0))]

    (is (= [{"n" 1}] (sql/eq pg ["SELECT $1 AS n" 1])))

    ;; Close TCP connection.
    (TcpCrusher/.close crusher)

    (let [p (promise)]
      ;; While the connection is closed, attempt a query.
      (thread/execute executor (deliver p (sql/eq pg ["SELECT $1 AS m" 2])))

      ;; Reopen connection.
      (TcpCrusher/.open crusher)

      ;; The attempted query eventually succeeds.
      (is (= [{"m" 2}] @(thread/submit executor (deref p 1000 ::timeout)))))))

(deftest return-busy
  (with-open [pg (pool/fill :capacity 1)]
    (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/incorrect}
          (sql/eq pg ["BEGIN"] ["CREATE TEMPORARY TABLE t (n int)"])))

    (is (thrown-match? ExceptionInfo
          {:cause :undefined-table
           :error-code "42P01"
           :severity "ERROR"}
          (sql/eq pg ["SELECT * FROM t"])))

    (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/incorrect}
          (sql/eq pg ["BEGIN"])))

    (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/incorrect}
          (sql/sq pg "BEGIN; CREATE TEMPORARY TABLE t (n int); INSERT INTO t (n) VALUES (1)")))

    (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/incorrect}
          (sql/sq pg "BEGIN")))

    (is (= [{"n" 1}] (sql/eq pg ["SELECT $1 AS n" 1])))))
