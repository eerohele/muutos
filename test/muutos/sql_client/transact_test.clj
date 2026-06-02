(ns muutos.sql-client.transact-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [cognitect.anomalies :as-alias anomalies]
            [matcher-combinators.test]
            [muutos.sql-client :refer [connect eq sq transact] :as sql]
            [muutos.error :as-alias error]
            [muutos.type])
  (:import (clojure.lang ExceptionInfo)
           (java.lang AutoCloseable)))

(set! *warn-on-reflection* true)

(defn $ ^AutoCloseable [& {:as opts}]
  (connect
    (merge {:database "test" :port 5432} opts)))

(use-fixtures :each
  (fn [f]
    (with-open [client (connect :port 5432)]
      (eq client ["DROP DATABASE IF EXISTS test WITH (FORCE)"])
      (eq client ["CREATE DATABASE test"]))

    (f)))

(defmacro regardless [& body]
  `(try (do ~@body) (catch Throwable ex#)))

(def q-create-branches
  "CREATE TABLE branches (name TEXT PRIMARY KEY, balance DECIMAL(10, 2))")

(def q-create-accounts
  "CREATE TABLE accounts (
    name TEXT PRIMARY KEY,
    branch_name TEXT,
    balance DECIMAL(10, 2),
    FOREIGN KEY (branch_name) REFERENCES branches(name)
  )")

(deftest pg-example
  (with-open [pg-1 ($)
              pg-2 ($)]

    (sq pg-1 q-create-branches)
    (sq pg-1 q-create-accounts)

    (eq pg-1 ["INSERT INTO branches VALUES ($1, $2), ($3, $4)" "HQ" 1000.00M "Downtown" 500.00M])
    (eq pg-1 ["INSERT INTO accounts VALUES ($1, $2, $3), ($4, $5, $6)" "Alice" "HQ" 200.00M "Bob" "Downtown" 300.00M])

    ;; https://www.postgresql.org/docs/current/tutorial-transactions.html
    (transact pg-1
      ;; Deduct $100 from Alice's account.
      (eq pg-1 ["UPDATE accounts SET balance = balance - $2 WHERE name = $1" "Alice" 100.00M])

      ;; Because the transaction hasn't been committed yet, when using another
      ;; client to check, both Alice and Bob's account show the pre-
      ;; transaction status.
      (is (= [{"name" "Alice" "balance" 200.00M}
              {"name" "Bob"  "balance" 300.00M}]
            (eq pg-2 ["SELECT name, balance FROM accounts"])))

      ;; Deduct $100 from the account of the branch Alice belongs to (HQ).
      (eq pg-1 ["UPDATE branches SET balance = balance - $2 WHERE name = (SELECT branch_name FROM accounts WHERE name = $1)" "Alice" 100.00M])

      ;; Because the transaction hasn't been committed yet, when using another
      ;; client to check, both branches balances have the pre-transaction
      ;; status.
      (is (= [{"name" "HQ" "balance" 1000.00M}
              {"name" "Downtown" "balance" 500.00M}]
            (eq pg-2 ["SELECT * FROM branches"])))

      ;; Add $100 to Bob's account.
      (eq pg-1 ["UPDATE accounts SET balance = balance + $2 WHERE name = $1" "Bob" 100.00M])
      ;; Add $100 to the account of the branch Bob belongs to (Downtown).
      (eq pg-1 ["UPDATE branches SET balance = balance + $2 WHERE name = (SELECT branch_name FROM accounts WHERE name = $1)" "Bob" 100.00M]))

    (is (= [{"balance" 900.00M "name" "HQ"}
            {"balance" 600.00M "name" "Downtown"}]
          (eq pg-1 ["SELECT name, balance FROM branches"])))

    (is (= [{"balance" 100.00M
             "branch_name" "HQ"
             "name" "Alice"}
            {"balance" 400.00M
             "branch_name" "Downtown"
             "name" "Bob"}]
          (eq pg-1 ["SELECT name, branch_name, balance FROM accounts"])))))

(deftest no-commit-on-exception
  (with-open [pg ($)]
    (regardless
      (transact pg
        (sq pg "CREATE TABLE t (a int)")
        (sq pg "INSERT INTO t VALUES (1)")
        (throw (Exception.))))

    (is (thrown-match? ExceptionInfo {:cause :undefined-table
                                      :error-code "42P01"
                                      :kind ::error/server-error
                                      :severity "ERROR"}
          (sq pg "SELECT * FROM t")))))
