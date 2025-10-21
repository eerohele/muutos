(ns muutos.sql-client.tag-test
  (:require [muutos.impl.decode :refer [parse-command-tag]]
            [clojure.test :refer [deftest is]]))

;; https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE
(deftest ^:unit command-tag
  (is (= (parse-command-tag "INSERT 0 1") {:command "INSERT" :rows 1}))
  (is (= (parse-command-tag "DELETE 2") {:command "DELETE" :rows 2}))
  (is (= (parse-command-tag "UPDATE 3") {:command "UPDATE" :rows 3}))
  (is (= (parse-command-tag "MERGE 4") {:command "MERGE" :rows 4}))
  (is (= (parse-command-tag "SELECT 5") {:command "SELECT" :rows 5}))
  (is (= (parse-command-tag "MOVE 6") {:command "MOVE" :rows 6}))
  (is (= (parse-command-tag "FETCH 7") {:command "FETCH" :rows 7}))
  (is (= (parse-command-tag "COPY 8") {:command "COPY" :rows 8}))
  (is (= (parse-command-tag "CREATE TABLE") {:command "CREATE TABLE"})))
