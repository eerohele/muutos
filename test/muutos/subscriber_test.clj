(ns muutos.subscriber-test
  (:refer-clojure :exclude [type])
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [cognitect.anomalies :as-alias anomalies]
            [matcher-combinators.core :refer [Matcher]]
            [matcher-combinators.matchers :as matchers]
            [matcher-combinators.model :as model]
            [matcher-combinators.result :as result]
            [matcher-combinators.test]
            [muutos.error :as-alias error]
            [muutos.impl.client :as client]
            [muutos.impl.lockable :refer [Lockable]]
            [muutos.impl.thread :as thread]
            [muutos.sql-client :refer [eq sq emit-message] :as sql-client]
            [muutos.subscriber :as subscriber]
            [muutos.test.concurrency :refer [concurrently]]
            [muutos.test.container :as container]
            [muutos.test.server :as server :refer [host port]]
            [muutos.type])
  (:import (clojure.lang ExceptionInfo)
           (java.lang AutoCloseable)
           (java.net SocketException)
           (java.nio.charset StandardCharsets)
           (java.security SecureRandom)
           (java.time Duration Instant)
           (java.util Arrays)
           (java.util.concurrent ArrayBlockingQueue BlockingQueue ExecutorService RejectedExecutionException SynchronousQueue TimeUnit)
           (java.util.concurrent.locks ReentrantLock)))

(extend-protocol Matcher
  byte/1
  (-match [this actual]
    (if (and (bytes? this) (bytes? actual) (Arrays/equals this actual))
      {::result/type   :match
       ::result/value  actual
       ::result/weight 0}
      {::result/type   :mismatch
       ::result/value  (if (nil? actual)
                         (model/->Missing this)
                         (model/->Mismatch this actual))
       ::result/weight 1})))

(def container-opts
  (assoc container/default-opts :command ["postgres"
                                          "-c" "wal_level=logical"
                                          "-c" "wal_sender_timeout=10s"
                                          "-c" "max_wal_senders=4"
                                          "-c" "max_replication_slots=1"]))

(defonce server
  (delay (server/start container-opts)))

(comment (.close @server) ,,,)

(use-fixtures :each
  (fn [f]
    (with-open [client (sql-client/connect :host (host @server) :port (port @server))]
      (sq client "DROP DATABASE test WITH (FORCE)")
      (sq client "CREATE DATABASE test"))

    (f)))

(defn test-client ^AutoCloseable [& {:as opts}]
  (try
    (sql-client/connect
      (merge {:database "test" :host (host @server) :port (port @server)} opts))
    (catch ExceptionInfo ex
      (if (= :cannot-connect-now (:cause (ex-data ex)))
        (do (Thread/sleep 1000)
          (test-client opts))
        (throw ex)))))

(defn test-options [& {:as options}]
  (conj {:database "test"
         :host (host @server)
         :port (port @server)} options))

(defn connect [slot-name & {:as options}]
  (subscriber/connect slot-name (test-options options)))

(deftest ^:integration unknown-slot
  (is (thrown-with-msg? ExceptionInfo
        #"^replication slot \"none\" does not exist$"
        (connect "none"))))

;; If you subscribe to a publication that does not exist, Postgres emits an
;; error not when starting replication, but when it sends you the first logical
;; replication message regarding a change to one of the tables in your
;; database.
;;
;; In other words, calling `subscribe` with a non-existent publication name
;; cannot throw an error, because Postgres delivers the error to the client
;; asynchronously.
;;
;; I guess one option would be to add unit tests that test that a certain data
;; gets logged upon specific events.

(defn replication-slot ^AutoCloseable [slot-name & {:as opts}]
  (let [client (test-client (assoc opts :replication :database))]
    (sq client (format "CREATE_REPLICATION_SLOT %s LOGICAL pgoutput" slot-name))

    (reify AutoCloseable
      (close [_]
        (sq client (format "DROP_REPLICATION_SLOT %s WAIT" slot-name))
        (AutoCloseable/.close client)))))

(defn instant? [x] (instance? Instant x))
(defn int64? [x] (instance? Long x))
(defn int32? [x] (instance? Integer x))

(defn poll
  ([q] (poll q 5 :seconds))
  ([q amount] (poll q amount :seconds))
  ([q amount unit]
   (BlockingQueue/.poll q amount
     (case unit
       :millis TimeUnit/MILLISECONDS
       :seconds TimeUnit/SECONDS
       :minutes TimeUnit/MINUTES))))

(defn q-handler [q]
  (fn handle
    ([msg] (BlockingQueue/.put q msg))
    ;; This arity is only called on :commit messages.
    ([msg ack]
     (handle msg)
     (ack))))

(defn utf8-str [^bytes bs]
  (some-> bs (String. StandardCharsets/UTF_8)))

(deftest ^:integration no-slot
  (is (thrown-match? ExceptionInfo {:cause :undefined-object
                                    :file "slot.c"
                                    :error-code "42704"
                                    :routine "ReplicationSlotAcquire"
                                    :kind ::error/server-error
                                    :severity "ERROR"}
        (connect "s"))))

(deftest ^:integration active-slot
  (with-open [_ (replication-slot "s")
              _sub (connect "s")]
    (is (thrown-with-msg? ExceptionInfo
          #"^replication slot \"s\" is active for PID \d+$"
          (connect "s")))))

(def begin
  {:type :begin
   :commit-timestamp instant?
   :lsn int64?
   :xid int32?})

(def commit
  {:type :commit
   :commit-lsn int64?
   :tx-end-lsn int64?
   :commit-timestamp instant?})

(defn message [prefix content]
  {:type :message
   :flags :transactional
   :lsn int64?
   :prefix prefix
   :content content})

(defn ^:private type [namespace name]
  {:type :type
   :oid int32?
   :namespace namespace
   :name name})

(deftest ^:integration bad-password
  (with-open [_ (replication-slot "s")]
    (is (thrown-match? ExceptionInfo {:cause :invalid-password
                                      :error-code "28P01"
                                      :routine "auth_failed"
                                      :severity-localized "FATAL"
                                      :severity "FATAL"}
          (connect "s" :password "bad")))))

(deftest ^:integration bad-username
  (is (thrown-match? ExceptionInfo {:cause :undefined-object
                                    :error-code "42704"
                                    :routine "ReplicationSlotAcquire"
                                    :severity-localized "ERROR"
                                    :severity "ERROR"}
        (connect "s" :username "bad"))))

(deftest ^:integration minimal-example
  (let [q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]
      ;; Emit message prior to subscribing.
      (emit-message client "prefix" "message-1")

      (with-open [_sub (connect "s" :handler (q-handler q))]
        ;; Emit message after subscribing.
        (emit-message client "prefix" "message-2")

        (testing "emit message before subscribe"
          (is (match? begin (poll q)))

          (is (match? (message "prefix" "message-1")
                (update (poll q) :content utf8-str)))

          (is (match? commit (poll q))))

        (testing "emit message after subscribe"
          (is (match? begin (poll q)))

          (is (match? (message "prefix" "message-2")
                (update (poll q) :content utf8-str)))

          (is (match? commit (poll q))))))))

(deftest ^:integration handler-throws-msg
  (with-open [_slot (replication-slot "s")
              client (test-client)
              sub (connect "s"
                     :handler (fn handle
                                ([msg] (throw (ex-info "Boom!" msg)))
                                ([msg _ack] (handle msg))))]
    (emit-message client "prefix" "message-1")
    (is (thrown-match? ExceptionInfo {:type :begin} (deref sub)))

    (let [q (SynchronousQueue. true)]
      (with-open [_sub (connect "s"
                        :handler (q-handler q))]
        (is (match? begin (poll q)))

          (is (match? (message "prefix" "message-1")
                (update (poll q) :content utf8-str)))

        (is (match? commit (poll q)))))))

(deftest ^:integration handler-throws-msg-pre-ack
  (with-open [_slot (replication-slot "s")
              client (test-client)
              sub (connect "s"
                     :handler (fn handle
                                ([_msg])
                                ([msg ack] (throw (ex-info "Boom!" msg)) (ack))))]
    (emit-message client "prefix" "message-1")
    (is (thrown-match? ExceptionInfo {:type :commit} (deref sub)))

    (let [q (SynchronousQueue. true)]
      (with-open [_sub (connect "s"
                         :handler (q-handler q))]
        (is (match? begin (poll q)))

        (is (match? (message "prefix" "message-1")
              (update (poll q) :content utf8-str)))

        (is (match? commit (poll q)))))))

(deftest ^:integration handler-throws-msg-post-ack
  (with-open [_slot (replication-slot "s")
              client (test-client)
              sub (connect "s"
                     :handler (fn handle
                                ([_msg])
                                ([msg ack] (ack) (throw (ex-info "Boom!" msg)))))]
    (emit-message client "prefix" "message-1")
    (is (thrown-match? ExceptionInfo {:type :commit} (deref sub)))
    (emit-message client "prefix" "message-2")

    (let [q (SynchronousQueue. true)]
      (with-open [_sub (connect "s"
                         :handler (q-handler q))]
        (is (match? begin (poll q)))

        (is (match? (message "prefix" "message-2")
              (update (poll q) :content utf8-str)))

        (is (match? commit (poll q)))))))

(deftest ^:integration alter-table
  (let [q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]
      (sq client "CREATE TABLE t (n int NOT NULL)")
      (sq client "CREATE PUBLICATION p FOR TABLE t")

      (with-open [_sub (connect "s"
                         :publications #{"p"}
                         :handler (q-handler q))]
        (sq client "INSERT INTO t (n) VALUES (1)")

        (is (match? begin (poll q)))

        (is (match? {:type :relation
                     :oid int32?
                     :namespace "public"
                     :relation "t"
                     :replica-identity :primary-key
                     :attributes [{:flags #{} :name "n" :data-type-oid 23 :type-modifier -1}]}
              (poll q)))

        (is (match? {:type :insert
                     :new-row {"n" 1}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? commit (poll q)))

        ;; Add a column while replication is in progress.
        (sq client "ALTER TABLE t ADD COLUMN m int")
        (sq client "INSERT INTO t (n, m) VALUES (2, 3)")

        (is (match? begin (poll q)))

        (is (match? {:type :relation
                     :oid int32?
                     :namespace "public"
                     :relation "t"
                     :replica-identity :primary-key
                     :attributes [{:flags #{}
                                   :name "n"
                                   :data-type-oid 23
                                   :type-modifier -1}
                                  {:flags #{}
                                   :name "m"
                                   :data-type-oid 23
                                   :type-modifier -1}]}
              (poll q)))

        (is (match? {:type :insert
                     :new-row {"n" 2 "m" 3}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? commit (poll q)))))))

(deftest ^:integration resume
  (let [q (SynchronousQueue. true)]
    (with-open [client (test-client)
                _slot (replication-slot "s")]
      ;; Emit message logical replication message.
      (with-open [_sub (connect "s" :handler (q-handler q))]
        (emit-message client "prefix" "message-1")

        (is (match? begin (poll q)))

        (is (match? (message "prefix" "message-1")
              (update (poll q) :content utf8-str)))

        (is (match? commit (poll q))))

      ;; Emit another message after closing the subscriber.
      (emit-message client "prefix" "message-2")

      (with-open [_sub (connect "s" :handler (q-handler q))]
        (is (match? begin (poll q)))

        ;; The subscriber receives message-2, not message-1.
        (is (match? (message "prefix" "message-2")
              (update (poll q) :content utf8-str)))

        (is (match? commit (poll q)))))))

(deftest ^:integration commit-no-ack
  (with-open [client (test-client)
              _ (replication-slot "s")]
    (with-open [_sub (connect "s" :handler (constantly nil))]
      (emit-message client "prefix" "message-1"))

    (let [q (SynchronousQueue. true)]
      (with-open [_sub (connect "s" :handler (q-handler q))]
        (is (match? begin (poll q)))

        (is (match? (message "prefix" "message-1")
              (update (poll q) :content utf8-str)))

        (is (match? commit (poll q)))))))

(deftest ^:integration commit-ack-on-quit
  (with-open [client (test-client)
              _ (replication-slot "s")]
    (let [q (SynchronousQueue. true)]
      (with-open [_sub (connect "s" :handler (q-handler q))]
        (emit-message client "prefix" "message-1")

        (is (match? begin (poll q)))

        (is (match? (message "prefix" "message-1")
              (update (poll q) :content utf8-str)))

        (is (match? commit (poll q)))))

    (let [q (SynchronousQueue. true)]
      (with-open [_sub (connect "s" :handler (q-handler q))]
        (emit-message client "prefix" "message-2")

        (is (match? begin (poll q)))

        (is (match? (message "prefix" "message-2")
              (update (poll q) :content utf8-str)))

        (is (match? commit (poll q)))))))

(deftest ^:integration backpressure
  (let [q (SynchronousQueue. true)
        work-queue (ArrayBlockingQueue. 1 true)
        executor (thread/executor
                   :core-pool-size 1
                   :max-pool-size 1
                   :rejection-policy (thread/flow-control-policy work-queue)
                   :work-queue work-queue)]
    (try
      (with-open [client (test-client)
                  _slot (replication-slot "s")
                  _sub (subscriber/connect "s"
                         {:executor executor
                          :executor-close-fn ExecutorService/.shutdownNow
                          :database "test"
                          :host (host @server)
                          :port (port @server)
                          :handler (q-handler q)})]

        (emit-message client "prefix" "message-1")

        ;; Work queue capacity is 1, so the executor exerts backpressure on the
        ;; producer (the thread reading PostgreSQL messages off the
        ;; socket) until message-1 has been taken off the queue.
        (emit-message client "prefix" "message-2")

        (is (match? begin (poll q)))
        (is (match? (message "prefix" "message-1") (update (poll q) :content utf8-str)))
        (is (match? commit (poll q)))

        (is (match? begin (poll q)))
        (is (match? (message "prefix" "message-2") (update (poll q) :content utf8-str)))
        (is (match? commit (poll q))))
      (finally
        (.shutdownNow executor)))))

(deftest ^:integration backpressure-timeout
  (let [q (SynchronousQueue. true)
        work-queue (ArrayBlockingQueue. 2 true)
        executor (thread/executor
                   :core-pool-size 1
                   :max-pool-size 1
                   :rejection-policy (thread/flow-control-policy work-queue :timeout (Duration/ofNanos 0))
                   :work-queue work-queue)]
    (try
      (with-open [client (test-client)
                  _slot (replication-slot "s")]
        (let [sub (subscriber/connect "s"
                    {:database "test"
                     :host (host @server)
                     :port (port @server)
                     :executor executor
                     :executor-close-fn ExecutorService/.shutdownNow
                     :handler (q-handler q)})]

          (emit-message client "prefix" "message-1")
          (emit-message client "prefix" "message-2")

          (is (match? begin (poll q)))
          (is (match? (message "prefix" "message-1") (update (poll q) :content utf8-str)))
          (is (match? commit (poll q)))

          (is (thrown? RejectedExecutionException (deref sub)))))
      (finally
        (.shutdownNow executor)))))

(deftest ^:integration fast-producer
  (let [n 1
        work-queue (ArrayBlockingQueue. 1 false)

        executor (thread/executor
                   :core-pool-size 1
                   :max-pool-size 1
                   :rejection-policy (thread/flow-control-policy work-queue)
                   :work-queue work-queue)]

    (with-open [_slot (replication-slot "s")
                client (test-client)]

      (let [q (SynchronousQueue. true)]
        (with-open [_sub (connect "s" :handler (q-handler q) :executor executor)]
          (concurrently {:threads n}
            (emit-message client "prefix" (str (random-uuid))))

          (let [message-uuids (into []
                                (comp
                                  (filter (comp #{:message} :type))
                                  (map (comp parse-uuid utf8-str :content))
                                  (map (fn [x] (prn x) x)))
                                ;; Every emit-message yields three logical replication messages
                                ;; (:begin, :message, and :commit).
                                (repeatedly (* 3 n) #(poll q)))]
            (is (every? uuid? message-uuids)))))

      (emit-message client "prefix" "Hello, world!")

      (let [q (SynchronousQueue. true)]
        (with-open [_sub (connect "s" :handler (q-handler q))]
          (is (match? begin (poll q)))

          (is (match? (message "prefix" "Hello, world!")
                (doto (update (poll q) :content utf8-str) prn)))

          (is (match? commit (poll q))))))))

(deftest ^:integration fast-producer-slow-consumer
  ;; Test that if the handler function (the consumer) can't keep up with the
  ;; thread that reads logical replication messages from the socket (the
  ;; producer), the consumer exerts backpressure on the producer.
  (let [n 256
        handler-queue (ArrayBlockingQueue. 4)
        slow-consumer (fn []
                        (let [ret (poll handler-queue)]
                          ;; Emulate slow consumer.
                          (Thread/sleep (Duration/ofMillis (rand-int 50)))
                          ret))
        executor (subscriber/flow-controlling-executor :work-queue (ArrayBlockingQueue. 16))
        handler (fn handle
                  ([{:keys [type] :as msg}]
                   (when (= :message type)
                     (let [uuid (-> msg :content utf8-str parse-uuid)]
                       (BlockingQueue/.put handler-queue uuid))))
                  ([msg ack]
                   (handle msg)
                   (ack)))
        options (test-options :executor executor :handler handler :ack-interval (Duration/ofSeconds 1))]
    (with-open [_slot (replication-slot "s")
                client (test-client)
                _sub (subscriber/connect "s" options)]
      (let [expected-uuids (into []
                             (map (fn [_]
                                    (let [uuid (random-uuid)]
                                      (emit-message client "prefix" (str uuid))
                                      uuid)))
                             (range n))
            actual-uuids (into [] (repeatedly n slow-consumer))]
        (is (= expected-uuids actual-uuids))))))

(deftest ^:integration composite-data-type
  (let [q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]

      ;; Create a custom composite data type and a table that uses that data
      ;; type.
      (eq client
        ["CREATE TYPE my_type AS (a INTEGER, b TEXT)"]
        ["CREATE TABLE t (id SERIAL PRIMARY KEY, data my_type)"])

      ;; Create a publication for the table.
      (eq client ["CREATE PUBLICATION p FOR TABLE t"])

      ;; Insert a value of the custom data type into the table.
      (eq client ["INSERT INTO t (data) VALUES (ROW($1, $2))" 42 "hello"])

      (with-open [_sub (connect "s"
                         :publications #{"p"}
                         :handler (q-handler q))]

        (is (match? begin (poll q)))

        (is (match? (type "public" "my_type") (poll q)))

        (is (match? {:type :relation
                     :oid int32?
                     :namespace "public"
                     :relation "t"
                     :replica-identity :primary-key
                     :attributes
                     [{:flags #{:key} :name "id" :data-type-oid 23 :type-modifier -1}
                      {:flags #{} :name "data" :data-type-oid int32? :type-modifier -1}]}
              (poll q)))

        (is (match? {:type :insert
                     :new-row {"id" 1 "data" {42 "hello"}}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? commit (poll q)))))))

(deftest ^:integration data-type-enum
  (let [q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]

      ;; Create an enum and a table that uses that enum.
      (eq client
        ["CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"]
        ["CREATE TABLE person (name text, current_mood mood)"])

      ;; Create a publication for the table.
      (eq client ["CREATE PUBLICATION p FOR TABLE person"])

      (eq client ["INSERT INTO person VALUES ($1, $2::mood)" "Moe" "happy"])

      (with-open [_sub (connect "s"
                         :publications #{"p"}
                         :handler (q-handler q))]

        (is (match? begin (poll q)))

        (is (match? (type "public" "mood") (poll q)))

        (is (match? {:type :relation
                     :oid int32?
                     :namespace "public"
                     :relation "person"
                     :replica-identity :primary-key
                     :attributes
                     [{:flags #{} :name "name" :data-type-oid 25 :type-modifier -1}
                      {:flags #{} :name "current_mood" :data-type-oid int32? :type-modifier -1}]}
              (poll q)))

        (is (match? {:type :insert
                     :new-row {"name" "Moe" "current_mood" "happy"}
                     :schema "public"
                     :table "person"}
              (poll q)))

        (is (match? commit (poll q)))))))

(deftest ^:integration data-type-base
  (let [q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]

      ;; Create a domain and a table that uses that domain.
      (eq client
        ["CREATE DOMAIN us_postal_code AS TEXT CHECK(VALUE ~ '^\\d{5}$' OR VALUE ~ '^\\d{5}-\\d{4}$')"]
        ["CREATE TABLE us_snail_addy (
            address_id SERIAL PRIMARY KEY,
            city TEXT NOT NULL,
            postal us_postal_code NOT NULL
          )"])

      ;; Create a publication for the table.
      (eq client ["CREATE PUBLICATION p FOR TABLE us_snail_addy"])

      (eq client ["INSERT INTO us_snail_addy (city, postal) VALUES ($1, $2::us_postal_code)" "Cornish" "03745"])

      (with-open [_sub (connect "s"
                         :publications #{"p"}
                         :handler (q-handler q))]

        (is (match? begin (poll q)))

        (is (match? (type "" "text") (poll q)))

        (is (match? {:type :relation
                     :oid int32?
                     :namespace "public"
                     :relation "us_snail_addy"
                     :replica-identity :primary-key
                     :attributes
                     [{:flags #{:key} :name "address_id" :data-type-oid 23 :type-modifier -1}
                      {:flags #{} :name "city" :data-type-oid 25 :type-modifier -1}
                      {:flags #{} :name "postal" :data-type-oid int32? :type-modifier -1}]}
              (poll q)))

        (is (match? {:type :insert
                     :new-row {"address_id" 1 "city" "Cornish" "postal" "03745"}
                     :schema "public"
                     :table "us_snail_addy"}
              (poll q)))

        (is (match? commit (poll q)))))))

(deftest ^:integration data-type-unknown
  (let [q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]
      (eq client ["DROP EXTENSION IF EXISTS citext"])
      (eq client ["CREATE EXTENSION citext"])
      (eq client ["CREATE TABLE u (username citext PRIMARY KEY)"])
      (eq client ["CREATE PUBLICATION p FOR TABLE u"])
      (eq client ["INSERT INTO u VALUES ('Alice')"])

      (with-open [sub (subscriber/connect "s"
                        (test-options
                          :publications #{"p"}
                          :handler (q-handler q)))]

        (is (match? begin (poll q)))

        (is (match? (type "public" "citext") (poll q)))

        (is (match? {:type :relation
                     :oid int32?
                     :namespace "public"
                     :relation "u"
                     :replica-identity :primary-key
                     :attributes
                     [{:flags #{:key}
                       :name "username"
                       :data-type-oid int32?
                       :type-modifier -1}]}
              (poll q)))

        (is (thrown-match? ExceptionInfo
              {:oid int32? ::anomalies/category ::anomalies/unsupported}
              (deref sub)))))))

(deftest ^:integration permission-denied-pg-type
  (let [q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]

      ;; Create a user that doesn't have access to pg_catalog.pg_type.
      (eq client
        ["DROP USER IF EXISTS u"]
        ["CREATE USER u WITH PASSWORD 'p'"]
        ["ALTER ROLE u REPLICATION"]
        ["REVOKE SELECT ON pg_type FROM public"]
        ["GRANT SELECT ON pg_type TO postgres"])

      ;; Create a new type. This causes Muutos to attempt to fetch metadata for
      ;; the new type when it first encounters it.
      (eq client
        ["CREATE TYPE suit AS ENUM ('hearts', 'diamonds', 'clubs', 'spades')"]
        ["CREATE TABLE card (rank int, suit suit)"])

      (eq client ["CREATE PUBLICATION p FOR TABLE card"])
      (eq client ["INSERT INTO card VALUES ($1, $2::suit)" 1 "spades"])

      (let [subscriber (subscriber/connect "s"
                         (test-options
                           :user "u"
                           :password "p"
                           :publications #{"p"}
                           :handler (q-handler q)))]
        (is (match? begin (poll q)))

        (is (thrown-match? ExceptionInfo {:cause :insufficient-privilege
                                          :file "aclchk.c"
                                          :error-code "42501"
                                          :routine "aclcheck_error"
                                          :line int?
                                          :severity "ERROR"}
              (deref subscriber)))))))

(deftest ^:integration deref-throws-upon-disconnect
  ;; Check that dereffing the subscriber throws when disconnected from server.
  (let [server (server/start container-opts)
        client (test-client :port (port server) :replication :database)]
    (try
      (sq client (format "CREATE_REPLICATION_SLOT %s LOGICAL pgoutput" "s"))
      (let [subscriber (subscriber/connect "s"
                         (test-options
                           :port (port server)
                           :publications #{"p"}))]
        (.close server)

        (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/unavailable}
              (deref subscriber))))
      (finally
        (AutoCloseable/.close client)))))

(defn ^:private broken-pipe-client []
  (let [lock (ReentrantLock.)]
    (reify client/Client
      (send [_ _message]
        (throw (SocketException. "Broken pipe")))

      (oid [_ _parameter] 25)

      Lockable
      (lock [_] lock)

      AutoCloseable
      (close [_] #_noop))))

(deftest ^:integration fail-type-metadata-retrieval
  ;; Check that dereffing the subscriber throws when it fails to retrieve type
  ;; metadata.
  (let [q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]

      ;; Create a situation where Muutos needs to retrieve type metadata upon
      ;; receiving a logical replication message.
      (eq client
        ["CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"]
        ["CREATE TABLE person (name text, current_mood mood)"])

      (eq client ["CREATE PUBLICATION p FOR TABLE person"])

      ;; Make a change that emits a logical replication message that refers to
      ;; a type Muutos doesn't know yet.
      (eq client ["INSERT INTO person VALUES ($1, $2::mood)" "Moe" "happy"])

      (let [subscriber (subscriber/connect "s"
                         (test-options
                           :sql-client (broken-pipe-client)
                           :publications #{"p"}
                           :handler (q-handler q)))]
        (is (match? begin (poll q)))

        (is (thrown-with-msg? SocketException #"Broken pipe"
              (deref subscriber)))))))

(deftest ^:integration publication-disappears
  (with-open [_slot (replication-slot "s")
              client (test-client)]
    (eq client ["CREATE TABLE t (n int NOT NULL)"])
    (eq client ["CREATE PUBLICATION p FOR TABLE t"])

    (let [subscriber (subscriber/connect "s"
                       (test-options :publications #{"p"}))]

      ;; Publication disappears.
      (eq client ["DROP PUBLICATION p"])

      ;; Effect a change to the table the publication listens on that
      ;; yields a logical replication message
      (eq client ["INSERT INTO t (n) VALUES ($1)" 1])

      ;; Dereferencing the subscriber throws.
      (is (thrown-match? ExceptionInfo {:cause :undefined-object
                                        :error-code "42704"
                                        :routine "get_publication_oid"
                                        :kind ::error/server-error
                                        :severity "ERROR"}
            (deref subscriber))))))

(deftest ^:integration start-lsn
  (let [q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]
      (emit-message client "prefix" "message-1")

      (let [{:keys [segment byte-offset]}
            (-> (emit-message client "prefix" "message-2") first (get "pg_logical_emit_message"))
            lsn (bit-or (bit-shift-left segment 32) byte-offset)]
        (with-open [_sub (connect "s" :handler (q-handler q) :start-lsn lsn)]
          (is (match? begin (poll q)))

          ;; The first message the subscriber gets is message-2. If we didn't
          ;; specify :start-lsn, it would be message-1
          (is (match? (message "prefix" "message-2")
                (update (poll q) :content utf8-str)))

          (is (match? commit (poll q))))))))

(defn ^:private generate-random-bytes
  [kb-size]
  (let [array-size (* kb-size 1024)
        byte-array (byte-array array-size)]
    (doto (SecureRandom.) (.nextBytes byte-array))
    byte-array))

(deftest ^:integration transaction-streaming-commit
  (let [q (SynchronousQueue.)
        bytes-1 (generate-random-bytes 64)
        bytes-2 (generate-random-bytes 64)]
    (with-open [server (server/start
                         (update container-opts :command conj "-c" "logical_decoding_work_mem=64kB"))
                _slot (replication-slot "s" {:port (port server)})
                client (test-client :port (port server))]

      (with-open [_sub (connect "s"
                         :port (port server)
                         :handler (q-handler q)
                         :protocol-version 2
                         :streaming true)]
        (eq client ["START TRANSACTION"])

        (emit-message client "prefix" bytes-1)
        (emit-message client "prefix" bytes-2)

        (is (match? {:type :stream-start
                     :xid int?
                     :segment :first}
              (poll q)))

        (is (match? (message "prefix" bytes-1) (poll q)))
        (is (match? {:type :stream-stop} (poll q)))

        (eq client ["COMMIT"])

        (is (match? {:type :stream-start
                     :xid int?
                     :segment :other}
              (poll q 1 :seconds)))

        (is (match? (message "prefix" bytes-2) (poll q)))
        (is (match? {:type :stream-stop} (poll q)))
        (is (match? {:type :stream-commit
                     :xid int?
                     :commit-lsn int?
                     :tx-end-lsn int?
                     :commit-timestamp instant?}
              (poll q))))

      ;; Large transaction was acked (via :stream-commit); replication resumes
      ;; from this message.
      (emit-message client "prefix" (byte-array [65]))

      (with-open [_sub (connect "s" :port (port server) :handler (q-handler q))]
        (is (match? begin (poll q)))
        (is (match? (message "prefix" (byte-array [65])) (poll q)))
        (is (match? commit (poll q)))))))

(deftest ^:integration transaction-streaming-abort
  (let [q (SynchronousQueue.)
        bytes-1 (generate-random-bytes 64)]
    (with-open [server (server/start
                         (update container-opts :command conj "-c" "logical_decoding_work_mem=64kB"))
                _slot (replication-slot "s" {:port (port server)})
                client (test-client :port (port server))]

      (with-open [_sub (connect "s"
                         :port (port server)
                         :handler (q-handler q)
                         :protocol-version 2
                         :streaming true)]

        (eq client ["START TRANSACTION"])

        (emit-message client "prefix" bytes-1)

        (eq client ["ROLLBACK"])

        (is (match? {:type :stream-start
                     :xid int?
                     :segment :first}
              (poll q)))

        (is (match? (message "prefix" bytes-1) (poll q)))

        (is (match? {:type :stream-stop} (poll q)))

        (is (match? {:type :stream-abort
                     :xid int?
                     :subtransaction-xid int?}
              (poll q))))

      ;; Large transaction was acked (via :stream-abort); replication resumes
      ;; from this message.
      (emit-message client "prefix" (byte-array [65]))

      (with-open [_sub (connect "s" :port (port server) :handler (q-handler q))]
        (is (match? begin (poll q)))
        (is (match? (message "prefix" (byte-array [65])) (poll q)))
        (is (match? commit (poll q)))))))

(deftest ^:integration transaction-streaming-parallel-abort
  (let [q (SynchronousQueue.)
        bytes-1 (generate-random-bytes 64)]
    (with-open [server (server/start
                         (update container-opts :command conj "-c" "logical_decoding_work_mem=64kB"))
                _slot (replication-slot "s" {:port (port server)})
                client (test-client :port (port server))]

      (with-open [_sub (connect "s"
                         :port (port server)
                         :handler (q-handler q)
                         :protocol-version 4
                         :streaming :parallel)]

        (eq client ["START TRANSACTION"])

        (emit-message client "prefix" bytes-1)

        (eq client ["ROLLBACK"])

        (is (match? {:type :stream-start
                     :xid int?
                     :segment :first}
              (poll q)))

        (is (match? (message "prefix" bytes-1) (poll q)))

        (is (match? {:type :stream-stop} (poll q)))

        (is (match? {:type :stream-abort
                     :xid int?
                     :subtransaction-xid int?
                     :abort-lsn int?
                     :tx-timestamp instant?}
              (poll q))))

      ;; Large transaction was acked (via :stream-abort); replication resumes
      ;; from this message.
      (emit-message client "prefix" (byte-array [65]))

      (with-open [_sub (connect "s" :port (port server) :handler (q-handler q))]
        (is (match? begin (poll q)))
        (is (match? (message "prefix" (byte-array [65])) (poll q)))
        (is (match? commit (poll q)))))))

(deftest ^:integration create-alter-insert-update-delete-truncate
  (let [uuid-1 #uuid "13858d5d-c742-498f-89f6-2a34734b10f7"
        uuid-2 #uuid "824298f7-47f6-4689-88e4-dbb64b14655c"
        q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]
      (sq client "create publication p for all tables")

      (with-open [_sub (connect "s" :handler (q-handler q) :publications #{"p"})]
        (eq client
          ["create table t (id uuid primary key, n int8 not null)"]
          ["alter table t replica identity full"])

        (eq client
          ["insert into t (id, n) values ($1, $2)" uuid-1 1]
          ["insert into t (id, n) values ($1, $2)" uuid-2 2]
          ["update t set n = $2 WHERE id = $1" uuid-1 3]
          ["delete from t where id = $1" uuid-1]
          ["truncate table t restart identity"])

        (is (match? begin (poll q)))
        (is (match? {:type :relation} (poll q)))

        (is (match? {:new-row {"id" uuid-1 "n" 1}
                     :schema "public"
                     :table "t"
                     :type :insert} (poll q)))

        (is (match? {:new-row {"id" uuid-2 "n" 2}
                     :schema "public"
                     :table "t"
                     :type :insert} (poll q)))

        (is (match? {:keys ["id" "n"]
                     :old-row {"id" uuid-1 "n" 1}
                     :new-row {"id" uuid-1 "n" 3}
                     :schema "public"
                     :table "t"
                     :replica-identity :full
                     :type :update} (poll q)))

        (is (match? {:keys ["id" "n"]
                     :old-row {"id" uuid-1 "n" 3}
                     :replica-identity :full
                     :schema "public"
                     :table "t"
                     :type :delete} (poll q)))

        (is (match? {:attributes [{:data-type-oid 2950 :flags #{:key} :name "id" :type-modifier -1}
                                  {:data-type-oid 20 :flags #{:key} :name "n" :type-modifier -1}]
                     :namespace "public"
                     :oid int?
                     :relation "t"
                     :replica-identity :full
                     :type :relation} (poll q)))

        (is (= {:parameters #{:restart-identity}
                :targets [{:schema "public" :table "t"}]
                :type :truncate} (poll q)))

        (is (match? commit (poll q)))))))

(deftest nil-val-strategy
  (let [q (SynchronousQueue. true)]
    (with-open [_slot (replication-slot "s")
                client (test-client)]

      (sq client "create publication p for all tables")

      (with-open [_sub (connect "s" :handler (q-handler q) :publications #{"p"})]
        (eq client ["create table t (id integer primary key generated always as identity, t text null)"])
        (eq client ["insert into t (t) values ($1)" "t"])

        (is (match? begin (poll q)))
        (is (match? {:type :relation} (poll q)))

        (is (match? {:new-row {"id" 1 "t" "t"}
                     :schema "public"
                     :table "t"
                     :type :insert}
              (poll q)))

        (is (match? commit (poll q)))

        (eq client ["insert into t (t) values ($1)" nil])

        (is (match? begin (poll q)))

        (is (match? {:type :insert
                     :new-row {"id" 2 "t" matchers/absent}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? commit (poll q)))

        (eq client ["update t set t = $1" "v"])

        (is (match? begin (poll q)))

        (is (match? {:type :update
                     :new-row {"id" 1 "t" "v"}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? {:type :update
                     :new-row {"id" 2 "t" "v"}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? commit (poll q)))

        (eq client ["update t set t = $2 where id = $1" 1 nil])

        (is (match? begin (poll q)))

        (is (match? {:type :update
                     :new-row {"id" 1 "t" matchers/absent}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? commit (poll q)))

        (eq client ["alter table t replica identity full"])
        (eq client ["update t set t = $2 where id = $1" 2 nil])

        (is (match? begin (poll q)))
        (is (match? {:type :relation} (poll q)))

        (is (match? {:type :update
                     :old-row {"id" 2 "t" "v"}
                     :new-row {"id" 2 "t" matchers/absent}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? commit (poll q)))

        (eq client ["update t set t = $2 where id = $1" 2 "v"])

        (is (match? begin (poll q)))

        (is (match? {:type :update
                     :old-row {"id" 2 "t" matchers/absent}
                     :new-row {"id" 2 "t" "v"}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? commit (poll q)))

        (eq client ["delete from t where id = $1" 1])

        (is (match? begin (poll q)))

        (is (match? {:type :delete
                     :replica-identity :full
                     :keys ["id" "t"]
                     :old-row {"id" 1 "t" matchers/absent}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? commit (poll q)))

        (eq client ["alter table t replica identity default"])
        (eq client ["delete from t where id = $1" 2])

        (is (match? begin (poll q)))
        (is (match? {:type :relation} (poll q)))

        (is (match? {:type :delete
                     :replica-identity :primary-key
                     :keys ["id"]
                     :old-row {"id" 2 "t" matchers/absent}
                     :schema "public"
                     :table "t"}
              (poll q)))

        (is (match? commit (poll q)))))))

(deftest wal-sender-timeout
  (with-open [server (server/start
                       (-> container-opts
                         (update :command conj "-c" "wal_sender_timeout=1")))
              _slot (replication-slot "s" {:port (port server)})
              sub (connect "s" :port (port server))]
    (is (thrown-match? ExceptionInfo {::anomalies/category ::anomalies/unavailable}
          (deref sub)))))
