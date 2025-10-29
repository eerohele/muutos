# Muutos [![0 dependencies!](https://0dependencies.dev/0dependencies.svg)](https://0dependencies.dev)

>/ˈmuːtos/ — **change** _(process of making or becoming different)_

Muutos is a [zero-dependency](https://0dependencies.dev/) [Clojure](https://clojure.org) library for reacting to changes in a [PostgreSQL](https://www.postgresql.org/) database.

You give Muutos a callback function. Muutos then subscribes to a [PostgreSQL logical decoding stream](https://www.postgresql.org/docs/current/logicaldecoding.html) and calls the callback function on every logical replication message PostgreSQL sends it.

Muutos uses the built-in [`pgoutput`](https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION) logical decoding output plugin and requires no additional dependencies on the PostgreSQL server.

**NOTE**: Muutos is in **alpha**. I'll avoid making breaking changes to the API to the best of my ability, but they are possible.

## Example

```clojure
(require '[clojure.pprint :as pprint])
(require '[muutos.sql-client :as sql])
(require '[muutos.subscriber :as subscriber])

;; Muutos comes with a SQL client. You can use it to set up logical
;; replication, as well as for diagnostics and debugging.
;;
;; Connect to the PostgreSQL database listening on localhost:5432, using the
;; username and password combination "postgres:postgres".
(def pg (sql/connect))

;; Create a logical replication slot called "my_slot".
(sql/create-slot pg "my_slot")

;; Create a publication. A publication is a group of tables whose data to
;; replicate.
(sql/eq pg ["CREATE PUBLICATION my_pub FOR ALL TABLES"])

;; Create a logical replication message handler function.
;;
;; This handler function pretty-prints the message into stdout.
(defn handle
  ([msg]
   (pprint/pprint msg))
  ([msg ack]
   (handle msg)
   ;; Acknowledge the message, telling PostgreSQL that it's free to remove this
   ;; transaction from its write-ahead log.
   (ack)))

;; Connect to the logical replication slot "my_slot" and subscribe to
;; publication "my_pub".
(def subscriber
  (subscriber/connect "my_slot"
    :publications #{"my_pub"}
    :handler handle))

;; Create a table whose changes publication "my_pub" listens on and insert data
;; into it.
(sql/eq pg
  ;; This is a pipeline of queries. If one query fails, PostgreSQL will not
  ;; execute the rest.
  ["DROP TABLE IF EXISTS t"]
  ["CREATE TABLE t (id int PRIMARY KEY, s TEXT NOT NULL)"]
  ["INSERT INTO t (id, s) VALUES ($1, $2) RETURNING *" 1 "a"])

;; Evaluating the form above will yield messages like these into stdout:
{:type :begin
 :lsn 37931232
 :commit-timestamp #instant "2025-05-26T12:06:45.873754Z"
 :xid 992}
{:type :relation
 :oid 16931
 :namespace "public"
 :relation "t"
 :replica-identity :primary-key
 :attributes
 [{:flags #{:key} :name "id" :data-type-oid 23 :type-modifier -1}
  {:flags #{} :name "s" :data-type-oid 25 :type-modifier -1}]}
{:type :insert
 :new-row {"id" 1 "s" "a"}
 :schema "public"
 :table "t"}
{:type :commit
 :commit-lsn 37931232
 :tx-end-lsn 37932568
 :commit-timestamp #instant "2025-05-26T12:06:45.873754Z"}

;; You can also emit logical replication messages (with binary payloads)
;; without making any changes to a table.
;;
;; This is useful if you want to decouple the structure of your database from
;; the messages you send.
;;
;; Unlike LISTEN/NOTIFY, logical replication messages persist even if they're
;; produced while the subscriber is disconnected.
;;
;; For more on using logical replication messages for change data capture, see:
;;
;; https://www.infoq.com/articles/wonders-of-postgres-logical-decoding-messages/
(sql/emit-message pg "my-prefix" (.getBytes "Hello, world!" "UTF-8"))

;; Yields:
{:type :begin
 :lsn 37958776
 :commit-timestamp #instant "2025-05-26T12:16:38.133276Z"
 :xid 999}
{:type :message
 :flags :transactional
 :lsn 37958776
 :prefix "my-prefix"
 ;; The UTF-8 bytes for the string "Hello, world!".
 :content #bytes "48656C6C6F2C20776F726C6421"}
{:type :commit
 :commit-lsn 37958776
 :tx-end-lsn 37958824
 :commit-timestamp #instant "2025-05-26T12:16:38.133276Z"}

;; A subscriber is AutoCloseable.
(.close subscriber)

;; So is a SQL client.
(.close pg)
```

## Documentation

- [Documentation](/docs/INDEX.md)
- [API documentation](/docs/API.md)

## Acknowledgements

- Emil Lenngren for [the invaluable Npgsql data type reference](https://www.npgsql.org/dev/types.html).
