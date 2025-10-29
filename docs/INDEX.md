# Muutos

Muutos is a zero-dependency [PostgreSQL](https://www.postgresql.org/)
[logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html)
client library written in and for Clojure.

You can use Muutos to subscribe to changes in a PostgreSQL database.
You tell Muutos which changes you're interested in and give it a callback
function. When such a change occurs, Muutos calls the callback function you
give it with a Clojure map that describes the change.

Muutos uses the built-in [`pgoutput`](https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION) logical decoding output plugin, and requires no additional dependencies on the PostgreSQL server.

## Rationale

Applications that write to a database often need to do something (only) if the
write succeeds. For example:

- Synchronize data to other data sources (e.g. a search or analytics engine)
- Broadcast changes to browsers over a SSE or WebSocket connection
- Send data to other services via a message broker
- Update or invalidate a cache

Publishing change data reliably is not a trivial task. Writing to two data stores consecutively, or _dual writing_, seems like the most obvious solution, but is exceedingly difficult to implement reliably[^5]. You cannot act on a
change before or during a database transaction, because the transaction can
fail. Neither can you act on a change _after_ your app has successfully committed the transaction because your
app may crash or quit [after the transaction succeeds but before the
reaction occurs](https://brandur.org/job-drain).

To capture change data reliably, there are two common solutions. One solution is to transact an outgoing message into a database table and poll it. This is known as the [transactional outbox pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html). Another is an approach called "change data capture"[^6] (CDC). That means tapping into the [logical decoding stream](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-EXPLANATION-LOG-DEC) of the database. That is the approach Muutos uses.

Like all technologies, both options have their pros and cons.  For an extended discussion on change data capture and its benefits and drawbacks, see Chapter 11 of [_Designing Data-Intensive Applications_](https://dataintensive.net/) by Martin Kleppmann (2017).

Here is a list of some of the benefits and drawbacks of using change data capture vs. a transactional outbox, as well as comparisons between Muutos and other change data capture tools.

### Benefits of using change data capture instead of polling

- Near-instant; no polling interval delays.
- No polling-incurred performance penalty.
- Retains ordering.
- No need to design and implement a transactional outbox table (including pruning it). If you have multiple apps  (e.g. microservices) that need to react to changes in the database, you don't need to implement a transactional outbox in each one.
- You can use [`pg_logical_emit_message`](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-REPLICATION) to [emit arbitrary binary payloads](https://www.infoq.com/articles/wonders-of-postgres-logical-decoding-messages/) from the PostgreSQL server, transactionally or not. This allows you to decouple the messages you send from the structure of your database without having to implement an outbox table.
- With logical decoding, you can use [pg_cron](https://github.com/citusdata/pg_cron) together with [`pg_logical_emit_message`](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-REPLICATION) as a lightweight task scheduler.

### Drawbacks of using change data capture instead of polling

- Increased disk use while PostgreSQL retains write-ahead logs until the change data capture tool has successfully processed changes. This can be a problem especially is the tool is offline for an extended length of time. (An outbox table can also eat up disk space if the outbox processor is offline, of course.)
- When replicating tables (as opposed to using [`pg_logical_emit_message`](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-REPLICATION)), you must make sure that the change data capture tool is resilient to database schema changes. PostgreSQL does not write [data definition language](https://www.postgresql.org/docs/current/ddl.html) (DDL) statements into its write-ahead log.

### Benefits of using Muutos instead of other change data capture tools

- Muutos has zero dependencies on the PostgreSQL server and on the client.
- Muutos is designed introduce minimal operational overhead by slotting into your existing infrastructure (it does not require e.g. [Apache Kafka](https://kafka.apache.org/)).
- Muutos emits Clojure data structures ([EDN](https://github.com/edn-format/edn)) directly, avoiding the loss of performance and fidelity of using JSON as an intermediate format.

### Drawbacks of using Muutos instead of other change data capture tools

- Muutos only supports PostgreSQL.
- Muutos is not (yet) battle-tested.
- Muutos has massively fewer features than tools such as [Debezium](https://debezium.io/).
- Muutos makes no attempt at [exactly-once delivery](https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/). You must be able to live with duplicate messages (only when Muutos shuts down uncleanly).
- ...and any number of other drawbacks yet to be discovered.

## Subscribing to a logical replication stream

To make Muutos listen on changes to a PostgreSQL database, you must:

1. Configure PostgreSQL for [logical replication](https://www.postgresql.org/docs/current/logical-replication.html).
1. Create a [publication](https://www.postgresql.org/docs/current/sql-createpublication.html).
1. Create a [logical replication slot](https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-REPLICATION-SLOT).
1. Define a logical replication message handler function.
1. Connect a Muutos subscriber to the database.

### Prerequisites

> [!NOTE]
> Muutos requires Clojure 1.12 or newer. Muutos is tested on PostgreSQL 17 and Java 25, but might work with older versions of each.

This documentation assumes that you have a PostgreSQL database server listening
on `localhost:5432` and that it accepts connections with these credentials:

- **Username**: `postgres`
- **Password**: `postgres`
- **Database**: `postgres`

### Configuring PostgreSQL for logical decoding

To configure PostgreSQL for logical decoding, on your PostgreSQL instance:

1. Set [`wal_level`](https://www.postgresql.org/docs/current/runtime-config-wal.html#RUNTIME-CONFIG-WAL-SETTINGS) to `logical`.
1. (_Optional_) Set [`max_replication_slots`](https://www.postgresql.org/docs/current/runtime-config-replication.html#RUNTIME-CONFIG-REPLICATION-SENDER) to the maximum number of replication slots you need.
1. (_Optional_) Set [`max_wal_senders`](https://www.postgresql.org/docs/current/runtime-config-replication.html#RUNTIME-CONFIG-REPLICATION-SENDER) to the maximum number of allowed replication connections.

### Creating a publication

Per PostgreSQL documentation:

> A publication is ... a group of tables whose data changes are intended to be
> replicated through logical replication.

To create a publication, you can use [the SQL client](#working-with-sql) Muutos comes with.

To begin with, let's require a few [vars](https://clojure.org/reference/vars) we need and then connect a SQL client to the PostgreSQL database.

```clojure
user=> (require '[muutos.sql-client :as sql :refer [eq]])
nil
user=> (def pg (sql/connect))
#'user/pg
```

The PostgreSQL instance this documentation uses includes the [Pagila](https://github.com/devrimgunduz/pagila) database. Let's query it to test the SQL client.

```clojure
user=> (def q "SELECT release_year, initcap(title) AS title, rental_rate, description
               FROM film
               ORDER BY release_year DESC, title ASC
               LIMIT 10")
#'user/q
user=> (require '[clojure.pprint :refer [print-table]])
nil
user=> (print-table (eq pg [q]))
| release_year |                 title | rental_rate |                                                                                              description |
|--------------+-----------------------+-------------+----------------------------------------------------------------------------------------------------------|
|         2024 |       Alamo Videotape |        0.99 |                A Boring Epistle of a Butler And a Cat who must Fight a Pastry Chef in A MySQL Convention |
|         2024 |       Baked Cleopatra |        2.99 |    A Stunning Drama of a Forensic Psychologist And a Husband who must Overcome a Waitress in A Monastery |
|         2024 |         Beauty Grease |        4.99 |          A Fast-Paced Display of a Composer And a Moose who must Sink a Robot in An Abandoned Mine Shaft |
|         2024 |         Boulevard Mob |        0.99 |                A Fateful Epistle of a Moose And a Monkey who must Confront a Lumberjack in Ancient China |
|         2024 |  Breakfast Goldfinger |        4.99 |                       A Beautiful Reflection of a Student And a Student who must Fight a Moose in Berlin |
|         2024 | Bulworth Commandments |        2.99 |              A Amazing Display of a Mad Cow And a Pioneer who must Redeem a Sumo Wrestler in The Outback |
|         2024 |            Cause Date |        2.99 |              A Taut Tale of a Explorer And a Pastry Chef who must Conquer a Hunter in A MySQL Convention |
|         2024 |            Clue Grail |        4.99 |                  A Taut Tale of a Butler And a Mad Scientist who must Build a Crocodile in Ancient China |
|         2024 |     Conspiracy Spirit |        2.99 | A Awe-Inspiring Story of a Student And a Frisbee who must Conquer a Crocodile in An Abandoned Mine Shaft |
|         2024 |    Cruelty Unforgiven |        0.99 |                               A Brilliant Tale of a Car And a Moose who must Battle a Dentist in Nigeria |
```

Now that we're connected to the PostgreSQL database, we can create a
publication. A publication specifies which changes you want to listen on.

For example, you can say that you only want to listen on inserts and
deletes on relation `bar` in schema `foo`:

```sql
CREATE PUBLICATION pub
FOR TABLE bar
IN SCHEMA foo
WITH (publish = 'insert, delete');
```

For this example, however, we'll just say we want to listen on changes to
all relations in the database.

```clojure
user=> (eq pg ["CREATE PUBLICATION my_pub FOR ALL TABLES"])
[]
```

For more information on publications, see [Publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) and
and [CREATE PUBLICATION](https://www.postgresql.org/docs/current/sql-createpublication.html)
in the PostgreSQL documentation.

### Creating a logical replication slot

When you create a replication slot, PostgreSQL begins retaining information
on changes to the database in its memory and on its disk. It does this by
writing more detailed information about every changes into its [write-ahead log](https://www.postgresql.org/docs/current/runtime-config-wal.html) (WAL)
and storing those changes until we tell PostgreSQL that it's free to truncate
the log.

For more information on creating replication slots, see [Replication Slots](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS)
in the PostgreSQL documentation.

```clojure
user=> (sql/create-slot pg "my_slot")
[{"pg_create_logical_replication_slot"
  {"my_slot" #muutos.type.LogSequenceNumber{:segment 0 :byte-offset 37921760}}}]
```

### Defining a handler function

Next, we'll define the handler function Muutos will call on every
change-describing message PostgreSQL sends it.

For this example, we will put every message into a concurrent queue. We will
also make a queue we can use to acknowledge transactions as having been
successfully processed.

```clojure
user=> (import '(java.util.concurrent LinkedBlockingQueue))
java.util.concurrent.LinkedBlockingQueue
user=> (def rcvq (LinkedBlockingQueue. 1))
#'user/recvq
user=> (def ackq (LinkedBlockingQueue. 1))
#'user/ackq
```

Muutos handler functions have two arities: the **receive** arity (`[msg]`) and the **commit** arity (`[msg ack]`).

Muutos calls the one-argument receive arity with `msg`, a Clojure map that
describes the change PostgreSQL informs Muutos about.

Muutos calls the two-argument commit arity with a `msg` (as above) and `ack`,
a zero-argument function that, when called, tells Muutos to inform PostgreSQL
that it (or, more precisely, your handler function) has successfully handled the
transaction the message pertains to. Once PostgreSQL receives the
acknowledgement, it will truncate its write-ahead log to free memory and disk space.

If you do not acknowledge a transaction, when you reconnect Muutos to a
PostgreSQL server, the server will re-send Muutos all changes that belong
to that transaction.

Here is our handler function:

```clojure
user=> (import '(java.nio.charset StandardCharsets))
java.nio.charset.StandardCharsets
user=> (defn handle
         ;; recv arity
         ([{:keys [type] :as msg}]
          (LinkedBlockingQueue/.put rcvq
            (cond-> msg
              ;; Messages of type :message are logical decoding messages
              ;; emitted using the pg_logical_emit_message function.
              ;;
              ;; Logical decoding messages carry a binary payload. In this
              ;; example, we're going to be working solely with strings, so
              ;; we'll decode the payload of every message into a UTF-8 string.
              ;;
              ;; Every other message type we'll retain as is.
              (= type :message)
              (update :content
                (fn [^bytes bs]
                  (String. bs StandardCharsets/UTF_8))))))

         ;; commit arity
         ([msg ack]
          ;; To handle all messages the same way, call the recv arity with the
          ;; message.
          (handle msg)

          ;; Wait for an item to appear in the acknowledgement queue. When that
          ;; happens, call the ack function to inform PostgreSQL that we
          ;; successfully processed this batch of changes.
          (when (.take ackq)
            (ack))))
#'user/handle
```

### Connecting a Muutos subscriber to a database

We now have everything we need to connect a Muutos subscriber to a
PostgreSQL database, so let's do that.

```clojure
user=> (require '[muutos.subscriber :as subscriber])
nil
user=> (def subscriber
         ;; The only required argument to muutos.subscribe/connect is the name
         ;; of the logical replication slot you want Muutos to use.
         (subscriber/connect "my_slot"

           ;; Optionally, give Muutos a set of publication names to subscribe
           ;; to.
           ;;
           ;; If you only want to subscribe to logical replication messages
           ;; emitted using the pg_logical_emit_message PostgreSQL function,
           ;; you do not need to subscribe to a publication.
           :publications #{"my_pub"}

           ;; Tell Muutos to use the handler function we defined above.
           ;;
           ;; If you do not pass a handler function, Muutos uses a default
           ;; handler function, which is a no-op.
           ;;
           ;; By default, to execute the handler function, Muutos creates and
           ;; uses a single-thread ExecutorService that works off a bounded
           ;; queue. If the queue is full and the thread is busy, the executor
           ;; tells the producer (the thread reading logical replication
           ;; messages from the socket connected to PostgreSQL) to back off.
           ;;
           ;; To use a custom ExecutorService, see the :executor and
           ;; :executor-close-fn options of muutos.subscriber/connect.
           :handler handle))
#'user/subscriber
```

A Muutos subscriber is now connected to the database.

The most straightforward way to test the subscriber is to use the
`pg_logical_emit_message` PostgreSQL function to emit a logical replication
message. You can use `muutos.sql-client/emit-message` to call
`pg_logical_emit_message`.

```clojure
user=> (sql/emit-message pg "my-prefix" "Hello, world!")
[{"pg_logical_emit_message" #muutos.type.LogSequenceNumber{:segment 0 :byte-offset 37921896}}]
```

In response to the message we emitted, Muutos relays three messages to our
handler function: `:begin`, `:message`, and `:commit`.

```clojure
user=> (import '(java.util.concurrent TimeUnit))
java.util.concurrent.TimeUnit
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :begin
 :lsn 37921896
 ;; #instant is a java.time.Instant (see https://github.com/tonsky/clojure-plus?tab=readme-ov-file#clojureprint)
 :commit-timestamp #instant "2025-10-04T11:09:38.768184Z"
 :xid 989}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :message
 :flags :transactional
 :lsn 37921896
 :prefix "my-prefix"
 :content "Hello, world!"}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :commit
 :commit-lsn 37921896
 :tx-end-lsn 37921944
 :commit-timestamp #instant "2025-10-04T11:09:38.768184Z"}
```

Now that we've processed the messages, we must acknowledge the transaction
as having been successfully processed. This tells PostgreSQL that it is free
to truncate its write-ahead log (WAL) to free disk space, and that it does
not need to re-send information regarding that transaction to Muutos.

To do that, we'll offer a message into the acknowledgement queue (`ackq`).

```clojure
;; Before acknowledging the message, we can check the current LSN of the replication slot.
user=> (sql/eq pg ["SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'my_slot'"])
[{"confirmed_flush_lsn" #muutos.type.LogSequenceNumber{:segment 0 :byte-offset 37921760}}]
;; Then, we can acknowledge the message.
user=> (.offer ackq true)
true
;; By default, Muutos waits for 10 seconds after successfully processing each
;; transaction before acknowledging it to PostgreSQL.
;;
;; You can configure this duration using the :ack-interval option of
;; muutos.subscriber/subscribe.
;;
;; Once 10 seconds have elapsed, we can check the current LSN of the replication
;; slot once again:
user=> (Thread/sleep 5000)
nil
user=> (sql/eq pg ["SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'my_slot'"])
[{"confirmed_flush_lsn" #muutos.type.LogSequenceNumber{:segment 0 :byte-offset 37921944}}]
;; We can see that the current LSN is the same as the LSN of the transaction
;; (the :tx-end-lsn of the :commit message).
```

### Recovering from errors

Muutos [does not attempt to recover from errors such as connection
failures](https://en.wikipedia.org/wiki/Crash-only_software).

Instead, it is designed to be used with systems such as
[Kubernetes](https://kubernetes.io/) or [systemd](https://systemd.io/) (with [`Restart=on-failure`](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html#Restart=))
that are able to restart the application running Muutos upon failure.

To support this behavior, you must make sure that if Muutos encounters a
failure, the process running Muutos exits with a non-zero exit code.

To do this, in your [main entry point](https://clojure.org/reference/repl_and_main),
dereference the Muutos subscriber:

```clojure
(defn -main [& args]
  @(subscriber/connect "my_slot"
     :handler (fn ([msg] ,,,) ([msg ack] ,,,))))
```

If the subscriber encounters an exception, dereferencing the subscriber will complete and throw the exception.

If you're embedding Muutos into your app and don't want Muutos to crash the entire app when it encounters a fatal error, you must implement some sort of error recovery protocol yourself (e.g. retry reconnecting until success).

### Replicating old values of update and delete operations

By default, when PostgreSQL emits a message as a result of an [`UPDATE`](https://www.postgresql.org/docs/current/sql-update.html) or a [`DELETE`](https://www.postgresql.org/docs/current/sql-delete.html) operation, it only broadcasts the data _after_ the operation.

```clojure
user=> (eq pg
         ["CREATE TABLE t (n INT NOT NULL)"]
         ["INSERT INTO t (n) VALUES ($1)" 1])
[[] []]
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :begin
 :lsn 37924520
 :commit-timestamp #instant "2025-10-05T17:52:09.360740Z"
 :xid 990}
;; When you create or alter a table, PostgreSQL emits a :relation message that
;; describes the table.
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :relation
 :oid 16924
 :namespace "public"
 :relation "t"
 :replica-identity :primary-key
 :attributes [{:flags #{} :name "n" :data-type-oid 23 :type-modifier -1}]}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :insert
 :new-row {"n" 1} ; only new row
 :schema "public"
 :table "t"}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :commit
 :commit-lsn 37924520
 :tx-end-lsn 37924928
 :commit-timestamp #instant "2025-10-05T17:52:09.360740Z"}
user=> (.offer ackq true)
true
```

If you need both the old data (pre-update or pre-delete) and the new data, set [`REPLICA IDENTITY FULL`](https://www.postgresql.org/docs/current/logical-replication-publication.html#LOGICAL-REPLICATION-PUBLICATION-REPLICA-IDENTITY) on the source table:

```clojure
user=> (eq pg ["ALTER TABLE t REPLICA IDENTITY FULL"])
[]
```

Each `:update` message now contains both `:old-row` and `:new-row`.

```clojure
user=> (eq pg ["UPDATE t SET n = $1 WHERE n = $2" 2 1])
[]
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :begin ...}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :relation ...}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :update
 :replica-identity :full
 :keys ["n"]
 :old-row {"n" 1} ; both the old row...
 :new-row {"n" 2} ; ...and the new row
 :schema "public"
 :table "t"}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :commit ...}
user=> (.offer ackq true)
true
```

As does each `:delete` message:

```clojure
user=> (eq pg ["DELETE FROM t"])
[]
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :begin ...}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :delete
 :replica-identity :full
 :keys ["n"]
 :old-row {"n" 2} ; the row that was deleted
 :schema "public"
 :table "t"}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :commit ...}
user=> (.offer ackq true)
true
user=> (eq pg ["DROP TABLE t"])
[]
```

### Suppressing redundant updates

To tell PostgreSQL to only send logical replication messages for rows that
actually changed, you have two options:

- Use a `WHERE` clause in your `UPDATE` query to only update rows that have changed
- Use the built-in `suppress_redudant_updates_trigger()` trigger:

```clojure
user=> (eq pg ["CREATE TABLE n (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL)"])
[]
user=> (def q "CREATE TRIGGER suppress_redundant_updates
               BEFORE UPDATE ON n
               FOR EACH ROW
               EXECUTE FUNCTION suppress_redundant_updates_trigger()")
#'user/q
user=> (eq pg [q])
[]
user=> (def uuid
         (-> (eq pg ["INSERT INTO n (name) VALUES ('John Doe') RETURNING id"]) first (get "id")))
#'user/uuid
user=> (eq pg ["UPDATE n SET name = 'John Doe' WHERE id = $1" uuid])
[]
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
nil
```

Nothing actually changed, so PostgreSQL does not emit a logical replication
message.

Let's try executing an `UPDATE` statement that actually changes something.

```clojure
user=> (eq pg ["UPDATE n SET name = 'Jane Doe' WHERE id = $1" uuid])
[]
;; This time, PostgreSQL emits a logical replication message.
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :begin
 :lsn 37955032
 :commit-timestamp #instant "2025-10-05T18:00:18.920197Z"
 :xid 1001}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :update
 :keys ["id"]
 :new-row {"id" #uuid "6e2b1426-5f24-4282-b55d-8888d8c677dd" "name" "Jane Doe"}
 :schema "public"
 :table "n"}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :commit
 :commit-lsn 37955032
 :tx-end-lsn 37955080
 :commit-timestamp #instant "2025-10-05T18:00:18.920197Z"}
user=> (eq pg ["DROP TABLE n"])
[]
```

### Handling TOASTed values

PostgreSQL has a feature called [TOAST](https://www.postgresql.org/docs/current/storage-toast.html) (The Oversized-Attribute Storage Technique) for compressing and/or breaking up large values into chunks.

If you update a table row that contains such a large value, PostgreSQL will, by default, not send the large value to Muutos. Instead, it will send Muutos a marker that indicates an unchanged TOASTed value. Muutos translates these markers into `:unchanged-toasted-value` keywords.

To tell PostgreSQL to always send the TOASTed value instead of the unchanged TOASTed value marker, you can set [`REPLICA IDENTITY FULL`](https://www.postgresql.org/docs/current/logical-replication-publication.html#LOGICAL-REPLICATION-PUBLICATION-REPLICA-IDENTITY) on the table that contains the value.

Here's a REPL session that illustrates how PostgreSQL and Muutos handle TOASTed values.

```clojure
;; Create a table and configure it to use TOAST.
user=> (eq pg
         ["CREATE TABLE t (id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY, m TEXT NOT NULL, s TEXT NOT NULL)"]
         ["ALTER TABLE t SET (toast_tuple_target = 128)"])
[[] [] []]
;; Insert a value into the table that PostgreSQL will TOAST.
user=> (eq pg ["INSERT INTO t (m, s) VALUES ('a', repeat('x', 10000)) RETURNING id"])
[{"id" 1}]
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :begin
 :lsn 38014344
 :commit-timestamp #instant "2025-10-06T09:30:16.481710Z"
 :xid 994}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :relation
 :oid 16934
 :namespace "public"
 :relation "t"
 :replica-identity :primary-key
 :attributes
 [{:flags #{:key} :name "id" :data-type-oid 23 :type-modifier -1}
  {:flags #{} :name "m" :data-type-oid 25 :type-modifier -1}
  {:flags #{} :name "s" :data-type-oid 25 :type-modifier -1}]}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :insert
 :new-row {"id" 1
           "m" "a"
           ;; truncated (full) value
           "s" "xxx..."}
 :schema "public"
 :table "t"}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :commit
 :commit-lsn 38014344
 :tx-end-lsn 38014392
 :commit-timestamp #instant "2025-10-06T09:30:16.481710Z"}
user=> (.offer ackq true)
true
user=> (eq pg ["UPDATE t SET m = 'b' WHERE m = 'a' RETURNING id"])
[{"id" 1}]
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :begin ...}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :update
 :keys ["id"]
 :new-row {"id" 1
           "m" "b"
           ;; the value of "s" is TOASTed, and the UPDATE didn't change it,
           ;; so PostgreSQL sends the "unchanged toasted value" marker.
           ;;
           ;; To have PostgreSQL send the full value, use ALTER TABLE t
           ;; REPLICA IDENTITY FULL.
           "s" :unchanged-toasted-value}
 :schema "public"
 :table "t"}
user=> (.poll rcvq 500 TimeUnit/MILLISECONDS)
{:type :commit ...}
user=> (.offer ackq true)
true
```

### Streaming large in-progress transactions

Muutos supports [streaming of large in-progress transactions](https://www.postgresql.org/docs/current/logicaldecoding-streaming.html).

To use large transaction streaming, you must use at least [`pgoutput` protocol version 2](https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS) and use `:streaming true` (or at least protocol version 4 with `:streaming :parallel`):

Normally, PostgreSQL sends Muutos messages only when a transaction successfully completes. However, in streaming mode, if a transactions exceeds the size of the `logical_decoding_work_mem` PostgreSQL setting, PostgreSQL will send Muutos messages while the transaction is still in progress. Messages for in-progress transactions look like this:

```clojure
;; PostgreSQL emits the :stream-start message at the start of streaming an
;; in-progress transaction.
{:type :stream-start :xid 3 :segment :first}
;; The :stream-stop message indicates that the transaction remains in progress,
;; but PostgreSQL hasn't finished sending Muutos all data pertaining to the
;; transaction yet.
{:type :stream-stop}
;; The :stream-commit message indicates that the transaction was successfully
;; committed.
{:type :stream-commit
 :xid 3
 :commit-lsn 2
 :tx-end-lsn 18
 :commit-timestamp #instant "2026-07-29T13:38:08.778Z"}
;; The :stream-abort message indicates that the transaction was rolled back.
{:type :stream-abort
 :xid 3
 :subtransaction-xid 4
 ;; PostgreSQL sends :tx-timestamp and :abort-lsn only if :protocol-version 4
 ;; and :streaming :parallel.
 :tx-timestamp #instant "2026-07-29T13:34:01.002Z"
 :abort-lsn 21}
```

### Closing the subscriber

The Muutos subscriber implements [`java.lang.AutoCloseable`](https://download.java.net/java/early_access/jdk25/docs/api/java.base/java/lang/AutoCloseable.html).

```clojure
user=> (.close subscriber)
nil
```

## Working with SQL

Muutos includes a SQL client that's suitable for setting up logical
replication, diagnostics, debugging, and low-throughput scenarios.

The SQL client does not support pooling connections, and is therefore not suitable for high-throughput use cases. If you need a high-throughput PostgreSQL client, consider [pg2](https://github.com/igrishaev/pg2) or [next.jdbc](https://github.com/seancorfield/next-jdbc).

Additionally, Muutos does not automatically prepare statements like the [PostgreSQL JDBC Driver](https://jdbc.postgresql.org/documentation/server-prepare/#server-prepared-statements) does. This means repeated executions of the same extended query (using `muutos.sql-client/eq`) is slower than with JDBC.

### Connecting to a PostgreSQL server

By default, `muutos.sql-client/connect` connects to a PostgreSQL server
listening on `localhost:5432` and accepts connections from the user `postgres`
with the password `postgres`.

```clojure
user=> (require '[muutos.sql-client :as sql :refer [eq]])
nil
user=> (def pg (sql/connect))
#'user/pg
```

### Executing simple queries

The `muutos.sql-client/sq` function implements the PostgreSQL [simple query protocol](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY). A simple query is a SQL command string that does not support parameters.

```clojure
user=> (print-table (sq pg "SELECT * FROM pg_sequence LIMIT 5"))
| seqrelid | seqtypid | seqstart | seqincrement |              seqmax | seqmin | seqcache | seqcycle |
|----------+----------+----------+--------------+---------------------+--------+----------+----------|
|    16386 |       20 |        1 |            1 | 9223372036854775807 |      1 |        1 |    false |
|    16405 |       20 |        1 |            1 | 9223372036854775807 |      1 |        1 |    false |
|    16456 |       20 |        1 |            1 | 9223372036854775807 |      1 |        1 |    false |
|    16468 |       20 |        1 |            1 | 9223372036854775807 |      1 |        1 |    false |
|    16476 |       20 |        1 |            1 | 9223372036854775807 |      1 |        1 |    false |
```

Muutos does not keywordize column names by default. To add keywordization,
use the `:key-fn` option of `muutos.sql-client/connect`.

Let's first close the existing connection.

```clojure
user=> (.close pg)
nil
```

Then let's open a new connection that uses a custom `:key-fn` and try it out:

```clojure
user=> (def pg
         (letfn [(key-fn [_table-oid attr-name] (keyword attr-name))]
           (sql/connect :key-fn key-fn)))
#'user/pg
user=> (eq pg ["SELECT 1 AS a, 2 AS b"])
[{:a 1 :b 2}]
```

### Executing extended queries

The `muutos.sql-client/eq` function implements the PostgreSQL [extended query protocol](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY).

An extended query accepts parameters.

```clojure
user=> (print-table
         (eq pg ["SELECT typname, oid, typarray FROM pg_type WHERE typname = ANY($1)"
                 (into-array String ["json" "jsonb"])]))
| typname |  oid | typarray |
|---------+------+----------|
|    json |  114 |      199 |
|   jsonb | 3802 |     3807 |
```

The SQL client supports a wide range of Clojure and Java data types both as parameters and in query results:

```clojure
user=> (eq pg ["SELECT $1 AS uuid, $2 AS timestamptz"
               (random-uuid) (java.time.Instant/now)])
[{"uuid" #uuid "a18ee2e7-1964-45ba-82b9-0867e377ec9e"
  "timestamptz" #instant "2025-10-29T11:15:02.104857Z"}]
```

See appendix [_Supported PostgreSQL data types_](#supported-postgresql-data-types) for the data
types Muutos supports as parameters and in query results.

### Pipelining queries

Muutos supports [pipelining](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING) SQL queries.

Pipelining means sending a series of queries without waiting for earlier
ones to complete.

```clojure
user=> (eq pg
         ["CREATE TEMPORARY TABLE t (n INT NOT NULL)"]
         ["INSERT INTO t (n) VALUES (1)"]
         ["SELECT * FROM t"]
         ["DROP TABLE t"])
[[] [] [{:n 1}] []]
```

If a step in the pipeline fails, the PostgreSQL does not execute the
remaining steps.

```clojure
user=> (eq pg
         ["CREATE TEMPORARY TABLE t (n INT NOT NULL)"]
         ;; Wrong type; yields error.
         ["INSERT INTO t (n) VALUES ('a')"]
         ;; This step won't execute.
         ["DROP SCHEMA pg_catalog"])
Execution error (ExceptionInfo) at muutos.impl.decode/decode-error-response (decode.clj:420).
invalid input syntax for type integer: "a"
user=> (ex-data *e)
{:cause :invalid-text-representation
 :file "numutils.c"
 :error-code "22P02"
 :routine "pg_strtoint32_safe"
 :line 616
 :kind :muutos.error/server-error
 :severity-localized "ERROR"
 :severity "ERROR"
 :position 27}
```

The pg_catalog schema remains (not that it's possible to actually drop it, but anyway).

```clojure
user=> (eq pg ["SELECT typname, oid, typarray FROM pg_catalog.pg_type LIMIT 5"])
[{"typname" "bool" "oid" 16 "typarray" 1000}
 {"typname" "bytea" "oid" 17 "typarray" 1001}
 {"typname" "char" "oid" 18 "typarray" 1002}
 {"typname" "name" "oid" 19 "typarray" 1003}
 {"typname" "int8" "oid" 20 "typarray" 1016}]
```

### Closing the SQL client

The Muutos SQL client implements [`java.lang.AutoCloseable`](https://download.java.net/java/early_access/jdk25/docs/api/java.base/java/lang/AutoCloseable.html). When we're done using the SQL client, we can close it.

```clojure
user=> (.close pg)
nil
```

## Connecting to a PostgreSQL server using TLS encryption

Muutos can connect to a PostgreSQL instance configured to use [secure TCP/IP connections](https://www.postgresql.org/docs/current/ssl-tcp.html).

To connect to a PostgreSQL server that requires, you can either:

- Use a [X.509 trust manager](https://docs.oracle.com/en/java/javase/25/security/java-security-overview1.html#GUID-FCF419A7-B856-46DD-A36F-C6F88F9AF37F) derived from the server certificate.
- Use a [Java keystore](https://docs.oracle.com/en/java/javase/25/security/java-security-overview1.html#GUID-054AD71D-D449-47FF-B6F7-F416DA821D46) that contains the server certificate.
- Use a [X.509 trust manager](https://docs.oracle.com/en/java/javase/25/security/java-security-overview1.html#GUID-FCF419A7-B856-46DD-A36F-C6F88F9AF37F) that accepts any server certificate (**not recommended**; exposes you to a man-in-the-middle attack)

To derive a collection of X.509 trust managers from a server certificate:

1. Use `muutos.trust-manager/of-rfc-7468-resource` to derive a collection
   of trust managers.
1. Pass the trust managers as the value of the `:trust-managers` option to `muutos.sql-client/connect` or `muutos.subscriber/connect`:

```clojure
user=> (require '[muutos.trust-manager :as trust-manager])
nil
user=> (def trust-managers
         (trust-manager/of-rfc-7468-resource "/path/to/server.pem"))
#'user/trust-managers
user=> (def pg (connect :trust-managers trust-managers))
#'user/pg
```

If you're absolutely sure you know what you're doing, you can use `muutos.trust-manager/credulous-x509-trust-manager` to have Muutos accept any server certificate.

## Using Muutos as a scheduler with pg_cron

See [`003_cron.repl`](/examples/003_cron.repl) for an example on how to use pg_cron and [`pg_logical_emit_message`](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-REPLICATION) to use Muutos as a scheduler.

## Adding support for new data types

To teach Muutos how to decode a PostgreSQL data type it doesn't know about, implement the `muutos.codec.bin/decode` (or `muutos.codec.txt/decode`) multimethod.

To teach Muutos how to encode a Clojure/Java data type into a PostgreSQL data type, extend the `muutos.codec.bin/Parameter` protocol.

See [`002_jsonb.repl`](/examples/002_jsonb.repl) for an example that demonstrates both.

You can adapt the code from the REPL session to add encoders and/or decoders
for other PostgreSQL data types.

## Deploying Muutos as a Kubernetes app

See [`dev/app`](/dev/app) for an example on how to deploy Muutos as a Kubernetes app. To run the example app, you must have Kubernetes set up such that you can use the `kubectl` command, and then:

```bash
$ cd dev/app
$ clj -T:build image
...
$ kubectl apply -f k8s/postgres.yml
configmap/pg-init-scripts created
persistentvolumeclaim/postgres-pvc created
secret/postgres-secret created
deployment.apps/postgres created
service/postgres created
$ kubectl apply -f k8s/app.yml
namespace/app created
deployment.apps/app created
$ kubectl port-forward svc/postgres 5432:5432 --namespace postgres
Forwarding from 127.0.0.1:5432 -> 5432
Forwarding from [::1]:5432 -> 5432
# In another terminal:
$ kubectl logs deployment/app --follow
```

You can then connect to the PostgreSQL server listening on localhost:5432, run SQL statements, and see what appears in the app logs.

Here's a SQL statement you can try:

```sql
SELECT pg_logical_emit_message(true, 'my-prefix', convert_to('Hello, world!', 'UTF-8'));
```

## Appendices

### Supported PostgreSQL data types

Muutos supports translation between [PostgreSQL data types](https://www.postgresql.org/docs/current/datatype.html) and Java data types as described by this table.

Muutos only supports the [binary format for parameter binding](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-BIND).

In this table, "B" stands for binary, and "T" stands for text.

| OID   | Postgres       | Java                                                            | Read (B) | Write (B) | Read (T)  |
|-------|----------------|-----------------------------------------------------------------|----------|-----------|-----------|
| 16    | `bool`         | `boolean`                                                       | ✓        | ✓         | ✓         |
| 17    | `bytea`        | `bytes`                                                         | ✓        | ✓         | ✗         |
| 18    | `char`         | `char`                                                          | ✓        | ✓         | ✓         |
| 19    | `name`         | [`String`][java.lang.String]                                    | ✓        | ✓         | ✓         |
| 20    | `int8`         | `long`                                                          | ✓        | ✓         | ✓         |
| 21    | `int2`         | `short`                                                         | ✓        | ✓         | ✓         |
| 23    | `int4`         | `int`                                                           | ✓        | ✓         | ✓         |
| 24    | `regproc`      | [`String`][java.lang.String]                                    | ✓        | ✗         | ✓         |
| 25    | `text`         | [`String`][java.lang.String]                                    | ✓        | ✓         | ✓         |
| 26    | `oid`          | `int`                                                           | ✓        | ✗         | ✗         |
| 114   | `json`         | `bytes`[^1] (B), [`String`][java.lang.String][^4] (T)           | ✓        | ✓         | ✓         |
| 194   | `pg_node_tree` | [`String`][java.lang.String]                                    | ✓        | ✗         | ✓         |
| 600   | `point`        | `m.t.Point`                                                     | ✓        | ✓         | ✗         |
| 601   | `lseg`         | `m.t.LineSegment`                                               | ✓        | ✓         | ✗         |
| 602   | `path`         | `m.t.Path`                                                      | ✓        | ✓         | ✗         |
| 603   | `box`          | `m.t.Box`                                                       | ✓        | ✓         | ✗         |
| 604   | `polygon`      | `m.t.Polygon`                                                   | ✓        | ✓         | ✗         |
| 628   | `line`         | `m.t.Line`                                                      | ✓        | ✓         | ✗         |
| 700   | `float4`       | `float`                                                         | ✓        | ✓         | ✓         |
| 701   | `float8`       | `double`                                                        | ✓        | ✓         | ✓         |
| 705   | `unknown`      | `null`                                                          | ✓        | ✗         | ✓         |
| 718   | `circle`       | `m.t.Circle`                                                    | ✓        | ✓         | ✗         |
| 774   | `macaddr8`     | [`String`][java.lang.String]                                    | ✗        | ✗         | ✓         |
| 790   | [`money`][pg.money] | [`BigInteger`][java.math.BigInteger][^2] (B) / [`BigDecimal`][java.math.BigDecimal] (T) | ✓        | ✓         | ✓         |
| 829   | `macaddr`     | [`String`][java.lang.String]                                     | ✗        | ✗         | ✓         |
| 869   | `inet`         | `m.t.Inet` (B), [`InetAddress`][java.net.InetAddress] (T)       | ✓        | ✓         | ✓         |
| 1042  | `bpchar`       | [`String`][java.lang.String]                                    | ✓        | ✓         | ✓         |
| 1043  | `varchar`      | [`String`][java.lang.String]                                    | ✓        | ✓         | ✓         |
| 1082  | `date`         | [`LocalDate`][java.time.LocalDate]                              | ✓        | ✓         | ✓         |
| 1083  | `time`         | [`LocalTime`][java.time.LocalTime]                              | ✓        | ✓         | ✓         |
| 1114  | `timestamp`    | [`LocalDateTime`][java.time.LocalDateTime]                      | ✓        | ✓         | ✓         |
| 1184  | `timestamptz`  | [`Instant`][java.time.Instant] (B) / [`OffsetDateTime`][java.time.OffsetDateTime] (T)                    | ✓        | ✓         | ✓         |
| 1186  | `interval`[^3] | [`Duration`][java.time.Duration] / [`Period`][java.time.Period] | ✓        | ✓         | ✗         |
| 1266  | `timetz`       | [`OffsetTime`][java.time.OffsetTime]                            | ✓        | ✓         | ✓         |
| 1560  | `bit`          | [`String`][java.lang.String]                                    | ✗        | ✗         | ✓         |                            | ✓        | [✗]     | ✓         |
| 1562  | `varbit`       | [`String`][java.lang.String]                                    | ✗        | ✗         | ✓         |
| 1700  | `numeric`      | [`BigDecimal`][java.math.BigDecimal]                            | ✓        | ✗         | ✓         |
| 2249  | `record`       | [`IPersistentMap`][clojure.lang.IPersistentMap]                 | ✓        | ✗         | ✗         |
| 2278  | `void`         | `null`                                                          | ✓        | ✗         | ✓         |
| 2950  | `uuid`         | [`UUID`][java.util.UUID]                                        | ✓        | ✓         | ✓         |
| 3220  | `pg_lsn`       | `m.t.LogSequenceNumber`                                         | ✓        | ✓         | ✓         |
| 3614  | `tsvector`     | `m.t.Document` of `m.t.Lexeme`                                          | ✓        | ✗         | ✗         |
| 3802  | `jsonb`        | `bytes`[^1] (B), [`String`][java.lang.String][^4] (T)           | ✓        | ✓         | ✓         |
| 3904  | `int4range`    | `m.t.Range`                                                     | ✓        | ✗         | ✗         |
| 3906  | `numrange`     | `m.t.Range`                                                     | ✓        | ✗         | ✗         |
| 3908  | `tsrange`      | `m.t.Range`                                                     | ✓        | ✗         | ✗         |
| 3910  | `tstzrange`    | `m.t.Range`                                                     | ✓        | ✗         | ✗         |
| 3912  | `daterange`    | `m.t.Range`                                                     | ✓        | ✗         | ✗         |
| 3926  | `int8range`    | `m.t.Range`                                                     | ✓        | ✗         | ✗         |

> [!IMPORTANT]
> Attempting to decode a non-supported data type will throw an exception and close the client.

## Limitations

- PostgreSQL does not replicate database schema and DDL commands, so neither does Muutos.
- Muutos does not support [two-phase commits](https://www.postgresql.org/docs/current/logicaldecoding-two-phase-commits.html) (2PC).
- The Muutos SQL client does not support `LISTEN`/`NOTIFY`.

[^1]: See [Adding support for new data types](#adding-support-for-new-data-types) on how to teach Muutos to read and write `json`/`jsonb` data.
[^2]: The fractional precision of the `money` type is [locale-dependent](https://www.postgresql.org/docs/current/datatype-money.html). Converting `money` to a floating-point presentation is up to the user.
[^3]: The maximum unit of `java.time.Duration` is an hour and the minimum unit of a `java.time.Period` is a day, so intervals such as `INTERVAL '1 year 1 minute'` will lose fidelity in translation.
[^4]: By default, in text mode, when PostgreSQL sends it a JSON string, Muutos hands it to you as is. See [Adding support for new data types](#adding-support-for-new-data-types) on how to teach Muutos to read JSON data into Clojure data structures.
[^5]: Kleppmann, Martin. "Designing Data-Intensive Applications." O’Reilly Media, Inc. 2017, pp. 452–453.
[^6]: Kleppmann, pp. 454–457.

[pg.money]: https://www.postgresql.org/docs/current/datatype-money.html
[clojure.lang.IPersistentMap]: https://clojure.org/reference/data_structures#Maps
[java.lang.String]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/String.html
[java.math.BigDecimal]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/math/BigDecimal.html
[java.math.BigInteger]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/math/BigInteger.html
[java.net.InetAddress]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/net/InetAddress.html
[java.time.LocalDate]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/LocalDate.html
[java.time.LocalDateTime]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/LocalDateTime.html
[java.time.LocalTime]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/LocalTime.html
[java.time.OffsetDateTime]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/OffsetDateTime.html
[java.time.Instant]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/Instant.html
[java.time.OffsetTime]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/OffsetTime.html
[java.time.Duration]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/Duration.html
[java.time.Period]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/Period.html
[java.util.UUID]: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/UUID.html
