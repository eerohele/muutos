# Table of contents
-  [`muutos.codec.bin`](#muutos.codec.bin)  - Turn Postgres binary data into Java data types and vice versa.
    -  [`Parameter`](#muutos.codec.bin/parameter) - A Clojure/Java type that can be encoded into a <code>java.nio.ByteBuffer</code> for use as a PostgreSQL query parameter.
    -  [`decode`](#muutos.codec.bin/decode) - Given a PostgreSQL data type OID (<code>int</code>) and a <code>java.nio.ByteBuffer</code>, decode the PostgreSQL value (of the given data type) in the byte buffer into a Java data type.
    -  [`decode-cstring`](#muutos.codec.bin/decode-cstring) - Given a <code>java.nio.ByteBuffer</code>, decode a null-terminated string (aka C string) from the buffer.
    -  [`encode`](#muutos.codec.bin/encode) - Encode a parameter into a <code>java.nio.ByteBuffer</code>.
-  [`muutos.codec.txt`](#muutos.codec.txt)  - Turn string representations of Postgres data types into Java data types.
    -  [`decode`](#muutos.codec.txt/decode) - Given a PostgreSQL data type OID (<code>int</code>) and a string representing that data type, parse the string into a Java data type.
-  [`muutos.sql-client`](#muutos.sql-client)  - SQL client.
    -  [`connect`](#muutos.sql-client/connect) - Connect to a PostgreSQL database.
    -  [`create-slot`](#muutos.sql-client/create-slot) - Given a client and a slot name (string), create a (pgoutput) logical replication slot with the given name.
    -  [`drop-slot`](#muutos.sql-client/drop-slot) - Given a client and a slot name (string), drop the named logical replication slot.
    -  [`emit-message`](#muutos.sql-client/emit-message) - Given a SQL client, a prefix (string), content (string or bytes), and options, emit a [logical replication message](https://www.postgresql.org/docs/current/functions-admin.html#PG-LOGICAL-EMIT-MESSAGE).
    -  [`eq`](#muutos.sql-client/eq) - Given a client and any number of query vectors, run an extended query.
    -  [`ignoring-dupes`](#muutos.sql-client/ignoring-dupes) - Execute body.
    -  [`sq`](#muutos.sql-client/sq) - Given a client and a query string, run a simple query.
-  [`muutos.subscriber`](#muutos.subscriber)  - Subscribe to a PostgreSQL logical replication stream.
    -  [`connect`](#muutos.subscriber/connect) - Given the name of a logical replication slot (ident or string) and options, subscribe to a PostgreSQL logical replication stream.
    -  [`flow-controlling-executor`](#muutos.subscriber/flow-controlling-executor) - Return a single-thread <code>java.util.concurrent.ExecutorService</code> that runs off a non-fair, bounded queue and exerts backpressure when saturated.
-  [`muutos.trust-manager`](#muutos.trust-manager)  - X.509 trust managers (to support encrypted connections).
    -  [`credulous-x509-trust-manager`](#muutos.trust-manager/credulous-x509-trust-manager) - An X.509 trust manager that trusts any certificate the server presents to it.
    -  [`of-rfc-7468-resource`](#muutos.trust-manager/of-rfc-7468-resource) - Given a <code>clojure.java.io/IOFactory</code> (e.g.

-----
# <a name="muutos.codec.bin">muutos.codec.bin</a>


Turn Postgres binary data into Java data types and vice versa.




## <a name="muutos.codec.bin/parameter">`Parameter`</a>




A Clojure/Java type that can be encoded into a `java.nio.ByteBuffer` for use
  as a PostgreSQL query parameter.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/codec/bin.clj#L392-L397">Source</a></sub></p>

## <a name="muutos.codec.bin/decode">`decode`</a>
``` clojure
(decode oid bb)
```
Function.

Given a PostgreSQL data type OID (`int`) and a `java.nio.ByteBuffer`, decode
  the PostgreSQL value (of the given data type) in the byte buffer into a Java
  data type.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/codec/bin.clj#L44-L48">Source</a></sub></p>

## <a name="muutos.codec.bin/decode-cstring">`decode-cstring`</a>
``` clojure
(decode-cstring bb)
```
Function.

Given a `java.nio.ByteBuffer`, decode a null-terminated string (aka C
  string) from the buffer.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/codec/bin.clj#L26-L38">Source</a></sub></p>

## <a name="muutos.codec.bin/encode">`encode`</a>
``` clojure
(encode this)
```
Function.

Encode a parameter into a `java.nio.ByteBuffer`.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/codec/bin.clj#L396-L397">Source</a></sub></p>

-----
# <a name="muutos.codec.txt">muutos.codec.txt</a>


Turn string representations of Postgres data types into Java data types.




## <a name="muutos.codec.txt/decode">`decode`</a>
``` clojure
(decode oid s)
```
Function.

Given a PostgreSQL data type OID (`int`) and a string representing that data
  type, parse the string into a Java data type.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/codec/txt.clj#L15-L18">Source</a></sub></p>

-----
# <a name="muutos.sql-client">muutos.sql-client</a>


SQL client.

  Suitable for diagnostics, debugging, and low-throughput use cases.




## <a name="muutos.sql-client/connect">`connect`</a>
``` clojure
(connect
 &
 {:keys [host port user password database replication log oid-fn],
  :or {oid-fn (constantly nil), log (constantly nil)},
  :as options})
```
Function.

Connect to a PostgreSQL database.

  Options:

  - `:host` (string, default: `"localhost"`)

    The host name of the PostgreSQL server to connect to.

  - `:port` (long, default: `5432`)

    Port number.

  - `:user` (string, default: `"postgres"`)

    PostgreSQL user.

  - `:password` (string, default: `"postgres"`)

    PostgreSQL user password.

  - `:database` (string, default: `"postgres"`)

    PostgreSQL database.

  - `:replication` (boolean/ident, default: `false`)

    The replication mode to use for the connection. Use `:database` to execute
    replication commands (e.g. `CREATE_REPLICATION_SLOT`).

  - `:oid-fn` (fn, default: `(constantly nil)`)

    A fn that, given `x`, must return the [PostgreSQL data type OID](https://github.com/postgres/postgres/blob/d3d0983169130a9b81e3fe48d5c2ca4931480956/src/include/catalog/pg_type.dat)
    (an integer) associated with `x`, or `nil`.

    Use this option to teach Muutos to tell PostgreSQL the data types of
    extended query parameters. For example, to teach Muutos to tell
    PostgreSQL that it should interpret persistent Clojure collections as
    `jsonb` data:

    ```clojure
    ;; 3802 is the PostgreSQL data type OID for JSONB.
    :oid-fn (fn [x] (when (instance? IPersistentCollection x) 3802))
    ```

    If `oid-fn` returns `nil`, Muutos falls back to the built-in implementation.
    If the built-in implementation does not recognize the class of `x`, Muutos
    uses OID 0 to tell PostgreSQL that the data type is unspecified.

  - `:trust-managers` (coll of `javax.net.ssl.TrustManager`, default: `nil`)

    The trust managers to use when encrypting client/server communication using TLS.

    Use `nil` to use the default implementation built into your JVM
    distribution.

    See the [`muutos.trust-manager`](#muutos.trust-manager) namespace for a set of pre-defined trust managers.

  - `:key-fn` (fn, default: `(fn [_table-oid attr-name] attr-name)`)

    A fn that, given a PostgreSQL table OID (`int`) and an attribute name
    (`string`), returns a transformed attribute name.

    The default implementation returns the attribute name as is.

    The most common use case for this function is to transform attribute
    names into keywords. For example:

        :key-fn (fn [_table-oid attr-name] (keyword attr-name))
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/sql_client.clj#L27-L146">Source</a></sub></p>

## <a name="muutos.sql-client/create-slot">`create-slot`</a>
``` clojure
(create-slot client slot-name & {:keys [temporary?], :or {temporary? false}})
```
Function.

Given a client and a slot name (string), create a (pgoutput) logical
  replication slot with the given name.

  Options:

  - `:temporary?` (boolean, default: `false`)

    If true, do not persist the slot to disk and release it when the current
    session ends.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/sql_client.clj#L389-L400">Source</a></sub></p>

## <a name="muutos.sql-client/drop-slot">`drop-slot`</a>
``` clojure
(drop-slot client slot-name)
```
Function.

Given a client and a slot name (string), drop the named logical replication
  slot.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/sql_client.clj#L405-L409">Source</a></sub></p>

## <a name="muutos.sql-client/emit-message">`emit-message`</a>
``` clojure
(emit-message client prefix content & {:keys [transactional? flush?], :or {transactional? true, flush? false}})
```
Function.

Given a SQL client, a prefix (string), content (string or bytes), and
  options, emit a [logical replication message](https://www.postgresql.org/docs/current/functions-admin.html#PG-LOGICAL-EMIT-MESSAGE).

  Options:

  - `:transactional?` (boolean, default: `true`)

    Iff true, send message as part of the current transaction.

    A non-transactional logical replication message can be useful e.g. when you
    want to create an audit log entry regardless of whether the SQL statement
    succeeds.

  - `:flush?` (boolean, default: `false`)

     Iff true, immediately flush the message into the write-ahead log.

     Has no effect if `:transactional?` is `true`.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/sql_client.clj#L414-L434">Source</a></sub></p>

## <a name="muutos.sql-client/eq">`eq`</a>
``` clojure
(eq client & qs)
```
Function.

Given a client and any number of query vectors, run an extended query.

  To run a [pipeline](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING) of queries, pass more than one query vector.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/sql_client.clj#L151-L267">Source</a></sub></p>

## <a name="muutos.sql-client/ignoring-dupes">`ignoring-dupes`</a>
``` clojure
(ignoring-dupes & body)
```
Macro.

Execute body.

  If the body throws a `clojure.lang.ExceptionInfo` that indicates a PostgreSQL
  duplicate object, ignore the exception.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/sql_client.clj#L374-L384">Source</a></sub></p>

## <a name="muutos.sql-client/sq">`sq`</a>
``` clojure
(sq client q)
(sq client q opts)
```
Function.

Given a client and a query string, run a simple query.

  Simple queries do not support parameter placeholders. They can only be used
  with trusted inputs.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/sql_client.clj#L276-L355">Source</a></sub></p>

-----
# <a name="muutos.subscriber">muutos.subscriber</a>


Subscribe to a PostgreSQL logical replication stream.




## <a name="muutos.subscriber/connect">`connect`</a>
``` clojure
(connect
 slot-name
 &
 {:as options,
  :keys [publications handler executor executor-close-fn start-lsn log ack-interval messages?],
  :or
  {publications #{},
   handler (constantly nil),
   executor-close-fn default-executor-close-fn,
   start-lsn 0,
   log (constantly nil),
   messages? true,
   ack-interval (Duration/ofSeconds 10)}})
```
Function.

Given the name of a logical replication slot (ident or string) and options,
  subscribe to a PostgreSQL logical replication stream.

  Options:

  - `:publications` (set, default: `#{}`)

    A set of [publication names](https://www.postgresql.org/docs/current/sql-createpublication.html) to subscribe to.

    To create a publication, use e.g. [`muutos.sql-client/eq`](#muutos.sql-client/eq).

  - `:handler` (fn, default: `(constantly nil)`)

    A function with two arities, a 1-arg and 2-arg arity:

        (fn ([msg] ...) ([msg ack] ...))

    The 1-arg arity receives a Clojure map that describes a PostgreSQL
    logical replication message.

    The 2-arg arity receives the same and a 0-arg fn that you must call to
    acknowledge a database transaction as having been processed.

  - `:executor` (`java.util.concurrent.ExecutorService`, default: `(flow-controlling-executor)`)

    A `java.util.concurrent.ExecutorService` that the subscriber uses to
    execute the handler function (see `:handler`).

  - `:executor-close-fn` (fn, default: see doc)

    A fn of one arg that the subscriber calls after closing, passing it the
    handler function executor (see `:executor`).

    The default implementation shuts down the executor and awaits for
    its termination for 30 seconds.

    **Note**: If you shut down the executor immediately (using `.shutdownNow`),
    it is possible that the executor shuts down before the handler function can
    inform Muutos that it has successfully processed a transaction. This results
    in PostgreSQL re-sending Muutos that transaction upon resumption.

  - `:start-lsn` (long or string, default: 0)

    The log sequence number to start replicating from. 0 means the oldest
    transaction available in the replication slot.

  - `:protocol-version` (long, default: 2)

     [pgoutput protocol version](https://www.postgresql.org/docs/current/protocol-logical-replication.html).

  - `:log` (fn, default: `(constantly nil)`)

    A logging function with this signature:

        (fn [level event data] ...)

  - `:trust-managers` (coll of `javax.net.ssl.TrustManager`, default: `nil`)

    The trust managers to use when encrypting client/server communication
    using TLS.

    Use `nil` to use the default implementation built into your JVM
    distribution.

    See the [`muutos.trust-manager`](#muutos.trust-manager) namespace for a set of pre-defined trust
    managers.

  - `:ack-interval` (`java.time.Duration`, default: `(Duration/ofSeconds 10)`)

    The interval at which to send acknowledgement messages to the PostgreSQL
    server.

    An acknowledgement message contains the last log sequence number (LSN)
    the subscriber has successfully processed. An ack message tells PostgreSQL
    that it can delete entries in its logical replication stream through the
    given LSN.

    The default value (10 seconds) is the same as the default value of the
    PostgreSQL `wal_receiver_status_interval` parameter, which specifies the
    frequency at which a standby PostgreSQL server sends status updates to the
    primary when replicating.

  - `:key-fn` (fn, default: `(fn [_table-oid attr-name] attr-name)`)

    A fn that, given a PostgreSQL table OID (integer) and an attribute name
    (string), returns a transformed attribute name.

    The default implementation returns the attribute name as is.

    The most common use case for this function is to transform attribute names
    into keywords. For example:

        :key-fn (fn [_table-oid attr-name] (keyword attr-name))

  - `:messages?` (boolean, default: `true`)

    If true, tell PostgreSQL to send messages emitted using the
    `pg_logical_emit_message` PostgreSQL function.

  - `:streaming` (boolean or `:parallel`, default: false)

    Whether to support [large transaction streaming](https://www.postgresql.org/docs/current/logicaldecoding-streaming.html).

    Using `:parallel` requires `:protocol-version` 4 or higher.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/subscriber.clj#L135-L413">Source</a></sub></p>

## <a name="muutos.subscriber/flow-controlling-executor">`flow-controlling-executor`</a>
``` clojure
(flow-controlling-executor & {:keys [work-queue], :as options})
```
Function.

Return a single-thread `java.util.concurrent.ExecutorService` that runs off
  a non-fair, bounded queue and exerts backpressure when saturated.

  Options:

  - `:work-queue` (`java.util.concurrent.BlockingQueue`, default: `(ArrayBlockingQueue. 256 false)`)

    A `java.util.concurrent.BlockingQueue` to use as the executor work queue.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/subscriber.clj#L113-L129">Source</a></sub></p>

-----
# <a name="muutos.trust-manager">muutos.trust-manager</a>


X.509 trust managers (to support encrypted connections).




## <a name="muutos.trust-manager/credulous-x509-trust-manager">`credulous-x509-trust-manager`</a>




An X.509 trust manager that trusts any certificate the server presents to it.

  **Warning**: This trust manager exposes you to man-in-the-middle (MITM) attacks.
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/trust_manager.clj#L10-L18">Source</a></sub></p>

## <a name="muutos.trust-manager/of-rfc-7468-resource">`of-rfc-7468-resource`</a>
``` clojure
(of-rfc-7468-resource io-factory)
```
Function.

Given a `clojure.java.io/IOFactory` (e.g. a `java.io.File` or
  `java.net.URI`) that points to a [X.509 certificate](https://datatracker.ietf.org/doc/html/rfc7468),
  return a collection of X.509 trust managers derived from the certificate.

  You can use this function e.g. to generate a collection of trust managers
  derived from [AWS RDS SSL certificates](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html#UsingWithRDS.SSL.CertificatesDownload).
<p><sub><a href="https://github.com/eerohele/muutos/blob/main/src/muutos/trust_manager.clj#L26-L50">Source</a></sub></p>
