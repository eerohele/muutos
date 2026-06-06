# Change Log

All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## UNRELEASED

- **BREAKING**: Decode PostgreSQL records into vectors instead of maps.

  Before:

  ```clojure
  user=> (eq pg ["SELECT ROW(1, 'foo'::text) AS record"])
  [{"record" {1 "foo"}}]
  ```

  After:

  ```clojure
  user=> (eq pg ["SELECT ROW(1, 'foo'::text) AS record"])
  [{"record" [1 "foo"]}]
  ```

  Maps were the wrong choice. Records are tuples. It is possible to go
  from a vector to a map without losing anything but not vice versa.

- **BREAKING**: Fix `muutos.sql-client/eq` return value for `SET` statements

  Prior to this fix, Muutos incorrectly returned e.g. `{:type :parameter :parameter ["TimeZone" "Europe/Helsinki"]}` -- that is, the return value wrapped in the PostgreSQL wire protocol envelope. After the fix, Muutos returns `["TimeZone" "Europe/Helsinki"]`.

- Fix binary encoding of unbounded ranges (e.g. `int8range`)

- Fix logging of last flushed LSN upon subscriber close

  Prior to this fix, Muutos incorrectly logged the LSN, when it was meant to log the LSN wrapped in a map (`{:lsn lsn}`).

- Add **experimental** `muutos.sql-client/lq` function.

  `lq` stands for "latent query". Latent queries are a way execute the same statement with (potentially) different parameters with maximum performance by parsing the statement only once, then re-executing the parsed statement.

- Add **experimental** `muutos.sql-client/transact` macro.

  To execute multiple statements inside a transaction, use the `transact` macro.

- Optimize SQL client by improving buffering

- Implement encoding of `BigDecimal` to PostgreSQL `NUMERIC`

  You can now use `BigDecimal` values in SQL queries. For example:

  ```clojure
  user=> (eq pg ["SELECT $1 AS n" 1.2345M])
  [{"n" 1.2345M}]
  ```

- Fix `NUMERIC` zero decoding

  Prior to this fix, the decoding of `NUMERIC` zeroes had a bug where Muutos neglected to discard the remaining data in the buffer after determining that
  the value is zero. This bug affected at least the decoding of numeric zeroes in PostgreSQL ranges.

- Fix byte array decoding

  Same as above, but with the PostgreSQL `bytea` data type.

  To prevent future bugs like this, Muutos now has a generative test that checks that all decoders consume the entire `ByteBuffer` they're given.

- Optimize `NUMERIC` decoding

  Muutos now decodes `NUMERIC` values rougly 2-3x faster.

- Fix race condition when creating SQL client simultaneously from multiple
  threads

  Prior to this change, calling `muutos.sql-client/connect` concurrently could
  fail, because every thread used the same non-thread-safe `MessageDigest`
  instance.

- Improve error handling when flushing LSNs

  Prior to this, a non-`Exception` error (e.g. an `OutOfMemoryError`) could cause the subscriber to hang indefinitely.

- Add `:connect-timeout` and `:socket-timeout` options to both `muutos.sql-client/connect` and `muutos.subscriber/connect`.

- Fix potential hang when a `SocketException` occurred during subscriber startup

- Add connection pooling example REPL session (see `examples/004_pool.repl`).

- Add support for clear-text password authentication

  Necessary for AWS RDS IAM authentication.

## 2025-12-18
- Fix integer overflow when converting log sequence numbers to hex strings
- Omit expected and actual SCRAM-SHA-256 signatures from exception data
- Improve docstrings

## 2025-10-30
- Initial alpha release
