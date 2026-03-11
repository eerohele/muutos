# Change Log

All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## UNRELEASED
- Fix race condition when creating SQL client simultaneously from multiple
  threads

  Prior to this change, calling `muutos.sql-client/connect` concurrently could
  fail, because every thread used the same non-thread-safe `MessageDigest`
  instance.

- Fix potential race condition in when flushing LSNs

  Prior to this change, it was possible for Muutos to miss sending one LSN to PostgreSQL. This did not cause data loss, because the situation was rectified when the next LSN came in. Or if it never did, PostgreSQL would simply have re-sent the batch that had the missed LSN. Still, the race no longer occurs.

- Improve error handling when flushing LSNs

  Prior to this, a non-`Exception` error (e.g. an `OutOfMemoryError`) could cause the subscriber to hang indefinitely.

- Add `:connect-timeout` and `:socket-timeout` options to both `muutos.sql-client/connect` and `muutos.subscriber/connect`.

- Fix potential hang when a `SocketException` occurred during subscriber startup

- Optimize SQL client by improving buffering

## 2025-12-18
- Fix integer overflow when converting log sequence numbers to hex strings
- Omit expected and actual SCRAM-SHA-256 signatures from exception data
- Improve docstrings

## 2025-10-30
- Initial alpha release
