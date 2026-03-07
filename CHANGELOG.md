# Change Log

All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## UNRELEASED
- Fix race condition when creating SQL client simultaneously from multiple
  threads

  Prior to this change, calling `muutos.sql-client/connect` concurrently could
  fail, because every thread used the same non-thread-safe `MessageDigest`
  instance.

## 2025-12-18
- Fix integer overflow when converting log sequence numbers to hex strings
- Omit expected and actual SCRAM-SHA-256 signatures from exception data
- Improve docstrings

## 2025-10-30
- Initial alpha release
