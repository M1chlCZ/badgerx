# Changelog

All notable changes to this package are documented in this file. The format
follows Keep a Changelog, and the project uses semantic versioning.

## [Unreleased]

### Added

- The `backup` subpackage: an operator-only online export over a private Unix
  socket (`Listen`, `SocketPath`, `Export`), including the store backup as an
  `ExportFunc`. The server accepts one export at a time, enforces an owner-only
  socket inside the Badger directory and validates the export checksum.
- Native restore and verification helpers (`Restore`, `Verify`). Malformed
  exports fail with an error instead of panicking.
- The `badgerverify` command for isolated verification and restore of native
  backups.
- A bounded batch iterator for raw key-value scans by prefix (`Iterate`,
  `IterateOptions`, `IterateItem`). Each batch runs in its own read-only
  transaction, the batch size defaults to 256 and is capped at 4096, and a
  missing visitor fails with `ErrBadgerVisitorRequired`.
- A value log garbage collection trigger (`RunValueLogGC`) that runs at most
  one pass and reports whether a rewrite ran. Invalid discard ratios fail with
  `ErrBadgerDiscardRatioInvalid`.
