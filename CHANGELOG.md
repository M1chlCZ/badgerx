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
