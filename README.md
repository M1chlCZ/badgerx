# badgerx

A typed JSON value store on top of BadgerDB.

## Features

- Store and load JSON values with Go generics.
- Store a value with a time to live.
- Increment int64 counters atomically, with overflow checks.
- Record one deduplicated counter event across several counters.
- Scan counters by key prefix.
- Back up the database to a writer while the store stays open.
- Serve an operator-only online export over a private Unix socket.
- Restore and verify native backups in isolation.
- Delete keys and close the store safely.

The subpackage `jsonx` provides deterministic, strict JSON helpers. The
subpackage `backup` provides the operator export socket and restore helpers.

## Install

```
go get github.com/m1chlcz/badgerx
```

Go 1.27 or later is required.

## Usage

```go
store, err := badgerx.OpenBadger("/data/store")
defer store.Close()

err = store.StoreValue(ctx, "user:1", profile)
profile, err := store.GetValue[Profile](ctx, "user:1")
err = store.StoreValueTTL(ctx, "session:1", token, time.Hour)
count, err := store.IncrementValue(ctx, "views:home")
counters, err := store.ScanCounters(ctx, "views:")
err = store.IncrementCountersOnce(ctx, []string{"event:abc"}, time.Now().Add(24*time.Hour), []string{"views:home"})
err = store.Backup(ctx, writer)
err = store.DeleteValue(ctx, "session:1")
```

## Backup

The `backup` subpackage serves `store.Backup` bytes over a private socket
inside the Badger directory and restores them elsewhere:

```go
server, err := backup.Listen("/data/store", backup.SocketPath("/data/store"), store.Backup)
defer server.Close()

err = backup.Export(ctx, "/data/store", backup.SocketPath("/data/store"), output)
count, err := backup.Restore("/backups/badger.backup", "/restore/store")
count, err = backup.Verify("/restore/store")
```

The `badgerverify` command wraps `Restore` and `Verify`:

```
go install github.com/m1chlcz/badgerx/cmd/badgerverify@latest
badgerverify /isolated/restore/store
badgerverify /backups/badger.backup /isolated/restore/store
```

## Notes

- The store owns one Badger database. Open one store for each path.
- A deduplicated event counts once. The store persists the first dedupe key as the event marker, and the marker expires at `expiresAt`.
- A backup keeps the store open. It blocks Close until the backup ends.
- The export socket is Unix-only and lives inside the Badger directory. It
  accepts one export at a time and an empty socket path disables it.
- Restore and Verify expect an isolated copy. Never point them at a live store.
- The package returns sentinel errors such as `badgerx.ErrBadgerKeyRequired`.

## Development

```
go test -race ./...
```

## License

MIT. See `LICENSE`.
