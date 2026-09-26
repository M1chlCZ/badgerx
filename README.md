# badgerx

A typed JSON value store on top of BadgerDB.

## Features

- Store and load JSON values with Go generics.
- Store a value with a time to live.
- Increment int64 counters atomically, with overflow checks.
- Record one deduplicated counter event across several counters.
- Scan counters by key prefix.
- Back up the database to a writer while the store stays open.
- Delete keys and close the store safely.

The subpackage `jsonx` provides deterministic, strict JSON helpers.

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

## Notes

- The store owns one Badger database. Open one store for each path.
- A deduplicated event counts once. The store persists the first dedupe key as the event marker, and the marker expires at `expiresAt`.
- A backup keeps the store open. It blocks Close until the backup ends.
- The package returns sentinel errors such as `badgerx.ErrBadgerKeyRequired`.

## Development

```
go test -race ./...
```

## License

MIT. See `LICENSE`.
