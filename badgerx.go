// Package badgerx provides a typed JSON value store on top of BadgerDB.
package badgerx

import (
	"context"
	"errors"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/m1chlcz/badgerx/jsonx"

	"github.com/dgraph-io/badger/v4"
)

var (
	ErrBadgerContextRequired     = errors.New("badger context is required")
	ErrBadgerStoreUnavailable    = errors.New("badger store is unavailable")
	ErrBadgerPathRequired        = errors.New("badger path is required")
	ErrBadgerKeyRequired         = errors.New("badger key is required")
	ErrBadgerKeyDuplicate        = errors.New("badger keys must be unique")
	ErrBadgerTTLInvalid          = errors.New("badger TTL must be positive")
	ErrBadgerWriterRequired      = errors.New("badger backup writer is required")
	ErrBadgerVisitorRequired     = errors.New("badger visitor is required")
	ErrBadgerValueEncode         = errors.New("badger value encoding failed")
	ErrBadgerValueDecode         = errors.New("badger value decoding failed")
	ErrBadgerCounterInvalid      = errors.New("badger counter is not an int64")
	ErrBadgerCounterOverflow     = errors.New("badger counter overflow")
	ErrBadgerDiscardRatioInvalid = errors.New("badger discard ratio must be between 0 and 1")
)

// BadgerStore owns one Badger database instance.
type BadgerStore struct {
	db        *badger.DB
	lifecycle sync.RWMutex
	close     sync.Once
	closeErr  error
}

// OpenBadger opens a persistent Badger store at path.
func OpenBadger(path string) (*BadgerStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, ErrBadgerPathRequired
	}
	database, err := badger.Open(badger.DefaultOptions(path).WithLogger(nil))
	if err != nil {
		return nil, err
	}
	return &BadgerStore{db: database}, nil
}

// Close closes the store. Repeated calls return the result of the first close.
func (store *BadgerStore) Close() error {
	if store == nil || store.db == nil {
		return ErrBadgerStoreUnavailable
	}
	store.lifecycle.Lock()
	defer store.lifecycle.Unlock()
	store.close.Do(func() {
		store.closeErr = store.db.Close()
	})
	return store.closeErr
}

// StoreValue stores one JSON-encoded value.
func (store *BadgerStore) StoreValue[T any](ctx context.Context, key string, value T) error {
	return store.storeJSON(ctx, key, value, 0)
}

// StoreValueTTL stores one JSON-encoded value with a positive TTL.
func (store *BadgerStore) StoreValueTTL[T any](ctx context.Context, key string, value T, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrBadgerTTLInvalid
	}
	return store.storeJSON(ctx, key, value, ttl)
}

func (store *BadgerStore) storeJSON[T any](ctx context.Context, key string, value T, ttl time.Duration) error {
	if err := validateBadgerArgs(ctx, store, key); err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ErrBadgerValueEncode
	}
	if errCause := ctx.Err(); errCause != nil {
		return errCause
	}
	return store.db.Update(func(txn *badger.Txn) error {
		if errCause := ctx.Err(); errCause != nil {
			return errCause
		}
		entry := badger.NewEntry([]byte(key), encoded)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
}

// GetValue loads one JSON-encoded value.
func (store *BadgerStore) GetValue[T any](ctx context.Context, key string) (T, error) {
	var value T
	if err := validateBadgerArgs(ctx, store, key); err != nil {
		return value, err
	}
	err := store.db.View(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(encoded []byte) error {
			if errCause := ctx.Err(); errCause != nil {
				return errCause
			}
			if unmarshalErr := json.Unmarshal(encoded, &value); unmarshalErr != nil {
				return ErrBadgerValueDecode
			}
			return nil
		})
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}

// GetArray loads one JSON-encoded slice.
func (store *BadgerStore) GetArray[T any](ctx context.Context, key string) ([]T, error) {
	return store.GetValue[[]T](ctx, key)
}

// IncrementValue atomically increments an int64 counter.
func (store *BadgerStore) IncrementValue(ctx context.Context, key string) (int64, error) {
	if err := validateBadgerArgs(ctx, store, key); err != nil {
		return 0, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		var next int64
		err := store.db.Update(func(txn *badger.Txn) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			current, err := readBadgerCounter(txn, []byte(key))
			if err != nil {
				return err
			}
			if current == math.MaxInt64 {
				return ErrBadgerCounterOverflow
			}
			if errCause := ctx.Err(); errCause != nil {
				return errCause
			}
			next = current + 1
			return txn.Set([]byte(key), []byte(strconv.FormatInt(next, 10)))
		})
		if errors.Is(err, badger.ErrConflict) {
			continue
		}
		return next, err
	}
}

// IncrementCountersOnce atomically records one deduplicated counter event.
func (store *BadgerStore) IncrementCountersOnce(
	ctx context.Context,
	dedupeKeys []string,
	expiresAt time.Time,
	counterKeys []string,
) error {
	return store.IncrementCountersOnceAt(ctx, dedupeKeys, time.Now(), expiresAt, counterKeys)
}

// IncrementCountersOnceAt atomically records one deduplicated counter event using now for TTL validation.
func (store *BadgerStore) IncrementCountersOnceAt(
	ctx context.Context,
	dedupeKeys []string,
	now time.Time,
	expiresAt time.Time,
	counterKeys []string,
) error {
	if ctx == nil {
		return ErrBadgerContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.db == nil {
		return ErrBadgerStoreUnavailable
	}
	expiresUnix, err := validateBadgerCounterEvent(dedupeKeys, counterKeys, now, expiresAt)
	if err != nil {
		return err
	}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		err = store.db.Update(func(txn *badger.Txn) error {
			return incrementBadgerCountersTxn(ctx, txn, dedupeKeys, counterKeys, expiresUnix)
		})
		if errors.Is(err, badger.ErrConflict) {
			continue
		}
		return err
	}
}

func validateBadgerCounterEvent(
	dedupeKeys, counterKeys []string,
	now, expiresAt time.Time,
) (uint64, error) {
	if err := validateUniqueBadgerKeys(dedupeKeys); err != nil {
		return 0, err
	}
	if err := validateUniqueBadgerKeys(counterKeys); err != nil {
		return 0, err
	}
	for _, dedupeKey := range dedupeKeys {
		if slices.Contains(counterKeys, dedupeKey) {
			return 0, ErrBadgerKeyDuplicate
		}
	}
	expiresUnix := expiresAt.Unix()
	if expiresUnix < 0 {
		return 0, ErrBadgerTTLInvalid
	}
	if expiresUnix <= now.Unix() {
		return 0, ErrBadgerTTLInvalid
	}
	return uint64(expiresUnix), nil
}

func incrementBadgerCountersTxn(
	ctx context.Context,
	txn *badger.Txn,
	dedupeKeys, counterKeys []string,
	expiresUnix uint64,
) error {
	seen, err := badgerDedupeSeen(ctx, txn, dedupeKeys)
	if err != nil || seen {
		return err
	}
	nextCounters, err := nextBadgerCounters(txn, counterKeys)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	entry := badger.NewEntry([]byte(dedupeKeys[0]), []byte{1})
	entry.ExpiresAt = expiresUnix
	if err = txn.SetEntry(entry); err != nil {
		return err
	}
	for index, key := range counterKeys {
		if err = txn.Set([]byte(key), []byte(strconv.FormatInt(nextCounters[index], 10))); err != nil {
			return err
		}
	}
	return nil
}

func badgerDedupeSeen(ctx context.Context, txn *badger.Txn, dedupeKeys []string) (bool, error) {
	seen := false
	for _, key := range dedupeKeys {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		_, err := txn.Get([]byte(key))
		switch {
		case err == nil:
			seen = true
		case errors.Is(err, badger.ErrKeyNotFound):
		default:
			return false, err
		}
	}
	return seen, nil
}

func nextBadgerCounters(txn *badger.Txn, counterKeys []string) ([]int64, error) {
	next := make([]int64, len(counterKeys))
	for index, key := range counterKeys {
		current, err := readBadgerCounter(txn, []byte(key))
		if err != nil {
			return nil, err
		}
		if current < 0 {
			return nil, ErrBadgerCounterInvalid
		}
		if current == math.MaxInt64 {
			return nil, ErrBadgerCounterOverflow
		}
		next[index] = current + 1
	}
	return next, nil
}

func validateUniqueBadgerKeys(keys []string) error {
	if len(keys) == 0 {
		return ErrBadgerKeyRequired
	}
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			return ErrBadgerKeyRequired
		}
		if _, exists := seen[key]; exists {
			return ErrBadgerKeyDuplicate
		}
		seen[key] = struct{}{}
	}
	return nil
}

func readBadgerCounter(txn *badger.Txn, key []byte) (int64, error) {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	encoded, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}
	return parseBadgerCounter(encoded)
}

func parseBadgerCounter(encoded []byte) (int64, error) {
	value, err := strconv.ParseInt(string(encoded), 10, 64)
	if err != nil {
		return 0, ErrBadgerCounterInvalid
	}
	return value, nil
}

// ScanCounters loads nonnegative counters whose keys start with prefix.
func (store *BadgerStore) ScanCounters(ctx context.Context, prefix string) (map[string]int64, error) {
	counters := make(map[string]int64)
	if err := validateBadgerArgs(ctx, store, prefix); err != nil {
		return counters, err
	}
	err := store.db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = false
		iterator := txn.NewIterator(options)
		defer iterator.Close()

		prefixBytes := []byte(prefix)
		for iterator.Seek(prefixBytes); iterator.ValidForPrefix(prefixBytes); iterator.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			item := iterator.Item()
			key := item.KeyCopy(nil)
			encoded, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if errCause := ctx.Err(); errCause != nil {
				return errCause
			}
			value, err := parseBadgerCounter(encoded)
			if err != nil {
				return err
			}
			if value < 0 {
				return ErrBadgerCounterInvalid
			}
			counters[string(key)] = value
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return counters, nil
}

const (
	defaultBadgerBatch = 256
	maxBadgerBatch     = 4096
)

// IterateOptions configures Iterate.
type IterateOptions struct {
	// Prefix limits iteration to keys that start with Prefix.
	Prefix string
	// Batch is the maximum number of items passed to one visit call. Values
	// above 4096 are capped at 4096; zero and negative values use 256.
	Batch int
}

// IterateItem is one raw key-value pair delivered by Iterate.
type IterateItem struct {
	Key   string
	Value json.RawMessage
}

// Iterate visits raw key-value pairs in lexicographic key order, one visit per
// batch of at most options.Batch items, for keys that start with
// options.Prefix. A batch defaults to 256 items and never exceeds 4096. Each
// batch runs in one read-only transaction, so a long scan does not pin one
// transaction; writes committed between batches may appear in later batches.
// Iteration resumes after the last delivered key. Visit errors stop iteration
// and are returned unchanged.
func (store *BadgerStore) Iterate(
	ctx context.Context,
	options IterateOptions,
	visit func(items []IterateItem) error,
) error {
	if ctx == nil {
		return ErrBadgerContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.db == nil {
		return ErrBadgerStoreUnavailable
	}
	if visit == nil {
		return ErrBadgerVisitorRequired
	}
	if store.db.IsClosed() {
		return ErrBadgerStoreUnavailable
	}
	batch := options.Batch
	if batch <= 0 {
		batch = defaultBadgerBatch
	}
	if batch > maxBadgerBatch {
		batch = maxBadgerBatch
	}
	prefix := []byte(options.Prefix)
	seek := prefix
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		items := make([]IterateItem, 0, batch)
		var lastKey []byte
		err := store.db.View(func(txn *badger.Txn) error {
			iteratorOptions := badger.DefaultIteratorOptions
			iteratorOptions.PrefetchValues = false
			iterator := txn.NewIterator(iteratorOptions)
			defer iterator.Close()
			for iterator.Seek(seek); iterator.ValidForPrefix(prefix); iterator.Next() {
				if err := ctx.Err(); err != nil {
					return err
				}
				item := iterator.Item()
				key := item.KeyCopy(nil)
				encoded, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				if errCause := ctx.Err(); errCause != nil {
					return errCause
				}
				items = append(items, IterateItem{Key: string(key), Value: encoded})
				lastKey = key
				if len(items) == batch {
					return nil
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		if err := visit(items); err != nil {
			return err
		}
		if len(items) < batch {
			return nil
		}
		seek = append(lastKey, 0)
	}
}

// Backup writes a native full backup and does not close writer.
func (store *BadgerStore) Backup(ctx context.Context, writer io.Writer) error {
	if ctx == nil {
		return ErrBadgerContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.db == nil {
		return ErrBadgerStoreUnavailable
	}
	if writer == nil {
		return ErrBadgerWriterRequired
	}
	store.lifecycle.RLock()
	defer store.lifecycle.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if store.db.IsClosed() {
		return badger.ErrDBClosed
	}
	contextualWriter := &contextWriter{ctx: ctx, writer: writer}
	_, backupErr := store.db.Backup(contextualWriter, 0)
	if contextualWriter.err != nil {
		return contextualWriter.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return backupErr
}

type contextWriter struct {
	ctx    context.Context
	writer io.Writer
	err    error
}

func (writer *contextWriter) Write(value []byte) (int, error) {
	if err := writer.ctx.Err(); err != nil {
		writer.err = err
		return 0, err
	}
	written, err := writer.writer.Write(value)
	if err != nil {
		writer.err = err
		return written, err
	}
	if errCause := writer.ctx.Err(); errCause != nil {
		writer.err = errCause
		return written, errCause
	}
	if written != len(value) {
		writer.err = io.ErrShortWrite
		return written, io.ErrShortWrite
	}
	return written, nil
}

// RunValueLogGC runs at most one value log garbage collection pass with the
// given discard ratio, which must be between 0 and 1. It reports whether a
// rewrite ran; false with a nil error means Badger found nothing to rewrite.
// Callers loop until it returns false.
func (store *BadgerStore) RunValueLogGC(ctx context.Context, discardRatio float64) (bool, error) {
	if ctx == nil {
		return false, ErrBadgerContextRequired
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if store == nil || store.db == nil {
		return false, ErrBadgerStoreUnavailable
	}
	if discardRatio <= 0 || discardRatio >= 1 {
		return false, ErrBadgerDiscardRatioInvalid
	}
	store.lifecycle.RLock()
	defer store.lifecycle.RUnlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if store.db.IsClosed() {
		return false, ErrBadgerStoreUnavailable
	}
	rewriteErr := store.db.RunValueLogGC(discardRatio)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	switch {
	case rewriteErr == nil:
		return true, nil
	case errors.Is(rewriteErr, badger.ErrNoRewrite):
		return false, nil
	default:
		return false, rewriteErr
	}
}

// DeleteValue deletes one value. Deleting a missing key succeeds.
func (store *BadgerStore) DeleteValue(ctx context.Context, key string) error {
	if err := validateBadgerArgs(ctx, store, key); err != nil {
		return err
	}
	return store.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return txn.Delete([]byte(key))
	})
}

func validateBadgerArgs(ctx context.Context, store *BadgerStore, key string) error {
	if ctx == nil {
		return ErrBadgerContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.db == nil {
		return ErrBadgerStoreUnavailable
	}
	if strings.TrimSpace(key) == "" {
		return ErrBadgerKeyRequired
	}
	return nil
}
