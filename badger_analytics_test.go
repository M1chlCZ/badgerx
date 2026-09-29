//nolint:testpackage // White-box tests exercise the unexported BadgerStore.db field.
package badgerx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestIncrementBadgerCountersOnceIncrementsEveryCounter(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)

	err := store.IncrementCountersOnce(
		context.Background(),
		[]string{"dedupe/today/view"},
		time.Now().Add(time.Hour),
		[]string{"day/today/product", "total/product"})
	if err != nil {
		t.Fatalf("increment counters once: %v", err)
	}

	for _, key := range []string{"day/today/product", "total/product"} {
		got, getBadgerValueErr := store.GetValue[int64](context.Background(), key)
		if getBadgerValueErr != nil {
			t.Fatalf("get %q: %v", key, getBadgerValueErr)
		}
		if got != 1 {
			t.Fatalf("%q = %d, want 1", key, got)
		}
	}
}

func TestIncrementBadgerCountersOnceAtUsesExplicitClock(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	now := time.Unix(1_000_000, 0).UTC()
	expiresAt := now.Add(time.Hour)

	if err := store.IncrementCountersOnce(
		context.Background(),
		[]string{"dedupe/legacy-clock"},
		expiresAt,
		[]string{"total/legacy-clock"}); !errors.Is(err, ErrBadgerTTLInvalid) {
		t.Fatalf("real-clock increment error = %v, want ErrBadgerTTLInvalid", err)
	}
	if err := store.IncrementCountersOnceAt(
		context.Background(),
		[]string{"dedupe/explicit-clock"},
		now,
		expiresAt,
		[]string{"total/explicit-clock"}); err != nil {
		t.Fatalf("explicit-clock increment: %v", err)
	}

	got, err := store.GetValue[int64](context.Background(), "total/explicit-clock")
	if err != nil {
		t.Fatalf("get explicit-clock counter: %v", err)
	}
	if got != 1 {
		t.Fatalf("explicit-clock counter = %d, want 1", got)
	}
}

func TestIncrementBadgerCountersOnceStoresOnlyPrimaryDedupeMarker(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	expiresAt := time.Now().Add(2 * time.Hour).Truncate(time.Second)
	dedupeKeys := []string{"dedupe/today/view", "dedupe/yesterday/view"}

	if err := store.IncrementCountersOnce(
		context.Background(),
		dedupeKeys,
		expiresAt,
		[]string{"total/product"}); err != nil {
		t.Fatalf("increment counters once: %v", err)
	}

	if err := store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(dedupeKeys[0]))
		if err != nil {
			return err
		}
		if got, want := item.ExpiresAt(), uint64(expiresAt.Unix()); got != want {
			t.Fatalf("marker expires at %d, want %d", got, want)
		}
		return nil
	}); err != nil {
		t.Fatalf("read primary marker: %v", err)
	}
	if err := store.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(dedupeKeys[1]))
		return err
	}); !errors.Is(err, badger.ErrKeyNotFound) {
		t.Fatalf("secondary marker error = %v, want badger.ErrKeyNotFound", err)
	}
}

func TestIncrementBadgerCountersOnceSkipsPreviouslySeenDedupeKey(t *testing.T) {
	t.Parallel()

	for _, seenIndex := range []int{0, 1} {
		t.Run([]string{"current", "previous"}[seenIndex], func(t *testing.T) {
			t.Parallel()
			store := openTestBadger(t)
			dedupeKeys := []string{"dedupe/today/view", "dedupe/yesterday/view"}
			counterKey := "total/product"

			if seenIndex == 0 {
				seedCurrentDedupeMarker(t, store, dedupeKeys, counterKey)
			} else {
				seedPreviousDedupeMarker(t, store, dedupeKeys[seenIndex])
			}

			if err := store.IncrementCountersOnce(
				context.Background(),
				dedupeKeys,
				time.Now().Add(time.Hour),
				[]string{counterKey}); err != nil {
				t.Fatalf("duplicate increment: %v", err)
			}

			if seenIndex == 0 {
				assertDedupeCounterIncremented(t, store, counterKey)
				return
			}
			assertDedupeCounterSkipped(t, store, counterKey, dedupeKeys[0])
		})
	}
}

func seedCurrentDedupeMarker(t *testing.T, store *BadgerStore, dedupeKeys []string, counterKey string) {
	t.Helper()
	if err := store.IncrementCountersOnce(
		context.Background(),
		dedupeKeys,
		time.Now().Add(time.Hour),
		[]string{counterKey}); err != nil {
		t.Fatalf("first increment: %v", err)
	}
}

func seedPreviousDedupeMarker(t *testing.T, store *BadgerStore, key string) {
	t.Helper()
	if err := store.StoreValueTTL(
		context.Background(),
		key,
		true,
		time.Hour); err != nil {
		t.Fatalf("seed previous marker: %v", err)
	}
}

func assertDedupeCounterIncremented(t *testing.T, store *BadgerStore, counterKey string) {
	t.Helper()
	got, err := store.GetValue[int64](context.Background(), counterKey)
	if err != nil {
		t.Fatalf("get counter: %v", err)
	}
	if got != 1 {
		t.Fatalf("counter = %d, want 1", got)
	}
}

func assertDedupeCounterSkipped(t *testing.T, store *BadgerStore, counterKey, primaryDedupeKey string) {
	t.Helper()
	if _, err := store.GetValue[int64](
		context.Background(),
		counterKey); !errors.Is(
		err,
		badger.ErrKeyNotFound,
	) {
		t.Fatalf("counter error = %v, want badger.ErrKeyNotFound", err)
	}
	if err := store.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(primaryDedupeKey))
		return err
	}); !errors.Is(err, badger.ErrKeyNotFound) {
		t.Fatalf("primary marker error = %v, want badger.ErrKeyNotFound", err)
	}
}

func TestIncrementBadgerCountersOnceRollsBackInvalidCounters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   any
		wantErr error
	}{
		{name: "invalid", value: "not-a-counter", wantErr: ErrBadgerCounterInvalid},
		{name: "negative", value: int64(-1), wantErr: ErrBadgerCounterInvalid},
		{name: "overflow", value: int64(math.MaxInt64), wantErr: ErrBadgerCounterOverflow},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := openTestBadger(t)
			const (
				markerKey = "dedupe/today/view"
				validKey  = "day/today/product"
				badKey    = "total/product"
			)
			if err := store.StoreValue(context.Background(), validKey, int64(10)); err != nil {
				t.Fatalf("seed valid counter: %v", err)
			}
			if err := store.StoreValue(context.Background(), badKey, test.value); err != nil {
				t.Fatalf("seed bad counter: %v", err)
			}

			err := store.IncrementCountersOnce(
				context.Background(),
				[]string{markerKey},
				time.Now().Add(time.Hour),
				[]string{validKey, badKey})

			if !errors.Is(err, test.wantErr) {
				t.Fatalf("increment error = %v, want %v", err, test.wantErr)
			}

			got, err := store.GetValue[int64](context.Background(), validKey)
			if err != nil {
				t.Fatalf("get valid counter: %v", err)
			}
			if got != 10 {
				t.Fatalf("valid counter = %d, want unchanged value 10", got)
			}
			if viewErr := store.db.View(func(txn *badger.Txn) error {
				_, getErr := txn.Get([]byte(markerKey))
				return getErr
			}); !errors.Is(viewErr, badger.ErrKeyNotFound) {
				t.Fatalf("marker error = %v, want badger.ErrKeyNotFound", viewErr)
			}
		})
	}
}

func TestIncrementBadgerCountersOnceRejectsDuplicateKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		dedupeKeys  []string
		counterKeys []string
	}{
		{
			name:        "dedupe",
			dedupeKeys:  []string{"dedupe/view", "dedupe/view"},
			counterKeys: []string{"total/product"},
		},
		{
			name:        "counter",
			dedupeKeys:  []string{"dedupe/view"},
			counterKeys: []string{"total/product", "total/product"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := openTestBadger(t)
			err := store.IncrementCountersOnce(
				context.Background(),
				test.dedupeKeys,
				time.Now().Add(time.Hour),
				test.counterKeys)

			if err == nil {
				t.Fatal("increment error = nil, want duplicate-key rejection")
			}
		})
	}
}

func TestIncrementBadgerCountersOnceRejectsOverlappingKeys(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	keys := []string{"dedupe/shared", "dedupe/previous", "day/product"}

	err := store.IncrementCountersOnce(
		context.Background(),
		keys[:2],
		time.Now().Add(time.Hour),
		[]string{keys[2], keys[0]})

	if !errors.Is(err, ErrBadgerKeyDuplicate) {
		t.Fatalf("increment error = %v, want ErrBadgerKeyDuplicate", err)
	}
	for _, key := range keys {
		if _, getBadgerValueErr := store.GetValue[int64](
			context.Background(),
			key); !errors.Is(
			getBadgerValueErr,
			badger.ErrKeyNotFound,
		) {
			t.Fatalf("%q error = %v, want badger.ErrKeyNotFound", key, getBadgerValueErr)
		}
	}
}

func TestIncrementBadgerCountersOnceRejectsBlankKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		dedupeKeys  []string
		counterKeys []string
	}{
		{
			name:        "dedupe",
			dedupeKeys:  []string{" \t"},
			counterKeys: []string{"total/product"},
		},
		{
			name:        "counter",
			dedupeKeys:  []string{"dedupe/view"},
			counterKeys: []string{""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := openTestBadger(t)
			err := store.IncrementCountersOnce(
				context.Background(),
				test.dedupeKeys,
				time.Now().Add(time.Hour),
				test.counterKeys)

			if !errors.Is(err, ErrBadgerKeyRequired) {
				t.Fatalf("increment error = %v, want ErrBadgerKeyRequired", err)
			}
		})
	}
}

func TestIncrementBadgerCountersOnceRequiresBothKeySets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		dedupeKeys  []string
		counterKeys []string
	}{
		{name: "dedupe", counterKeys: []string{"total/product"}},
		{name: "counter", dedupeKeys: []string{"dedupe/view"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := openTestBadger(t)
			err := store.IncrementCountersOnce(
				context.Background(),
				test.dedupeKeys,
				time.Now().Add(time.Hour),
				test.counterKeys)

			if !errors.Is(err, ErrBadgerKeyRequired) {
				t.Fatalf("increment error = %v, want ErrBadgerKeyRequired", err)
			}
		})
	}
}

func TestIncrementBadgerCountersOnceRequiresFutureExpiry(t *testing.T) {
	t.Parallel()

	now := time.Now()
	tests := []struct {
		name      string
		now       time.Time
		expiresAt time.Time
	}{
		{name: "zero", now: now, expiresAt: time.Time{}},
		{name: "past", now: now, expiresAt: now.Add(-time.Minute)},
		{name: "current second", now: now, expiresAt: time.Unix(now.Unix(), 0)},
		{name: "negative Unix future", now: time.Unix(-2, 0), expiresAt: time.Unix(-1, 0)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := openTestBadger(t)
			err := store.IncrementCountersOnceAt(
				context.Background(),
				[]string{"dedupe/view"},
				test.now,
				test.expiresAt,
				[]string{"total/product"})

			if !errors.Is(err, ErrBadgerTTLInvalid) {
				t.Fatalf("increment error = %v, want ErrBadgerTTLInvalid", err)
			}
			if _, getBadgerValueErr := store.GetValue[int64](
				context.Background(),
				"total/product"); !errors.Is(
				getBadgerValueErr,
				badger.ErrKeyNotFound,
			) {
				t.Fatalf("counter error = %v, want badger.ErrKeyNotFound", getBadgerValueErr)
			}
		})
	}
}

func TestIncrementBadgerCountersOnceRequiresContext(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)

	err := store.IncrementCountersOnce(
		nil, //nolint:staticcheck // The nil context asserts the ErrBadgerContextRequired guard.
		[]string{"dedupe/view"},
		time.Now().Add(time.Hour),
		[]string{"total/product"})

	if !errors.Is(err, ErrBadgerContextRequired) {
		t.Fatalf("increment error = %v, want ErrBadgerContextRequired", err)
	}
}

func TestIncrementBadgerCountersOnceRequiresStore(t *testing.T) {
	t.Parallel()

	for _, store := range []*BadgerStore{nil, {}} {
		err := store.IncrementCountersOnce(
			context.Background(),
			[]string{"dedupe/view"},
			time.Now().Add(time.Hour),
			[]string{"total/product"})

		if !errors.Is(err, ErrBadgerStoreUnavailable) {
			t.Fatalf("increment error = %v, want ErrBadgerStoreUnavailable", err)
		}
	}
}

func TestIncrementBadgerCountersOnceHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.IncrementCountersOnce(
		ctx,

		[]string{"dedupe/view"},
		time.Now().Add(time.Hour),
		[]string{"total/product"})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("increment error = %v, want context.Canceled", err)
	}
	if _, getBadgerValueErr := store.GetValue[int64](
		context.Background(),
		"total/product"); !errors.Is(
		getBadgerValueErr,
		badger.ErrKeyNotFound,
	) {
		t.Fatalf("counter error = %v, want badger.ErrKeyNotFound", getBadgerValueErr)
	}
}

func TestIncrementBadgerCountersOnceDoesNotCommitAfterCancellation(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	ctx := &cancelAfterChecksContext{Context: context.Background(), cancelAt: 4}

	err := store.IncrementCountersOnce(
		ctx,

		[]string{"dedupe/view"},
		time.Now().Add(time.Hour),
		[]string{"total/product"})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("increment error = %v, want context.Canceled", err)
	}
	for _, key := range []string{"dedupe/view", "total/product"} {
		if _, getBadgerValueErr := store.GetValue[int64](
			context.Background(),
			key); !errors.Is(
			getBadgerValueErr,
			badger.ErrKeyNotFound,
		) {
			t.Fatalf("%q error = %v, want badger.ErrKeyNotFound", key, getBadgerValueErr)
		}
	}
}

func TestIncrementBadgerCountersOnceRejectsClosedStore(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	err := store.IncrementCountersOnce(
		context.Background(),
		[]string{"dedupe/view"},
		time.Now().Add(time.Hour),
		[]string{"total/product"})

	if !errors.Is(err, badger.ErrDBClosed) {
		t.Fatalf("increment error = %v, want badger.ErrDBClosed", err)
	}
}

func TestIncrementBadgerCountersOnceIsAtomicUnderContention(t *testing.T) {
	t.Parallel()

	const workers = 32

	t.Run("same view", func(t *testing.T) {
		t.Parallel()
		store := openTestBadger(t)
		runConcurrentCounterEvents(t, store, workers, func(int) []string {
			return []string{"dedupe/today/shared-view", "dedupe/yesterday/shared-view"}
		})
		assertBadgerCounters(t, store, 1)
	})

	t.Run("different views", func(t *testing.T) {
		t.Parallel()
		store := openTestBadger(t)
		runConcurrentCounterEvents(t, store, workers, func(worker int) []string {
			return []string{fmt.Sprintf("dedupe/today/view-%d", worker)}
		})
		assertBadgerCounters(t, store, workers)
	})
}

func runConcurrentCounterEvents(
	t *testing.T,
	store *BadgerStore,
	workers int,
	dedupeKeys func(int) []string,
) {
	t.Helper()
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for worker := range workers {
		wait.Go(func() {
			if err := store.IncrementCountersOnce(
				context.Background(),
				dedupeKeys(worker),
				time.Now().Add(time.Hour),
				[]string{"day/today/product", "total/product"}); err != nil {
				errs <- err
			}
		})
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("increment under contention: %v", err)
	}
}

func assertBadgerCounters(t *testing.T, store *BadgerStore, want int) {
	t.Helper()
	for _, key := range []string{"day/today/product", "total/product"} {
		got, err := store.GetValue[int64](context.Background(), key)
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		if got != int64(want) {
			t.Fatalf("%q = %d, want %d", key, got, want)
		}
	}
}

func TestScanBadgerCountersReturnsOnlyExactPrefix(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	for key, value := range map[string]int64{
		"day/20260723/product-a": 2,
		"day/20260723/product-b": 0,
		"day/20260722/product-c": 7,
		"total/product-a":        9,
	} {
		if err := store.StoreValue(context.Background(), key, value); err != nil {
			t.Fatalf("seed %q: %v", key, err)
		}
	}

	got, err := store.ScanCounters(context.Background(), "day/20260723/")
	if err != nil {
		t.Fatalf("scan counters: %v", err)
	}
	want := map[string]int64{
		"day/20260723/product-a": 2,
		"day/20260723/product-b": 0,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("counters = %#v, want %#v", got, want)
	}
}

func TestScanBadgerCountersChecksContextDuringIteration(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	for index := range 4 {
		key := fmt.Sprintf("day/20260723/product-%d", index)
		if err := store.StoreValue(context.Background(), key, int64(index)); err != nil {
			t.Fatalf("seed %q: %v", key, err)
		}
	}
	ctx := &cancelAfterChecksContext{Context: context.Background(), cancelAt: 3}

	if _, err := store.ScanCounters(ctx, "day/20260723/"); !errors.Is(err, context.Canceled) {
		t.Fatalf("scan error = %v, want context.Canceled", err)
	}
}

type cancelAfterChecksContext struct {
	context.Context

	checks   atomic.Int64
	cancelAt int64
}

func (ctx *cancelAfterChecksContext) Err() error {
	if ctx.checks.Add(1) >= ctx.cancelAt {
		return context.Canceled
	}
	return nil
}

func TestScanBadgerCountersRejectsInvalidAndNegativeValues(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "invalid", value: "not-a-counter"},
		{name: "negative", value: int64(-1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := openTestBadger(t)
			if err := store.StoreValue(context.Background(), "day/20260723/product", test.value); err != nil {
				t.Fatalf("seed counter: %v", err)
			}

			if _, err := store.ScanCounters(
				context.Background(),
				"day/20260723/"); !errors.Is(
				err,
				ErrBadgerCounterInvalid,
			) {
				t.Fatalf("scan error = %v, want ErrBadgerCounterInvalid", err)
			}
		})
	}
}

func TestScanBadgerCountersReturnsNonNilEmptyMap(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)

	got, err := store.ScanCounters(context.Background(), "missing/")
	if err != nil {
		t.Fatalf("scan counters: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("counters = %#v, want non-nil empty map", got)
	}
}

func TestScanBadgerCountersRejectsInvalidArgumentsAndClosedStore(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	if got, err := store.ScanCounters(
		context.Background(),
		""); !errors.Is(err, ErrBadgerKeyRequired) ||
		got == nil {
		t.Fatalf("empty prefix result = %#v, error = %v; want non-nil map and ErrBadgerKeyRequired", got, err)
	}
	if _, err := store.ScanCounters(
		nil, //nolint:staticcheck // The nil context asserts the ErrBadgerContextRequired guard.
		"day/"); !errors.Is(
		err,
		ErrBadgerContextRequired,
	) {
		t.Fatalf("nil context error = %v, want ErrBadgerContextRequired", err)
	}
	if _, err := (*BadgerStore)(nil).ScanCounters(
		context.Background(), "day/",
	); !errors.Is(err, ErrBadgerStoreUnavailable) {
		t.Fatalf("nil store error = %v, want ErrBadgerStoreUnavailable", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.ScanCounters(ctx, "day/"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if _, err := store.ScanCounters(context.Background(), "day/"); !errors.Is(err, badger.ErrDBClosed) {
		t.Fatalf("closed store error = %v, want badger.ErrDBClosed", err)
	}
}

func TestBackupBadgerRestoresIntoFreshStore(t *testing.T) {
	t.Parallel()

	source := openTestBadger(t)
	if err := source.StoreValue(context.Background(), "total/product", int64(7)); err != nil {
		t.Fatalf("seed counter: %v", err)
	}
	if err := source.StoreValue(context.Background(), "meta/schema", "v1"); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	var backup bytes.Buffer
	if err := source.Backup(context.Background(), &backup); err != nil {
		t.Fatalf("backup: %v", err)
	}

	restored := openTestBadger(t)
	if err := restored.db.Load(bytes.NewReader(backup.Bytes()), 16); err != nil {
		t.Fatalf("restore native Badger backup: %v", err)
	}
	counter, err := restored.GetValue[int64](context.Background(), "total/product")
	if err != nil {
		t.Fatalf("get restored counter: %v", err)
	}
	if counter != 7 {
		t.Fatalf("restored counter = %d, want 7", counter)
	}
	schema, err := restored.GetValue[string](context.Background(), "meta/schema")
	if err != nil {
		t.Fatalf("get restored metadata: %v", err)
	}
	if schema != "v1" {
		t.Fatalf("restored metadata = %q, want v1", schema)
	}
}

func TestBackupBadgerHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	source := openTestBadger(t)
	if err := source.StoreValue(context.Background(), "total/product", int64(7)); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	t.Run("before backup", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := source.Backup(ctx, &bytes.Buffer{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("backup error = %v, want context.Canceled", err)
		}
	})

	t.Run("during write", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		writer := &cancelingWriter{cancel: cancel}

		if err := source.Backup(ctx, writer); !errors.Is(err, context.Canceled) {
			t.Fatalf("backup error = %v, want context.Canceled", err)
		}
	})
}

type cancelingWriter struct {
	bytes.Buffer

	cancel context.CancelFunc
}

func (writer *cancelingWriter) Write(value []byte) (int, error) {
	written, err := writer.Buffer.Write(value)
	writer.cancel()
	return written, err
}

func TestBackupBadgerRequiresContext(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)

	if err := store.Backup(
		nil, //nolint:staticcheck // The nil context asserts the ErrBadgerContextRequired guard.
		&bytes.Buffer{}); !errors.Is(
		err,
		ErrBadgerContextRequired,
	) {
		t.Fatalf("backup error = %v, want ErrBadgerContextRequired", err)
	}
}

func TestBackupBadgerRequiresStore(t *testing.T) {
	t.Parallel()

	for _, store := range []*BadgerStore{nil, {}} {
		if err := store.Backup(
			context.Background(),
			&bytes.Buffer{}); !errors.Is(
			err,
			ErrBadgerStoreUnavailable,
		) {
			t.Fatalf("backup error = %v, want ErrBadgerStoreUnavailable", err)
		}
	}
}

func TestBackupBadgerRequiresWriter(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)

	if err := store.Backup(context.Background(), nil); !errors.Is(err, ErrBadgerWriterRequired) {
		t.Fatalf("backup error = %v, want ErrBadgerWriterRequired", err)
	}
}

func TestBackupBadgerPropagatesWriterFailures(t *testing.T) {
	t.Parallel()

	source := openTestBadger(t)
	if err := source.StoreValue(context.Background(), "total/product", int64(7)); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	t.Run("writer error", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("backup writer failed")
		if err := source.Backup(context.Background(), errorWriter{err: wantErr}); !errors.Is(err, wantErr) {
			t.Fatalf("backup error = %v, want writer error", err)
		}
	})

	t.Run("short write", func(t *testing.T) {
		t.Parallel()
		if err := source.Backup(context.Background(), shortWriter{}); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("backup error = %v, want io.ErrShortWrite", err)
		}
	})
}

type errorWriter struct {
	err error
}

func (writer errorWriter) Write([]byte) (int, error) {
	return 0, writer.err
}

type shortWriter struct{}

func (shortWriter) Write(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	return len(value) - 1, nil
}

func TestBackupBadgerDoesNotCloseWriter(t *testing.T) {
	t.Parallel()

	source := openTestBadger(t)
	if err := source.StoreValue(context.Background(), "total/product", int64(7)); err != nil {
		t.Fatalf("seed counter: %v", err)
	}
	writer := &closeTrackingWriter{}

	if err := source.Backup(context.Background(), writer); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if writer.closed {
		t.Fatal("Backup closed caller-owned writer")
	}
}

type closeTrackingWriter struct {
	bytes.Buffer

	closed bool
}

func (writer *closeTrackingWriter) Close() error {
	writer.closed = true
	return nil
}

func TestBackupBadgerRejectsClosedStore(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if err := store.Backup(context.Background(), &bytes.Buffer{}); !errors.Is(err, badger.ErrDBClosed) {
		t.Fatalf("backup error = %v, want badger.ErrDBClosed", err)
	}
}

func TestBackupBadgerPreventsCloseUntilBackupReturns(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	if err := store.StoreValue(context.Background(), "total/product", int64(7)); err != nil {
		t.Fatalf("seed counter: %v", err)
	}
	writer := newBlockingWriter()
	t.Cleanup(writer.unblock)

	backupDone := make(chan error, 1)
	go func() {
		backupDone <- store.Backup(context.Background(), writer)
	}()
	waitForSignal(t, writer.started, "backup writer")

	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- store.Close()
	}()
	waitForSignal(t, closeStarted, "close call")

	var (
		closeErr      error
		closeReturned bool
	)
	select {
	case closeErr = <-closeDone:
		closeReturned = true
	case <-time.After(100 * time.Millisecond):
	}

	writer.unblock()
	backupErr := waitForError(t, backupDone, "backup")
	if !closeReturned {
		closeErr = waitForError(t, closeDone, "close")
	}
	if backupErr != nil {
		t.Fatalf("backup: %v", backupErr)
	}
	if closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	if closeReturned {
		t.Fatal("Close returned while native backup still owned the store")
	}
}

type blockingWriter struct {
	bytes.Buffer

	started     chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (writer *blockingWriter) Write(value []byte) (int, error) {
	writer.startOnce.Do(func() {
		close(writer.started)
		<-writer.release
	})
	return writer.Buffer.Write(value)
}

func (writer *blockingWriter) unblock() {
	writer.releaseOnce.Do(func() {
		close(writer.release)
	})
}

func waitForSignal(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func waitForError(t *testing.T, result <-chan error, operation string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
		return nil
	}
}
