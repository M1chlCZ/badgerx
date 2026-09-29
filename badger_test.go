//nolint:testpackage // White-box tests exercise the unexported BadgerStore.db field.
package badgerx

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type cachedProduct struct {
	Name  string `json:"name"`
	Views int64  `json:"views"`
}

const secretJSONError = "sentinel-secret-value"

type failingJSONValue struct{}

func (failingJSONValue) MarshalJSON() ([]byte, error) {
	return nil, errors.New(secretJSONError)
}

type failingJSONTarget struct{}

func (*failingJSONTarget) UnmarshalJSON([]byte) error {
	return errors.New(secretJSONError)
}

func TestBadgerValueRoundTrip(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	want := cachedProduct{Name: "Widget", Views: 42}

	if err := store.StoreValue(context.Background(), "product:1", want); err != nil {
		t.Fatalf("store value: %v", err)
	}
	got, err := store.GetValue[cachedProduct](context.Background(), "product:1")
	if err != nil {
		t.Fatalf("get value: %v", err)
	}
	if got != want {
		t.Fatalf("value = %#v, want %#v", got, want)
	}
}

func TestBadgerArrayRoundTrip(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	want := []cachedProduct{{Name: "Gadget", Views: 7}, {Name: "Item", Views: 9}}

	if err := store.StoreValue(context.Background(), "products", want); err != nil {
		t.Fatalf("store array: %v", err)
	}
	got, err := store.GetArray[cachedProduct](context.Background(), "products")
	if err != nil {
		t.Fatalf("get array: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("array length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("array[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func TestBadgerMissingKeyPreservesBadgerSemantics(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)

	got, err := store.GetValue[cachedProduct](context.Background(), "missing")
	if !errors.Is(err, badger.ErrKeyNotFound) {
		t.Fatalf("error = %v, want badger.ErrKeyNotFound", err)
	}
	if got != (cachedProduct{}) {
		t.Fatalf("missing value = %#v, want zero value", got)
	}
}

func TestBadgerJSONErrorsDoNotLeakValues(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)

	if err := store.StoreValue(
		context.Background(),
		"encode",
		failingJSONValue{}); !errors.Is(err, ErrBadgerValueEncode) ||
		strings.Contains(err.Error(), secretJSONError) {
		t.Fatalf("encode error = %q, want safe ErrBadgerValueEncode", err)
	}
	if err := store.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("decode"), []byte(`{}`))
	}); err != nil {
		t.Fatalf("seed decode value: %v", err)
	}
	if _, err := store.GetValue[failingJSONTarget](
		context.Background(),
		"decode"); !errors.Is(err, ErrBadgerValueDecode) ||
		strings.Contains(err.Error(), secretJSONError) {
		t.Fatalf("decode error = %q, want safe ErrBadgerValueDecode", err)
	}
}

func TestBadgerTTLStoresExpiryAndExpiredValuesAreHidden(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	const key = "product:ttl"
	ttl := 2 * time.Minute
	started := time.Now()

	if err := store.StoreValueTTL(context.Background(), key, cachedProduct{Name: "Device"}, ttl); err != nil {
		t.Fatalf("store value with TTL: %v", err)
	}

	var expiresAt uint64
	if err := store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		expiresAt = item.ExpiresAt()
		return nil
	}); err != nil {
		t.Fatalf("read expiry metadata: %v", err)
	}
	minimum := uint64(started.Add(ttl).Unix())
	maximum := uint64(time.Now().Add(ttl).Unix())
	if expiresAt < minimum || expiresAt > maximum {
		t.Fatalf("expiresAt = %d, want between %d and %d", expiresAt, minimum, maximum)
	}

	expired := badger.NewEntry([]byte("expired"), []byte(`{"name":"old","views":1}`))
	expired.ExpiresAt = 1
	if err := store.db.Update(func(txn *badger.Txn) error { return txn.SetEntry(expired) }); err != nil {
		t.Fatalf("seed expired value: %v", err)
	}
	if _, err := store.GetValue[cachedProduct](
		context.Background(),
		"expired"); !errors.Is(
		err,
		badger.ErrKeyNotFound,
	) {
		t.Fatalf("expired error = %v, want badger.ErrKeyNotFound", err)
	}
}

func TestDelBadgerValue(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	if err := store.StoreValue(context.Background(), "delete-me", 1); err != nil {
		t.Fatalf("store value: %v", err)
	}

	if err := store.DeleteValue(context.Background(), "delete-me"); err != nil {
		t.Fatalf("delete value: %v", err)
	}
	if _, err := store.GetValue[int](context.Background(), "delete-me"); !errors.Is(err, badger.ErrKeyNotFound) {
		t.Fatalf("error after delete = %v, want badger.ErrKeyNotFound", err)
	}
}

func TestCanceledContextDoesNotMutateBadger(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := store.StoreValue(context.Background(), "counter", int64(41)); err != nil {
		t.Fatalf("seed counter: %v", err)
	}
	if err := store.StoreValue(ctx, "counter", int64(99)); !errors.Is(err, context.Canceled) {
		t.Fatalf("store error = %v, want context.Canceled", err)
	}
	if err := store.StoreValueTTL(ctx, "ttl", "secret", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("TTL store error = %v, want context.Canceled", err)
	}
	if _, err := store.IncrementValue(ctx, "counter"); !errors.Is(err, context.Canceled) {
		t.Fatalf("increment error = %v, want context.Canceled", err)
	}
	if err := store.DeleteValue(ctx, "counter"); !errors.Is(err, context.Canceled) {
		t.Fatalf("delete error = %v, want context.Canceled", err)
	}

	got, err := store.GetValue[int64](context.Background(), "counter")
	if err != nil {
		t.Fatalf("get counter: %v", err)
	}
	if got != 41 {
		t.Fatalf("counter = %d, want unchanged value 41", got)
	}
	if _, getBadgerValueErr := store.GetValue[string](
		context.Background(),
		"ttl"); !errors.Is(
		getBadgerValueErr,
		badger.ErrKeyNotFound,
	) {
		t.Fatalf("canceled TTL store mutated database: %v", getBadgerValueErr)
	}
}

func TestIncrementBadgerValueIsAtomicUnderContention(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	const (
		workers    = 24
		increments = 75
	)

	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range increments {
				if _, err := store.IncrementValue(context.Background(), "views"); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("increment: %v", err)
	}

	got, err := store.GetValue[int64](context.Background(), "views")
	if err != nil {
		t.Fatalf("get counter: %v", err)
	}
	if want := int64(workers * increments); got != want {
		t.Fatalf("counter = %d, want %d", got, want)
	}
}

func TestIncrementBadgerValueRejectsInvalidAndOverflowingCounters(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	if err := store.StoreValue(context.Background(), "invalid", "not-a-number"); err != nil {
		t.Fatalf("store invalid counter: %v", err)
	}
	if _, err := store.IncrementValue(
		context.Background(),
		"invalid"); !errors.Is(
		err,
		ErrBadgerCounterInvalid,
	) {
		t.Fatalf("invalid counter error = %v, want ErrBadgerCounterInvalid", err)
	}

	if err := store.StoreValue(context.Background(), "overflow", int64(math.MaxInt64)); err != nil {
		t.Fatalf("store maximum counter: %v", err)
	}
	if _, err := store.IncrementValue(
		context.Background(),
		"overflow"); !errors.Is(
		err,
		ErrBadgerCounterOverflow,
	) {
		t.Fatalf("overflow error = %v, want ErrBadgerCounterOverflow", err)
	}
	got, err := store.GetValue[int64](context.Background(), "overflow")
	if err != nil {
		t.Fatalf("get maximum counter: %v", err)
	}
	if got != math.MaxInt64 {
		t.Fatalf("overflow counter mutated to %d", got)
	}
}

func TestIncrementBadgerValueIncrementsSignedCounter(t *testing.T) {
	t.Parallel()

	store := openTestBadger(t)
	if err := store.StoreValue(context.Background(), "negative", int64(-1)); err != nil {
		t.Fatalf("store negative counter: %v", err)
	}
	got, err := store.IncrementValue(context.Background(), "negative")
	if err != nil {
		t.Fatalf("increment negative counter: %v", err)
	}
	if got != 0 {
		t.Fatalf("incremented value = %d, want 0", got)
	}
	stored, err := store.GetValue[int64](context.Background(), "negative")
	if err != nil {
		t.Fatalf("get incremented counter: %v", err)
	}
	if stored != 0 {
		t.Fatalf("stored value = %d, want 0", stored)
	}
}

func TestBadgerPersistsAcrossCloseAndReopen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "badger")
	store, err := OpenBadger(path)
	if err != nil {
		t.Fatalf("open Badger: %v", err)
	}
	if storeBadgerValueErr := store.StoreValue(
		context.Background(),
		"persistent",
		cachedProduct{Name: "Tool", Views: 3}); storeBadgerValueErr != nil {
		t.Fatalf("store persistent value: %v", storeBadgerValueErr)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatalf("close Badger: %v", closeErr)
	}
	if closeErr2 := store.Close(); closeErr2 != nil {
		t.Fatalf("close Badger twice: %v", closeErr2)
	}

	reopened, err := OpenBadger(path)
	if err != nil {
		t.Fatalf("reopen Badger: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := reopened.Close(); closeErr != nil {
			t.Errorf("close reopened Badger: %v", closeErr)
		}
	})
	got, err := reopened.GetValue[cachedProduct](context.Background(), "persistent")
	if err != nil {
		t.Fatalf("get persistent value: %v", err)
	}
	if want := (cachedProduct{Name: "Tool", Views: 3}); got != want {
		t.Fatalf("persistent value = %#v, want %#v", got, want)
	}
}

func TestBadgerHelpersRejectInvalidArgumentsAndClosedStore(t *testing.T) {
	t.Parallel()

	if _, err := OpenBadger(""); !errors.Is(err, ErrBadgerPathRequired) {
		t.Fatalf("empty path error = %v, want ErrBadgerPathRequired", err)
	}
	if _, err := OpenBadger(" \t "); !errors.Is(err, ErrBadgerPathRequired) {
		t.Fatalf("blank path error = %v, want ErrBadgerPathRequired", err)
	}

	store := openTestBadger(t)
	if err := (*BadgerStore)(nil).StoreValue(
		context.Background(),
		"key",
		1); !errors.Is(
		err,
		ErrBadgerStoreUnavailable,
	) {
		t.Fatalf("nil store error = %v, want ErrBadgerStoreUnavailable", err)
	}
	if _, err := (&BadgerStore{}).GetValue[int](
		context.Background(),
		"key"); !errors.Is(
		err,
		ErrBadgerStoreUnavailable,
	) {
		t.Fatalf("zero store error = %v, want ErrBadgerStoreUnavailable", err)
	}
	if err := store.StoreValue(context.Background(), "", 1); !errors.Is(err, ErrBadgerKeyRequired) {
		t.Fatalf("empty key error = %v, want ErrBadgerKeyRequired", err)
	}
	if _, err := store.GetArray[int](context.Background(), " \t"); !errors.Is(err, ErrBadgerKeyRequired) {
		t.Fatalf("blank key error = %v, want ErrBadgerKeyRequired", err)
	}
	if err := store.StoreValueTTL(context.Background(), "key", 1, 0); !errors.Is(err, ErrBadgerTTLInvalid) {
		t.Fatalf("zero TTL error = %v, want ErrBadgerTTLInvalid", err)
	}
	if err := store.StoreValueTTL(
		context.Background(),
		"key",
		1,
		-time.Second); !errors.Is(
		err,
		ErrBadgerTTLInvalid,
	) {
		t.Fatalf("negative TTL error = %v, want ErrBadgerTTLInvalid", err)
	}
	if err := store.DeleteValue(
		nil, //nolint:staticcheck // The nil context asserts the ErrBadgerContextRequired guard.
		"key"); !errors.Is(
		err,
		ErrBadgerContextRequired,
	) {
		t.Fatalf("nil context error = %v, want ErrBadgerContextRequired", err)
	}

	closed, err := OpenBadger(filepath.Join(t.TempDir(), "closed"))
	if err != nil {
		t.Fatalf("open store to close: %v", err)
	}
	if closeErr := closed.Close(); closeErr != nil {
		t.Fatalf("close store: %v", closeErr)
	}
	if storeBadgerValueErr := closed.StoreValue(
		context.Background(),
		"key",
		1); !errors.Is(
		storeBadgerValueErr,
		badger.ErrDBClosed,
	) {
		t.Fatalf("closed store error = %v, want badger.ErrDBClosed", storeBadgerValueErr)
	}
}

func openTestBadger(t *testing.T) *BadgerStore {
	t.Helper()
	store, err := OpenBadger(filepath.Join(t.TempDir(), "badger"))
	if err != nil {
		t.Fatalf("open Badger: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close Badger: %v", closeErr)
		}
	})
	return store
}
