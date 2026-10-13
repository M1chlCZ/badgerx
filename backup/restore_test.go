package backup

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/m1chlcz/badgerx"
)

func TestRestoreNativeExport(t *testing.T) {
	t.Parallel()
	source, err := badgerx.OpenBadger(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	if err = source.StoreValue(context.Background(), "fixture/counter", "8"); err != nil {
		t.Fatal(err)
	}
	var export bytes.Buffer
	if err = source.Backup(context.Background(), &export); err != nil {
		t.Fatal(err)
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "badger.backup")
	if err = os.WriteFile(file, export.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	count, err := Restore(file, target)
	if err != nil || count != 1 {
		t.Fatalf("native restore keys=%d, err=%v", count, err)
	}
	if _, err = Restore(file, target); !errors.Is(err, ErrRestoreTargetUnavailable) {
		t.Fatal("restore overwrote a nonempty directory")
	}
	if count, err = Verify(target); err != nil || count != 1 {
		t.Fatal("restored data changed")
	}
}

func TestRestoreRejectsInvalidSourceOrTarget(t *testing.T) {
	t.Parallel()
	file := filepath.Join(t.TempDir(), "badger.backup")
	if err := os.WriteFile(file, []byte{1}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(filepath.Join(t.TempDir(), "missing"), t.TempDir()); !errors.Is(err, ErrExportUnavailable) {
		t.Fatal("missing export accepted")
	}
	if _, err := Restore(t.TempDir(), t.TempDir()); !errors.Is(err, ErrExportUnavailable) {
		t.Fatal("directory accepted as an export")
	}
	if _, err := Restore(file, filepath.Join(t.TempDir(), "missing")); !errors.Is(err, ErrRestoreTargetUnavailable) {
		t.Fatal("missing restore target accepted")
	}
	if _, err := Restore(file, t.TempDir()); !errors.Is(err, ErrRestoreFailed) {
		t.Fatal("malformed export accepted")
	}
}

func TestRestoreRejectsMalformedFramingWithoutPanicking(t *testing.T) {
	t.Parallel()
	for _, contents := range [][]byte{[]byte{1}, []byte("notbadger\n")} {
		file := filepath.Join(t.TempDir(), "badger.backup")
		if err := os.WriteFile(file, contents, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Restore(file, t.TempDir()); !errors.Is(err, ErrRestoreFailed) {
			t.Fatalf("malformed export accepted: %q", contents)
		}
	}
}

func TestVerifyReadsClosedNativeSnapshotWithoutChangingValues(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	database, err := badgerx.OpenBadger(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err = database.StoreValue(context.Background(), "views/v1/meta/schema", "v1"); err != nil {
		t.Fatal(err)
	}
	if err = database.StoreValue(context.Background(), "views/v1/total/fixture", "7"); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	count, err := Verify(directory)
	if err != nil || count != 2 {
		t.Fatalf("native restore keys = %d, error = %v", count, err)
	}
	count, err = Verify(directory)
	if err != nil || count != 2 {
		t.Fatalf("second readonly verification keys = %d, error = %v", count, err)
	}
}

func TestVerifyRefusesMalformedOrMissingStore(t *testing.T) {
	t.Parallel()
	if _, err := Verify(filepath.Join(t.TempDir(), "missing")); !errors.Is(err, ErrVerifyFailed) {
		t.Fatal("missing store accepted")
	}
	path := t.TempDir()
	if err := os.WriteFile(filepath.Join(path, "MANIFEST"), []byte("corrupt fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(path); !errors.Is(err, ErrVerifyFailed) {
		t.Fatal("accepted corrupt native Badger snapshot")
	}
}
