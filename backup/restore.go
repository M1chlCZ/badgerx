package backup

import (
	"os"

	"github.com/dgraph-io/badger/v4"
)

// Verify opens a closed Badger store read-only and returns the number of
// readable values. It never outputs keys or values and expects an isolated
// copy, never a live store.
func Verify(directory string) (uint64, error) {
	database, err := badger.Open(badger.DefaultOptions(directory).WithReadOnly(true).WithLogger(nil))
	if err != nil {
		return 0, ErrVerifyFailed
	}
	defer func() { _ = database.Close() }()
	var count uint64
	err = database.View(func(transaction *badger.Txn) error {
		iterator := transaction.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			if err := iterator.Item().Value(func([]byte) error { return nil }); err != nil {
				return ErrVerifyFailed
			}
			count++
		}
		return nil
	})
	if err != nil {
		return 0, ErrVerifyFailed
	}
	return count, nil
}

// Restore loads a native export file into an existing empty directory and
// verifies the restored store. The caller must discard the target when
// Restore fails.
func Restore(source, target string) (uint64, error) {
	info, err := os.Lstat(source)
	if err != nil || !info.Mode().IsRegular() {
		return 0, ErrExportUnavailable
	}
	contents, err := os.ReadDir(target)
	if err != nil || len(contents) != 0 {
		return 0, ErrRestoreTargetUnavailable
	}
	input, err := os.Open(source)
	if err != nil {
		return 0, ErrExportUnavailable
	}
	defer func() { _ = input.Close() }()
	database, err := badger.Open(badger.DefaultOptions(target).WithLogger(nil))
	if err != nil {
		return 0, ErrRestoreFailed
	}
	loadErr := loadExport(database, input)
	closeErr := database.Close()
	if loadErr != nil || closeErr != nil {
		return 0, ErrRestoreFailed
	}
	return Verify(target)
}

// loadExport converts a Load panic on malformed input into ErrRestoreFailed.
func loadExport(database *badger.DB, input *os.File) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrRestoreFailed
		}
	}()
	return database.Load(input, 2)
}
