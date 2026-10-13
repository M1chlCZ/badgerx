// Command badgerverify verifies a closed native Badger store or restores a
// native export file into an isolated empty directory. It never opens a live
// store by default and prints only the number of readable values.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/m1chlcz/badgerx/backup"
)

func main() {
	if len(os.Args) != 2 && len(os.Args) != 3 {
		fail("usage: badgerverify [/path/to/native/export] /isolated/restore/path")
	}
	var count uint64
	var err error
	if len(os.Args) == 3 {
		if err = requireIsolatedPaths(os.Args[1], os.Args[2]); err == nil {
			count, err = backup.Restore(os.Args[1], os.Args[2])
		}
	} else {
		if err = requireIsolatedPaths(os.Args[1]); err == nil {
			count, err = backup.Verify(os.Args[1])
		}
	}
	if err != nil {
		fail("Badger verification failed")
	}
	if err = json.NewEncoder(os.Stdout).Encode(struct {
		KeysRead uint64 `json:"keys_read"`
	}{count}); err != nil {
		os.Exit(1)
	}
}

func requireIsolatedPaths(paths ...string) error {
	for _, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) == "/" {
			return errors.New("an isolated absolute path is required")
		}
	}
	return nil
}

func fail(message string) {
	_, _ = fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
