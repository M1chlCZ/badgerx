// Package backup serves an operator-only online BadgerDB export over a private
// Unix socket and provides native backup restore and verification helpers.
//
// A server accepts one export at a time. The socket lives inside the Badger
// directory (see SocketName) with owner-only permissions and no network
// listener. The wire protocol is a fixed handshake, 64 KiB frames and a final
// status and SHA-256 trailer; Export validates the checksum and rejects
// partial or corrupted streams.
package backup

import (
	"context"
	"errors"
	"io"
	"path/filepath"
)

// ExportFunc writes a native Badger backup to output and stops when the
// context is canceled.
type ExportFunc func(ctx context.Context, output io.Writer) error

// SocketName is the file name of the private export socket inside a Badger
// directory.
const SocketName = ".backup.sock"

// Sentinel errors returned by this package.
var (
	ErrUnavailable              = errors.New("private Badger export is unavailable")
	ErrExportUnavailable        = errors.New("native Badger export is unavailable")
	ErrRestoreTargetUnavailable = errors.New("the restore target must be an existing empty directory")
	ErrRestoreFailed            = errors.New("native Badger export could not be restored")
	ErrVerifyFailed             = errors.New("the Badger store could not be verified")
)

// SocketPath returns the export socket path for a Badger directory.
func SocketPath(directory string) string {
	return filepath.Join(directory, SocketName)
}
