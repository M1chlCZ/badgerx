//go:build unix

package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/m1chlcz/badgerx"
)

func tempDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "badgerx-backup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

type gatedWriter struct {
	ctx     context.Context
	writer  io.Writer
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (writer *gatedWriter) Write(value []byte) (int, error) {
	writer.once.Do(func() { close(writer.entered) })
	select {
	case <-writer.resume:
		return writer.writer.Write(value)
	case <-writer.ctx.Done():
		return 0, writer.ctx.Err()
	}
}

func TestOnlineExportAllowsWrites(t *testing.T) {
	t.Parallel()
	directory := tempDirectory(t)
	store, err := badgerx.OpenBadger(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if _, err = store.IncrementValue(context.Background(), "fixture/count"); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	server, err := Listen(directory, SocketPath(directory), func(ctx context.Context, output io.Writer) error {
		return store.Backup(ctx, &gatedWriter{ctx: ctx, writer: output, entered: entered, resume: resume})
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	var output bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- Export(context.Background(), directory, SocketPath(directory), &output) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("native export did not begin")
	}
	written := make(chan error, 1)
	go func() {
		_, writeErr := store.IncrementValue(context.Background(), "fixture/count")
		written <- writeErr
	}()
	select {
	case err = <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("online backup blocked a live store write")
	}
	close(resume)
	select {
	case err = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("native export did not finish")
	}
	if err != nil {
		t.Fatal(err)
	}
	restored, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close() }()
	if err = restored.Load(bytes.NewReader(output.Bytes()), 2); err != nil {
		t.Fatal(err)
	}
	if err = restored.View(func(tx *badger.Txn) error {
		_, lookupErr := tx.Get([]byte("fixture/count"))
		return lookupErr
	}); err != nil {
		t.Fatal("online native export lost the existing fixture counter")
	}
	info, err := os.Lstat(SocketPath(directory))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("backup socket is not private")
	}
	server.Close()
	if _, err = os.Lstat(SocketPath(directory)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("backup socket remained after Close")
	}
	if _, err = store.IncrementValue(context.Background(), "fixture/count"); err != nil {
		t.Fatal("closing backup socket closed the live store")
	}
}

func TestSocketRefusesUnsafePaths(t *testing.T) {
	t.Parallel()
	directory := tempDirectory(t)
	export := func(context.Context, io.Writer) error { return nil }
	if server, err := Listen(directory, "", export); err != nil || server != nil {
		t.Fatal("backup must be opt-in")
	}
	for _, path := range []string{"relative.sock", filepath.Join(directory, "other.sock"), filepath.Join(t.TempDir(), SocketName)} {
		if _, err := Listen(directory, path, export); !errors.Is(err, ErrUnavailable) {
			t.Fatal("unsafe socket path accepted")
		}
	}
	path := SocketPath(directory)
	if err := os.WriteFile(path, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(directory, path, export); !errors.Is(err, ErrUnavailable) {
		t.Fatal("existing regular file was replaced")
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != "preserve" {
		t.Fatal("existing file changed")
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err = os.Symlink(directory, alias); err != nil {
		t.Fatal(err)
	}
	if _, err = Listen(alias, filepath.Join(alias, SocketName), export); !errors.Is(err, ErrUnavailable) {
		t.Fatal("symlinked Badger directory accepted")
	}
}

func TestOnlyOneExportAndCloseCancels(t *testing.T) {
	t.Parallel()
	directory := tempDirectory(t)
	entered := make(chan struct{})
	server, err := Listen(directory, SocketPath(directory), func(ctx context.Context, _ io.Writer) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	first := make(chan error, 1)
	go func() { first <- Export(context.Background(), directory, SocketPath(directory), io.Discard) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first export did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = Export(ctx, directory, SocketPath(directory), io.Discard); !errors.Is(err, ErrUnavailable) {
		t.Fatal("second concurrent export was accepted")
	}
	closed := make(chan struct{})
	go func() { server.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not cancel export")
	}
	select {
	case err = <-first:
		if err == nil {
			t.Fatal("interrupted export reported success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not disconnect")
	}
}

func TestDisconnectAndTimeoutCancelExport(t *testing.T) {
	t.Parallel()
	for _, expire := range []bool{false, true} {
		directory := tempDirectory(t)
		entered, canceled := make(chan struct{}), make(chan struct{})
		timeout := exportTimeout
		if expire {
			timeout = 100 * time.Millisecond
		}
		server, err := listen(directory, SocketPath(directory), func(ctx context.Context, _ io.Writer) error {
			close(entered)
			<-ctx.Done()
			close(canceled)
			return ctx.Err()
		}, timeout)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- Export(ctx, directory, SocketPath(directory), io.Discard) }()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			cancel()
			server.Close()
			t.Fatal("export did not start")
		}
		if !expire {
			cancel()
		}
		select {
		case <-canceled:
		case <-time.After(3 * time.Second):
			cancel()
			server.Close()
			t.Fatal("export did not observe cancellation")
		}
		select {
		case err = <-done:
			if err == nil {
				t.Fatal("canceled export reported success")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("client did not finish")
		}
		cancel()
		server.Close()
	}
}

func TestClientRejectsFailedPartialOrCorruptStream(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"failed", "partial", "checksum"} {
		directory := tempDirectory(t)
		path := SocketPath(directory)
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		if err = os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			connection, acceptErr := listener.AcceptUnix()
			if acceptErr != nil {
				return
			}
			defer func() { _ = connection.Close() }()
			_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
			header := make([]byte, len(handshake))
			_, _ = io.ReadFull(connection, header)
			var stream bytes.Buffer
			stream.WriteByte(0)
			value := []byte("fixture native bytes")
			_ = binary.Write(&stream, binary.BigEndian, uint32(len(value)))
			stream.Write(value)
			if mode != "partial" {
				stream.Write([]byte{0, 0, 0, 0})
				status := byte(0)
				if mode == "failed" {
					status = 1
				}
				stream.WriteByte(status)
				checksum := sha256.Sum256(value)
				if mode == "checksum" {
					checksum[0] ^= 1
				}
				stream.Write(checksum[:])
			}
			_, _ = connection.Write(stream.Bytes())
		}()
		var output bytes.Buffer
		if err = Export(context.Background(), directory, path, &output); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("accepted %s export", mode)
		}
		_ = listener.Close()
		<-done
	}
}
