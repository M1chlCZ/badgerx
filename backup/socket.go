package backup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const (
	handshake     = "BADGERX-BACKUP-V1\n"
	frameBytes    = 64 * 1024
	exportTimeout = 5 * time.Minute
)

// Server accepts one operator export at a time.
type Server struct {
	listener   *net.UnixListener
	path       string
	file       os.FileInfo
	export     ExportFunc
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	connection *net.UnixConn
	done       sync.WaitGroup
	closeOnce  sync.Once
	timeout    time.Duration
}

func socketPath(directory, path string) (string, error) {
	if path == "" || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory ||
		path != SocketPath(directory) {
		return "", ErrUnavailable
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil || resolved != directory {
		return "", ErrUnavailable
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || !owned(info) {
		return "", ErrUnavailable
	}
	return path, nil
}

func privateSocket(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode()&os.ModeSocket != 0 && info.Mode().Perm() == 0o600 && owned(info)
}

// Listen starts the private export socket inside the Badger directory. An
// empty path leaves the export disabled and returns a nil server. The
// directory must be absolute, canonical, owned and private.
func Listen(directory, path string, export ExportFunc) (*Server, error) {
	return listen(directory, path, export, exportTimeout)
}

func listen(directory, path string, export ExportFunc, timeout time.Duration) (*Server, error) {
	if path == "" {
		return nil, nil
	}
	path, err := socketPath(directory, path)
	if err != nil || export == nil {
		return nil, ErrUnavailable
	}
	if err = os.Chmod(directory, 0o700); err != nil {
		return nil, ErrUnavailable
	}
	if _, err = os.Lstat(path); err == nil {
		if !privateSocket(path) {
			return nil, ErrUnavailable
		}
		connection, dialErr := net.DialTimeout("unix", path, time.Second)
		if dialErr == nil {
			_ = connection.Close()
			return nil, ErrUnavailable
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, ErrUnavailable
		}
		if err = os.Remove(path); err != nil {
			return nil, ErrUnavailable
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, ErrUnavailable
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, ErrUnavailable
	}
	listener.SetUnlinkOnClose(false)
	if err = os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		return nil, ErrUnavailable
	}
	info, err := os.Lstat(path)
	if err != nil {
		_ = listener.Close()
		return nil, ErrUnavailable
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{listener: listener, path: path, file: info, export: export,
		ctx: ctx, cancel: cancel, timeout: timeout}
	server.done.Add(1)
	go server.accept()
	return server, nil
}

func (server *Server) accept() {
	defer server.done.Done()
	for {
		connection, err := server.listener.AcceptUnix()
		if err != nil {
			return
		}
		server.mu.Lock()
		if server.connection != nil || server.ctx.Err() != nil {
			server.mu.Unlock()
			_ = connection.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = connection.Write([]byte{1})
			_ = connection.Close()
			continue
		}
		server.connection = connection
		server.done.Add(1)
		server.mu.Unlock()
		go server.serve(connection)
	}
}

func (server *Server) serve(connection *net.UnixConn) {
	defer server.done.Done()
	defer func() {
		_ = connection.Close()
		server.mu.Lock()
		server.connection = nil
		server.mu.Unlock()
	}()
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	header := make([]byte, len(handshake))
	if _, err := io.ReadFull(connection, header); err != nil || string(header) != handshake {
		return
	}
	ctx, cancel := context.WithTimeout(server.ctx, server.timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	_ = connection.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	disconnected := make(chan struct{})
	go func() {
		defer close(disconnected)
		var extra [1]byte
		_, _ = connection.Read(extra[:])
		cancel()
	}()
	defer func() { _ = connection.Close(); <-disconnected }()
	if err := writeBytes(connection, []byte{0}); err != nil {
		return
	}
	writer := &frameWriter{connection: connection, hash: sha256.New()}
	err := server.export(ctx, writer)
	status := byte(0)
	if err != nil || ctx.Err() != nil {
		status = 1
	}
	trailer := make([]byte, 5, 5+sha256.Size)
	trailer[4] = status
	trailer = append(trailer, writer.hash.Sum(nil)...)
	_ = writeBytes(connection, trailer)
}

// Close stops the listener, cancels a running export and removes the socket.
func (server *Server) Close() {
	if server == nil {
		return
	}
	server.closeOnce.Do(func() {
		server.cancel()
		_ = server.listener.Close()
		server.mu.Lock()
		if server.connection != nil {
			_ = server.connection.Close()
		}
		server.mu.Unlock()
		server.done.Wait()
		if current, err := os.Lstat(server.path); err == nil && os.SameFile(server.file, current) {
			_ = os.Remove(server.path)
		}
	})
}

type frameWriter struct {
	connection io.Writer
	hash       hash.Hash
}

func (writer *frameWriter) Write(value []byte) (int, error) {
	written := 0
	for len(value) > 0 {
		size := min(len(value), frameBytes)
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(size))
		if err := writeBytes(writer.connection, header[:]); err != nil {
			return written, err
		}
		if err := writeBytes(writer.connection, value[:size]); err != nil {
			return written, err
		}
		_, _ = writer.hash.Write(value[:size])
		written += size
		value = value[size:]
	}
	return written, nil
}

func writeBytes(writer io.Writer, value []byte) error {
	for len(value) > 0 {
		written, err := writer.Write(value)
		if err != nil {
			return err
		}
		if written < 1 {
			return io.ErrShortWrite
		}
		value = value[written:]
	}
	return nil
}

// Export writes native backup bytes from the running store to output. It
// rejects failed, interrupted or corrupted streams; the caller must discard
// any partial output on error.
func Export(ctx context.Context, directory, path string, output io.Writer) error {
	if ctx == nil || output == nil {
		return ErrUnavailable
	}
	path, err := socketPath(directory, path)
	if err != nil || !privateSocket(path) {
		return ErrUnavailable
	}
	info, err := os.Lstat(directory)
	if err != nil || info.Mode().Perm() != 0o700 {
		return ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, exportTimeout)
	defer cancel()
	connection, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", path)
	if err != nil {
		return ErrUnavailable
	}
	defer func() { _ = connection.Close() }()
	deadline, _ := ctx.Deadline()
	_ = connection.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if err = writeBytes(connection, []byte(handshake)); err != nil {
		return ErrUnavailable
	}
	var status [1]byte
	if _, err = io.ReadFull(connection, status[:]); err != nil || status[0] != 0 {
		return ErrUnavailable
	}
	hash := sha256.New()
	buffer := make([]byte, frameBytes)
	for {
		var header [4]byte
		if _, err = io.ReadFull(connection, header[:]); err != nil {
			return ErrUnavailable
		}
		size := binary.BigEndian.Uint32(header[:])
		if size == 0 {
			var checksum [sha256.Size]byte
			if _, err = io.ReadFull(connection, status[:]); err != nil || status[0] != 0 {
				return ErrUnavailable
			}
			if _, err = io.ReadFull(connection, checksum[:]); err != nil ||
				string(checksum[:]) != string(hash.Sum(nil)) || ctx.Err() != nil {
				return ErrUnavailable
			}
			return nil
		}
		if size > frameBytes {
			return ErrUnavailable
		}
		if _, err = io.ReadFull(connection, buffer[:size]); err != nil {
			return ErrUnavailable
		}
		if err = writeBytes(output, buffer[:size]); err != nil {
			return ErrUnavailable
		}
		_, _ = hash.Write(buffer[:size])
	}
}
