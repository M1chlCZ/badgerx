//go:build unix

package backup

import (
	"os"
	"syscall"
)

func owned(info os.FileInfo) bool {
	metadata, ok := info.Sys().(*syscall.Stat_t)
	return ok && metadata.Uid == uint32(os.Geteuid())
}
