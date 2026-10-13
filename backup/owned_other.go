//go:build !unix

package backup

import "os"

func owned(os.FileInfo) bool {
	return false
}
