//go:build linux || darwin

package logging

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func lockRetainedHistory(f *os.File) error {
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return ErrRetentionOwner
	}
	return err
}

func logFilesystemBytes(path string) (uint64, error) {
	var info unix.Statfs_t
	if err := unix.Statfs(path, &info); err != nil {
		return 0, err
	}
	return info.Blocks * uint64(info.Bsize), nil
}
