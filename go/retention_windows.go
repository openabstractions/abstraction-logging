package logging

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func lockRetainedHistory(f *os.File) error {
	var overlapped windows.Overlapped
	err := windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return ErrRetentionOwner
	}
	return err
}

func logFilesystemBytes(path string) (uint64, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var total uint64
	err = windows.GetDiskFreeSpaceEx(name, nil, &total, nil)
	return total, err
}
