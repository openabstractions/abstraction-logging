//go:build !windows && !linux && !darwin

package logging

import (
	"fmt"
	"os"
	"runtime"
)

func lockRetainedHistory(*os.File) error {
	return fmt.Errorf("logging: retained file provider unsupported on %s", runtime.GOOS)
}

func logFilesystemBytes(string) (uint64, error) {
	return 0, fmt.Errorf("logging: filesystem size unavailable on %s", runtime.GOOS)
}
