//go:build linux

package service

import (
	"fmt"
	"os"
)

func nativeCommandPath(name string) (string, error) {
	if name != "journalctl" {
		return "", fmt.Errorf("unsupported Linux native command %q", name)
	}
	for _, path := range []string{"/usr/bin/journalctl", "/bin/journalctl"} {
		info, err := os.Stat(path)
		if err == nil && info.Mode().IsRegular() {
			return path, nil
		}
	}
	return "", fmt.Errorf("journalctl unavailable in fixed system locations")
}
