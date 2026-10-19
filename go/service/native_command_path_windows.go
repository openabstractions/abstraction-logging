//go:build windows

package service

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// protectedCommandEnvironment is the whole environment of the elevated
// helper's wevtutil child: SystemRoot and windir from the Windows directory,
// PATH as the system directory alone, and LC_ALL=C (LOG-N1).
func protectedCommandEnvironment() ([]string, error) {
	system, err := windows.GetSystemDirectory()
	if err != nil {
		return nil, fmt.Errorf("Windows system directory unavailable: %w", err)
	}
	root, err := windows.GetSystemWindowsDirectory()
	if err != nil {
		return nil, fmt.Errorf("Windows directory unavailable: %w", err)
	}
	return []string{"SystemRoot=" + root, "windir=" + root, "PATH=" + system, "LC_ALL=C"}, nil
}

func nativeCommandPath(name string) (string, error) {
	if name != "wevtutil.exe" {
		return "", fmt.Errorf("unsupported Windows native command %q", name)
	}
	systemDirectory, err := windows.GetSystemDirectory()
	if err != nil {
		return "", fmt.Errorf("Windows system directory unavailable: %w", err)
	}
	path := filepath.Join(systemDirectory, name)
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("Windows Event Log query tool unavailable in System32")
	}
	return path, nil
}
