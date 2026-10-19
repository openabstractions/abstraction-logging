//go:build !windows

package service

import "errors"

// protectedCommandEnvironment exists only on Windows, the one platform with a
// protected source in this release (LOG-N1).
func protectedCommandEnvironment() ([]string, error) {
	return nil, errors.New("logging source: no protected source on this platform")
}
