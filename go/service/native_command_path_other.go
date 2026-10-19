//go:build !windows && !linux && !darwin

package service

import "fmt"

func nativeCommandPath(name string) (string, error) {
	return "", fmt.Errorf("no native source command for %q", name)
}
