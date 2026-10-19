//go:build darwin

package service

import "fmt"

func nativePlatformSourceAdapters(nativeCommandRunner) map[string]SourceAdapter {
	// macOS unified logging has no portable durable cursor. This build keeps
	// the stable source ID visible as unsupported until an entitled native
	// adapter can provide tested source-instance and gap behavior.
	return nil
}

func nativeCommandPath(name string) (string, error) {
	return "", fmt.Errorf("no native command adapter for macOS source %q", name)
}
