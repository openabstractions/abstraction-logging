//go:build !windows

package logging

import "os"

// Separation reports what this file actually enforces, read from the mode the
// filesystem is enforcing rather than from the path convention. The convention
// is advice; the mode is the mechanism.
//
// A file only its owner may write is separated by the OS: another account
// cannot forge lines into it because it cannot open it. A world-writable file
// separates nothing, however carefully the records name themselves.
func (s *FileSink) Separation() Separation {
	fi, err := os.Stat(s.path)
	if err != nil {
		return SeparationNone
	}
	if fi.Mode().Perm()&0o022 != 0 {
		return SeparationNone // group or other may write it
	}
	return SeparationOwner
}
