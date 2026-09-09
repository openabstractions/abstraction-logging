//go:build !linux

package logging

import "net"

// PeerOf has no answer on this platform, and says so.
//
// Windows can do this — GetNamedPipeClientProcessId over a named pipe, then the
// client's token — but it needs a named pipe rather than AF_UNIX (Windows
// supports AF_UNIX sockets without supporting peer credentials on them) and a
// dependency to reach the API. Not written.
//
// Returning an error rather than a plausible Peer is the important part. A
// service on a platform that cannot verify identity must report lines as
// unattributed, so that anything making decisions on identity refuses rather
// than trusting a guess. The service records the reason on the record itself.
func PeerOf(c net.Conn) (Attestation, error) { return Attestation{}, errNoPeerCreds }
