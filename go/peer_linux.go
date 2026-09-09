//go:build linux

package logging

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"strconv"
	"syscall"
)

// PeerOf asks the kernel who is on the other end of a unix socket.
//
// SO_PEERCRED is not a message the client sends. It is an answer the kernel
// gives about a connection it is mediating, recorded at connect() time from the
// process's real credentials. A client cannot set it, spoof it, or opt out of
// it, and that is the entire reason the service tier exists rather than everyone
// appending to one file.
func PeerOf(c net.Conn) (Attestation, error) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return Attestation{}, fmt.Errorf("logging: %T is not a unix socket, so the kernel has nothing to say about its peer", c)
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return Attestation{}, err
	}
	var cred *syscall.Ucred
	var cerr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return Attestation{}, err
	}
	if cerr != nil {
		return Attestation{}, cerr
	}

	p := Attestation{
		By: ByPeerCred, Verified: true,
		UID: int(cred.Uid), GID: int(cred.Gid), PID: int(cred.Pid),
	}
	if h, err := os.Hostname(); err == nil {
		p.Host = h
	}

	// Name and executable are conveniences resolved from the authoritative
	// numbers. If either lookup fails the Peer is still valid — the uid is what
	// decisions are made on.
	if u, err := user.LookupId(strconv.Itoa(p.UID)); err == nil {
		p.User = u.Username
	}
	if exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", p.PID)); err == nil {
		p.Exe = exe
	}
	return p, nil
}
