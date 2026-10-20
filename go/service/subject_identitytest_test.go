//go:build oa_identitytest

package service

import (
	"errors"
	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/identitytest"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"os"
	"path/filepath"
	"testing"
)

func TestSubjectIdentityUnknownUIDRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.exe")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	peer := identitytest.New(t, identitytest.Facts{User: identity.User{Kind: "posix", UID: -1}, Path: path})[0]
	if _, _, err := applicationSubject(peer, "-1"); err == nil {
		t.Fatal("unknown uid accepted")
	}
}

func TestSubjectIdentityOwnerAdmissionKeepsVersionedRefusals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.exe")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	peers := identitytest.New(t,
		identitytest.Facts{User: identity.User{Kind: "windows", SID: "owner"}, Path: path},
		identitytest.Facts{User: identity.User{Kind: "windows", SID: "foreign"}, Path: path},
		identitytest.Facts{User: identity.User{Kind: "windows", SID: "owner"}},
		identitytest.Facts{User: identity.User{Kind: "windows"}, Path: path})
	for _, tc := range []struct {
		name  string
		peer  *identity.Peer
		owner string
		code  wire.ServiceErrorCode
	}{
		{"nil", nil, "owner", wire.ServiceErrorCodeCallerUnavailable},
		{"same", peers[0], "owner", ""},
		{"foreign", peers[1], "owner", wire.ServiceErrorCodeWrongUser},
		{"empty owner", peers[0], "", wire.ServiceErrorCodeWrongUser},
		{"no program", peers[2], "owner", wire.ServiceErrorCodeIdentityRequired},
		{"no account", peers[3], "owner", wire.ServiceErrorCodeWrongUser},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account, program, err := applicationSubject(tc.peer, tc.owner)
			if tc.code == "" {
				if err != nil || account != "owner" || program != filepath.Clean(path) {
					t.Fatal(account, program, err)
				}
				return
			}
			var refusal *wire.ServiceError
			if !errors.As(err, &refusal) || refusal.Code != tc.code || account != "" || program != "" {
				t.Fatal(account, program, err)
			}
		})
	}
}
