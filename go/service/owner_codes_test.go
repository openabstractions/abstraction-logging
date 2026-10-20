package service

import (
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"testing"
)

func TestLegacyHistoryCodesKeepUntypedCompatibility(t *testing.T) {
	var plain string = CodeForbidden
	var typed wire.ServiceErrorCode = CodeForbidden
	var policy wire.ServiceErrorCode = CodePolicyUnavailable
	if plain != string(wire.ServiceErrorCodeForbidden) || typed != wire.ServiceErrorCodeForbidden || policy != wire.ServiceErrorCodePolicyUnavailable {
		t.Fatal(plain, typed, policy)
	}
}
