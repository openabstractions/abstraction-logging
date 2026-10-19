package logging

import (
	"bytes"
	"errors"
	"strings"
	"unicode/utf8"

	identity "github.com/openabstractions/abstraction-identity"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

const ApplicationLogResourceProfile = "abstraction.logging/application-log@1"
const ApplicationLogReadAction = "abstraction.logging/history.read"
const MaxApplicationLogTargetBytes = 16 << 10

func ValidApplicationResource(v wire.ApplicationLogResource) bool {
	return utf8.ValidString(v.Program) && identity.ValidSubjectProgram(v.Program) &&
		len(v.CapturedFileKey) <= 512 && utf8.ValidString(v.CapturedFileKey) &&
		!strings.ContainsAny(v.CapturedFileKey, "\x00\r\n") &&
		len(wire.EncodeApplicationLogResource(&v)) <= MaxApplicationLogTargetBytes
}

func EncodeApplicationResource(v wire.ApplicationLogResource) []byte {
	if !ValidApplicationResource(v) {
		return nil
	}
	return wire.EncodeApplicationLogResource(&v)
}

func DecodeApplicationResource(b []byte) (wire.ApplicationLogResource, error) {
	if len(b) > MaxApplicationLogTargetBytes {
		return wire.ApplicationLogResource{}, errors.New("logging: resource too large")
	}
	v, err := wire.DecodeApplicationLogResource(b)
	if err != nil {
		return wire.ApplicationLogResource{}, err
	}
	if !ValidApplicationResource(*v) || !bytes.Equal(b, EncodeApplicationResource(*v)) {
		return wire.ApplicationLogResource{}, errors.New("logging: noncanonical resource")
	}
	return *v, nil
}

// PrepareApplicationResource captures current evidence once. Requesters name a
// program; they supply no FileKey. Resume uses the already sealed resource.
func PrepareApplicationResource(program string) (wire.ApplicationLogResource, error) {
	if !utf8.ValidString(program) {
		return wire.ApplicationLogResource{}, errors.New("logging: invalid program")
	}
	program = identity.NormalizeSubjectProgram(program)
	v := wire.ApplicationLogResource{Program: program, CapturedFileKey: identity.ProgramKey(program)}
	if !ValidApplicationResource(v) {
		return wire.ApplicationLogResource{}, errors.New("logging: invalid program")
	}
	return v, nil
}

func SameApplicationResource(a, b wire.ApplicationLogResource) bool {
	return ValidApplicationResource(a) && ValidApplicationResource(b) &&
		identity.SameNormalizedSubjectProgramKeyed(a.Program, b.Program,
			func() string { return a.CapturedFileKey }, func() string { return b.CapturedFileKey })
}

// PrepareApplicationLogTarget is the owning grant/Ask boundary. Supplied
// captured identity is refused; this owner captures it once from the program.
func PrepareApplicationLogTarget(request []byte) ([]byte, error) {
	if len(request) > MaxApplicationLogTargetBytes {
		return nil, errors.New("logging: requested target too large")
	}
	v, err := wire.DecodeApplicationLogResource(request)
	if err != nil || !ValidApplicationResource(*v) || v.CapturedFileKey != "" {
		return nil, errors.New("logging: requested program target required")
	}
	target, err := PrepareApplicationResource(v.Program)
	if err != nil {
		return nil, err
	}
	return EncodeApplicationResource(target), nil
}

// ValidateApplicationLogTarget and EqualApplicationLogTargets use only retained
// target evidence. Rights invokes them while evaluating one immutable snapshot.
func ValidateApplicationLogTarget(target []byte) bool {
	_, err := DecodeApplicationResource(target)
	return err == nil
}
func EqualApplicationLogTargets(a, b []byte) bool {
	x, err := DecodeApplicationResource(a)
	if err != nil {
		return false
	}
	y, err := DecodeApplicationResource(b)
	if err != nil {
		return false
	}
	return SameApplicationResource(x, y)
}
