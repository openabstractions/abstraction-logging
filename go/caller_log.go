package logging

import (
	"bytes"
	"errors"
	"unicode/utf8"

	identity "github.com/openabstractions/abstraction-identity"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// CallerLogResourceProfile admits reader@2 ReadAboutCaller: the records the
// receiving runtime wrote about the rule subject's own requests. Its one target
// is fixed to that subject; no program can be named.
const CallerLogResourceProfile = "abstraction.logging/caller-log@1"

// CallerLogTarget is the profile's one canonical target document: an
// application-log resource with no program, read as "the rule subject itself".
func CallerLogTarget() []byte {
	return wire.EncodeApplicationLogResource(&wire.ApplicationLogResource{})
}

// PrepareCallerLogTarget accepts only the self document; a named program or
// captured key is refused.
func PrepareCallerLogTarget(request []byte) ([]byte, error) {
	if len(request) > MaxApplicationLogTargetBytes {
		return nil, errors.New("logging: requested target too large")
	}
	v, err := wire.DecodeApplicationLogResource(request)
	if err != nil || v.Program != "" || v.CapturedFileKey != "" {
		return nil, errors.New("logging: caller-log target names no program")
	}
	return CallerLogTarget(), nil
}

// ValidateCallerLogTarget reports whether raw is the canonical self document.
func ValidateCallerLogTarget(raw []byte) bool { return bytes.Equal(raw, CallerLogTarget()) }

// AboutCaller returns a writer whose trusted in-process records are stamped as
// written about the proven caller program. The stamp stays private; the public
// Record is unchanged. out must come from WithEmitter or WithExactEmitter;
// another sink, which has no owned attribution, is returned unchanged.
func AboutCaller(out Sink, program string) (Sink, error) {
	w, ok := out.(attributedWriter)
	if !ok {
		return out, nil
	}
	if !utf8.ValidString(program) || !identity.ValidSubjectProgram(program) {
		return nil, errors.New("logging: invalid caller program")
	}
	w.emitter.Subject = identity.NormalizeSubjectProgram(program)
	if !w.emitter.valid() {
		return nil, errors.New("logging: invalid caller program")
	}
	return w, nil
}

// SameCaller reports whether a stamped subject names the proven caller program.
func SameCaller(subject, program string) bool {
	return subject != "" && program != "" && identity.NormalizeSubjectProgram(subject) == identity.NormalizeSubjectProgram(program)
}
