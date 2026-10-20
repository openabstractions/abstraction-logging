package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	identity "github.com/openabstractions/abstraction-identity"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// Emitter is receiving evidence at the owned native append boundary. No public
// Sink record includes this field, and direct/shared file writes remain claims.
type Emitter struct {
	Version int    `json:"version"`
	Account string `json:"account"`
	Program string `json:"program"`
	Key     string `json:"captured_file_key,omitempty"`
	// Subject is the proven caller program a trusted in-process record was
	// written about (AboutCaller). It is stored beside the stamp as
	// _oa_subject, which earlier readers ignore; IPC writers never set it.
	Subject string `json:"-"`
}

func (e Emitter) Resource() wire.ApplicationLogResource {
	return wire.ApplicationLogResource{Program: e.Program, CapturedFileKey: e.Key}
}
func (e Emitter) valid() bool {
	return e.Version == 1 && e.Account != "" && len(e.Account) <= 512 && utf8.ValidString(e.Account) &&
		!strings.ContainsAny(e.Account, "\x00\r\n") && ValidApplicationResource(e.Resource()) &&
		(e.Subject == "" || utf8.ValidString(e.Subject) && identity.ValidSubjectProgram(e.Subject))
}

type AttributedSink interface{ WriteAttributed(Record, Emitter) error }

// WriteAttributed is available only for the service-owned, single-writer store.
// Mandatory attribution stays outside the payload shrink ladder.
func (s *FileSink) WriteAttributed(r Record, e Emitter) error {
	if s.retention == nil || !e.valid() {
		return errors.New("logging: owned attribution required")
	}
	payload, err := r.Encode()
	if err != nil {
		return err
	}
	max := s.MaxLine
	if max <= 0 {
		max = DefaultMaxLine
	}
	if len(payload) > max {
		payload, err = s.truncated(r, max)
		if err != nil {
			return err
		}
	}
	line, err := encodeAttributed(payload, e, 65536)
	if err != nil {
		return err
	}
	return s.appendEncoded(line)
}

// EncodeAttributedExact checks the complete physical row, including attribution
// and newline, against a trusted finite cap. It never shrinks. Only an owned
// single-writer retained sink supports this path; a fit is no I/O reservation.
// MaxLine continues to govern ordinary writes independently.
func (s *FileSink) EncodeAttributedExact(r Record, e Emitter, physicalCap int) ([]byte, error) {
	if s.retention == nil || !e.valid() {
		return nil, errors.New("logging: owned attribution required")
	}
	s.mu.Lock()
	closed := s.historyClosed || s.f == nil
	s.mu.Unlock()
	if closed {
		return nil, os.ErrClosed
	}
	payload, err := r.Encode()
	if err != nil {
		return nil, err
	}
	return encodeAttributed(payload, e, physicalCap)
}

// WriteAttributedExact retains every supplied value or reports failure. Its
// complete-row encoder is also the pre-effect size-check owner. Retention and
// the existing append lock still apply; no multiwriter large-write guarantee is
// made, and callers must treat an I/O failure as potentially partial.
func (s *FileSink) WriteAttributedExact(r Record, e Emitter, physicalCap int) error {
	line, err := s.EncodeAttributedExact(r, e, physicalCap)
	if err != nil {
		return err
	}
	return s.appendEncoded(line)
}

func encodeAttributed(payload []byte, e Emitter, physicalCap int) ([]byte, error) {
	if physicalCap <= 0 || len(payload) > physicalCap {
		return nil, ErrLineCap
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, err
	}
	stamp, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	fields["_oa_emitter"] = stamp
	// "_oa_at" sorts before every other key, so the placeholder opens the row.
	fields["_oa_at"] = json.RawMessage(`"` + atZero + `"`)
	if e.Subject != "" {
		if fields["_oa_subject"], err = json.Marshal(e.Subject); err != nil {
			return nil, err
		}
	}
	line, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(line, atPlaceholder) {
		return nil, errors.New("logging: sink instant must open the row")
	}
	line = append(line, '\n')
	if len(line) > physicalCap {
		return nil, ErrLineCap
	}
	return line, nil
}

// attributedWriter labels trusted in-process writes without changing the raw
// provider's history or retention interfaces.
type attributedWriter struct {
	out     AttributedSink
	emitter Emitter
	exact   *FileSink
	cap     int
}

func (w attributedWriter) Write(r Record) error {
	if w.exact != nil {
		return w.exact.WriteAttributedExact(r, w.emitter, w.cap)
	}
	return w.out.WriteAttributed(r, w.emitter)
}
func WithEmitter(out AttributedSink, e Emitter) (Sink, error) {
	if out == nil || !e.valid() {
		return nil, errors.New("logging: invalid self emitter")
	}
	return attributedWriter{out: out, emitter: e}, nil
}

// WithExactEmitter binds an owned audit writer to one trusted physical cap.
// Ordinary WithEmitter and the provider's MaxLine retain their shrink behavior.
func WithExactEmitter(out *FileSink, e Emitter, physicalCap int) (Sink, error) {
	if out == nil || out.retention == nil || !e.valid() || physicalCap <= 0 {
		return nil, errors.New("logging: invalid exact owned emitter")
	}
	return attributedWriter{emitter: e, exact: out, cap: physicalCap}, nil
}

// The sink-written instant (_oa_at): fixed width, so the encoded row's length
// is known before the append lock and the instant is filled in under it.
const (
	atLayout = wire.TimestampLayout
	atZero   = "0000-00-00T00:00:00.000000Z"
)

var (
	atKey         = []byte(`{"_oa_at":"`)
	atPlaceholder = []byte(`{"_oa_at":"` + atZero + `"`)
)

// storedAt reads a row's sink-written instant; a row without one has none.
func storedAt(line []byte) (time.Time, bool) {
	if !bytes.HasPrefix(line, atKey) || len(line) < len(atKey)+len(atLayout)+1 || line[len(atKey)+len(atLayout)] != '"' {
		return time.Time{}, false
	}
	at, err := time.Parse(atLayout, string(line[len(atKey):len(atKey)+len(atLayout)]))
	return at, err == nil
}

func decodeStoredRow(line []byte, owned bool) (wire.Record, *Emitter, bool, error) {
	record, err := wire.Decode(line)
	if err != nil {
		return wire.Record{}, nil, false, err
	}
	if !owned {
		return *record, nil, false, nil
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(line, &fields); err != nil {
		return wire.Record{}, nil, false, err
	}
	raw, ok := fields["_oa_emitter"]
	if !ok {
		return *record, nil, false, nil
	}
	var e Emitter
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&e); err != nil || !e.valid() {
		return *record, nil, true, nil
	}
	// Canonical stamp rejects duplicate fields and unowned alternate spellings.
	canonical, _ := json.Marshal(e)
	if !bytes.Equal(raw, canonical) {
		return *record, nil, true, nil
	}
	if raw, ok := fields["_oa_subject"]; ok {
		var subject string
		if json.Unmarshal(raw, &subject) != nil || subject == "" {
			return *record, nil, true, nil
		}
		e.Subject = subject
		if canonical, _ := json.Marshal(subject); !bytes.Equal(raw, canonical) || !e.valid() {
			return *record, nil, true, nil
		}
	}
	return *record, &e, false, nil
}
