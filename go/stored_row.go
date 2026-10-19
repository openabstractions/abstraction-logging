package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// Emitter is receiving evidence at the owned native append boundary. No public
// Sink record includes this field, and direct/shared file writes remain claims.
type Emitter struct {
	Version int    `json:"version"`
	Account string `json:"account"`
	Program string `json:"program"`
	Key     string `json:"captured_file_key,omitempty"`
}

func (e Emitter) Resource() wire.ApplicationLogResource {
	return wire.ApplicationLogResource{Program: e.Program, CapturedFileKey: e.Key}
}
func (e Emitter) valid() bool {
	return e.Version == 1 && e.Account != "" && len(e.Account) <= 512 && utf8.ValidString(e.Account) &&
		!strings.ContainsAny(e.Account, "\x00\r\n") && ValidApplicationResource(e.Resource())
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
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(payload, &fields); err != nil {
		return err
	}
	stamp, err := json.Marshal(e)
	if err != nil {
		return err
	}
	fields["_oa_emitter"] = stamp
	line, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if len(line) > 65536 {
		return ErrLineCap
	}
	return s.appendEncoded(line)
}

// attributedWriter labels trusted in-process writes without changing the raw
// provider's history or retention interfaces.
type attributedWriter struct {
	out     AttributedSink
	emitter Emitter
}

func (w attributedWriter) Write(r Record) error { return w.out.WriteAttributed(r, w.emitter) }
func WithEmitter(out AttributedSink, e Emitter) (Sink, error) {
	if out == nil || !e.valid() {
		return nil, errors.New("logging: invalid self emitter")
	}
	return attributedWriter{out, e}, nil
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
	return *record, &e, false, nil
}
