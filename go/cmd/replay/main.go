// replay applies a scripted sequence of operations to this logging binding and
// prints what an observer would have seen after each one.
//
// The transcript carries shapes and verdicts and never a value: a message, a
// host name and an instant belong to one machine. `machine`, `attest` and
// `forge` script a world — the sink a machine offers, what a platform's
// attester answered, what a hostile writer attached — so a rule about the
// chain is judgeable on a machine whose platform has no attester at all.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	logging "github.com/openabstractions/abstraction-logging/go"
)

// operatorKey is the key the benched receiver holds. A forger binds under a
// key it made up and names the same id, which is the attack.
var operatorKey = []byte("operator-installed-relay-key")

var policy = logging.Policy{
	Relay: func(a logging.Attestation) bool { return a.Exe == "/usr/bin/relay" },
	Keys:  map[string][]byte{"k1": operatorKey},
}

type sinkState struct {
	sink   logging.Sink
	path   string
	served *served
}

// served collects what a benched service accepted. The service writes from
// the goroutine that read the line, so the receive is the arrival.
type served struct {
	ch    chan logging.Record
	lines int
}

func (s *served) Write(r logging.Record) error { s.ch <- r; return nil }

type counting struct {
	inner logging.Sink
	n     int
}

func (c *counting) Write(r logging.Record) error { c.n++; return c.inner.Write(r) }

type replay struct {
	work    string
	out     *bufio.Writer
	records map[string]*logging.Record
	anchors map[string]*logging.Attestation
	sinks   map[string]*sinkState
	server  *logging.Server
	current *served
	n       int
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--capabilities" {
		fmt.Print("logging\n")
		return
	}
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: replay <workdir> <scenario> | replay --capabilities")
		os.Exit(2)
	}
	script, err := os.ReadFile(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	p := &replay{
		work:    os.Args[1],
		out:     bufio.NewWriter(os.Stdout),
		records: map[string]*logging.Record{},
		anchors: map[string]*logging.Attestation{},
		sinks:   map[string]*sinkState{},
	}
	defer p.out.Flush()
	defer p.stopServer()
	step := 0
	for _, line := range strings.Split(string(script), "\n") {
		line = strings.TrimRight(line, " \t\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		step++
		fmt.Fprintf(p.out, "%02d %s -> %s\n", step, line, p.apply(strings.Fields(line)))
		p.out.Flush()
	}
}

func (p *replay) apply(f []string) string {
	arg := func(i int) string {
		if i < len(f) {
			return f[i]
		}
		return ""
	}
	has := func(flag string) bool {
		for _, x := range f[1:] {
			if x == flag {
				return true
			}
		}
		return false
	}
	switch f[0] {
	case "level":
		n, err := strconv.Atoi(arg(1))
		if err != nil {
			return "invalid"
		}
		return "ok name=" + logging.Level(n).String()
	case "record":
		n, err := strconv.Atoi(arg(2))
		if err != nil || arg(1) == "" {
			return "invalid"
		}
		r := &logging.Record{Schema: logging.SchemaVersion, Time: logging.At(time.Unix(0, 0).UTC()),
			Level: logging.Level(n), Msg: "hello", Identity: logging.Identity{logging.Claim("replay")}}
		p.records[arg(1)] = r
		delete(p.anchors, arg(1))
		return fmt.Sprintf("ok schema=%d", r.Schema)
	case "attr":
		r := p.records[arg(1)]
		if r == nil || arg(3) == "" {
			return "invalid"
		}
		if r.Attrs == nil {
			r.Attrs = map[string]string{}
		}
		r.Attrs[arg(2)] = arg(3)
		return "ok"
	case "group":
		r := p.records[arg(1)]
		if r == nil || arg(4) == "" {
			return "invalid"
		}
		return p.viaHandler(r, arg(2), arg(3), arg(4))
	case "msg":
		r := p.records[arg(1)]
		n, err := strconv.Atoi(arg(2))
		if r == nil || err != nil {
			return "invalid"
		}
		// `wide` counts characters rather than bytes, two bytes each, so that a
		// shrink has somewhere to split a character if it slices by byte index.
		if has("wide") {
			r.Msg = strings.Repeat("é", n)
		} else {
			r.Msg = strings.Repeat("x", n)
		}
		return "ok"
	case "job":
		r := p.records[arg(1)]
		n, err := strconv.Atoi(arg(2))
		if r == nil || err != nil {
			return "invalid"
		}
		r.Job = strings.Repeat("x", n)
		return "ok"
	case "read":
		r := p.records[arg(1)]
		if r == nil {
			return "invalid"
		}
		v, ok := r.Attrs[arg(2)]
		if !ok {
			return "no-answer absent"
		}
		return "ok type=string value=" + v
	case "keys":
		r := p.records[arg(1)]
		if r == nil {
			return "invalid"
		}
		keys := make([]string, 0, len(r.Attrs))
		for k := range r.Attrs {
			keys = append(keys, k)
		}
		if len(keys) == 0 {
			return "ok -"
		}
		sort.Strings(keys)
		return "ok " + strings.Join(keys, ",")
	case "encode":
		r := p.records[arg(1)]
		if r == nil {
			return "invalid"
		}
		return shape(*r)
	case "decode":
		n, err := strconv.Atoi(arg(1))
		if err != nil {
			return "invalid"
		}
		line := fmt.Sprintf(`{"schema":%d,"time":"1970-01-01T00:00:00.000000Z","level":0,"msg":"x"}`, n)
		if _, err := logging.DecodeRecord([]byte(line)); err != nil {
			return fmt.Sprintf("refused schema=%d", n)
		}
		return "ok"
	case "machine":
		return p.machine(arg(1))
	case "auto":
		if arg(1) == "" {
			return "invalid"
		}
		s := logging.Auto("replay")
		st := &sinkState{sink: s}
		switch v := s.(type) {
		case *logging.ServiceSink:
			v.Fallback = &counting{inner: v.Fallback}
			st.served = p.current
		case *logging.FileSink:
			st.path = v.Path()
		}
		p.sinks[arg(1)] = st
		return "ok tier=" + tier(s)
	case "default":
		if _, ok := logging.Default("replay").(*logging.Handler); ok {
			return "ok out=sink"
		}
		return "ok out=stderr"
	case "separation":
		st := p.sinks[arg(1)]
		if st == nil {
			return "invalid"
		}
		return "ok sep=" + logging.SeparationOf(st.sink).String()
	case "cap":
		st := p.sinks[arg(1)]
		n, err := strconv.Atoi(arg(2))
		if st == nil || err != nil {
			return "invalid"
		}
		fs, ok := st.sink.(*logging.FileSink)
		if !ok {
			return "invalid"
		}
		fs.MaxLine = n
		return "ok"
	case "write":
		st := p.sinks[arg(1)]
		if st == nil {
			return "invalid"
		}
		r := logging.Record{Schema: logging.SchemaVersion, Time: logging.At(time.Unix(0, 0).UTC()),
			Msg: "hello", Identity: logging.Identity{logging.Claim("replay")}}
		if arg(2) != "" {
			if p.records[arg(2)] == nil {
				return "invalid"
			}
			r = *p.records[arg(2)]
		}
		return p.write(st, r)
	case "held":
		st := p.sinks[arg(1)]
		if st == nil {
			return "invalid"
		}
		return p.held(st)
	case "forge":
		r := p.records[arg(1)]
		if r == nil || arg(2) == "" {
			return "invalid"
		}
		claim, _ := r.Identity.Claimed()
		hop := len(r.Identity)
		r.Identity = append(r.Identity, logging.Attestation{By: arg(2), Verified: true, Hop: hop,
			Host: claim.Host, User: "root", Exe: "/sbin/init", UID: 0, GID: 0, PID: 1})
		if has("bind") {
			r.Bind(hop, "k1", []byte("a key the forger made up"))
		}
		return "ok"
	case "send":
		r := p.records[arg(1)]
		if r == nil {
			return "invalid"
		}
		return p.send(arg(1), r)
	case "attest":
		r := p.records[arg(1)]
		if r == nil || arg(2) == "" {
			return "invalid"
		}
		return p.attest(arg(1), r, arg(2), has("contradicts"), has("relay"), has("bind"))
	case "chain":
		r := p.records[arg(1)]
		if r == nil {
			return "invalid"
		}
		pv := r.Assess(p.anchors[arg(1)], policy)
		if len(pv.Chain) == 0 {
			return "ok -"
		}
		var hops []string
		for k, a := range pv.Chain {
			hops = append(hops, fmt.Sprintf("%d:%s:%s", a.Hop, a.By, pv.Standing[k]))
		}
		return "ok " + strings.Join(hops, " ")
	case "uid":
		r := p.records[arg(1)]
		k, err := strconv.Atoi(arg(2))
		if r == nil || err != nil || k < 0 || k >= len(r.Identity) {
			return "invalid"
		}
		return fmt.Sprintf("ok uid=%d", r.Identity[k].UID)
	case "author", "relay":
		r := p.records[arg(1)]
		if r == nil {
			return "invalid"
		}
		pv := r.Assess(p.anchors[arg(1)], policy)
		a, ok := pv.Author()
		if f[0] == "relay" {
			a, ok = pv.Relay()
		}
		if !ok {
			return "no-answer unattributed"
		}
		return fmt.Sprintf("ok by=%s hop=%d", a.By, a.Hop)
	case "disputed":
		r := p.records[arg(1)]
		if r == nil {
			return "invalid"
		}
		if r.Assess(p.anchors[arg(1)], policy).Disputed() {
			return "ok disputed=yes"
		}
		return "ok disputed=no"
	}
	return "unknown-op"
}

// send renders the record as it leaves the process: through the service sink,
// which hands its fallback the record exactly as it would have sent it, and
// then through the encoder and the decoder, which is the wire. The anchor was
// the previous party's and goes.
func (p *replay) send(alias string, r *logging.Record) string {
	var handed logging.Record
	sink := logging.NewServiceSink(filepath.Join(p.work, "nobody-listens.sock"),
		sinkFunc(func(x logging.Record) error { handed = x; return nil }))
	defer sink.Close()
	if err := sink.Write(*r); err != nil {
		return "refused"
	}
	b, err := handed.Encode()
	if err != nil {
		return "invalid"
	}
	back, err := logging.DecodeRecord(bytes.TrimSuffix(b, []byte("\n")))
	if err != nil {
		return "refused"
	}
	*r = back
	delete(p.anchors, alias)
	return "ok"
}

// viaHandler runs one grouped attribute through the slog handler, because the
// handler is where a group flattens; a driver that flattened it itself would
// be testing the driver.
func (p *replay) viaHandler(r *logging.Record, group, key, value string) string {
	var got logging.Record
	h := logging.NewHandler(sinkFunc(func(x logging.Record) error { got = x; return nil }), nil).WithGroup(group)
	rec := slog.NewRecord(time.Unix(0, 0), slog.LevelInfo, "x", 0)
	rec.AddAttrs(slog.String(key, value))
	if err := h.Handle(context.Background(), rec); err != nil {
		return "invalid"
	}
	if r.Attrs == nil {
		r.Attrs = map[string]string{}
	}
	for k, v := range got.Attrs {
		r.Attrs[k] = v
	}
	return "ok"
}

type sinkFunc func(logging.Record) error

func (f sinkFunc) Write(r logging.Record) error { return f(r) }

func shape(r logging.Record) string {
	b, err := r.Encode()
	if err != nil {
		return "invalid"
	}
	end := "none"
	if bytes.HasSuffix(b, []byte("\n")) {
		end = "lf"
	}
	lines := bytes.Count(b, []byte("\n"))
	var wire struct {
		Schema int    `json:"schema"`
		Level  int    `json:"level"`
		Time   string `json:"time"`
	}
	if err := json.Unmarshal(bytes.TrimSuffix(b, []byte("\n")), &wire); err != nil {
		return "invalid"
	}
	frac := 0
	zone := "-"
	if i := strings.LastIndex(wire.Time, "."); i >= 0 {
		rest := wire.Time[i+1:]
		for frac < len(rest) && rest[frac] >= '0' && rest[frac] <= '9' {
			frac++
		}
		if frac < len(rest) {
			zone = rest[frac:]
		}
	}
	return fmt.Sprintf("ok schema=%d level=%d frac=%d zone=%s lines=%d end=%s", wire.Schema, wire.Level, frac, zone, lines, end)
}

func tier(s logging.Sink) string {
	switch s.(type) {
	case *logging.ServiceSink:
		return "service"
	case *logging.FileSink:
		return "file"
	}
	return "discard"
}

func (p *replay) machine(what string) string {
	p.stopServer()
	os.Unsetenv(logging.EnvSink)
	os.Unsetenv(logging.EnvService)
	p.n++
	parts := strings.Split(what, "+")
	for _, part := range parts {
		switch part {
		case "none":
		case "file":
			os.Setenv(logging.EnvSink, filepath.Join(p.work, fmt.Sprintf("m%d.jsonl", p.n)))
		case "unwritable":
			blocker := filepath.Join(p.work, fmt.Sprintf("blocker%d", p.n))
			os.WriteFile(blocker, nil, 0o644)
			os.Setenv(logging.EnvSink, filepath.Join(blocker, "x.jsonl"))
		case "service":
			addr := filepath.Join(p.work, fmt.Sprintf("s%d.sock", p.n))
			sv := &served{ch: make(chan logging.Record, 64)}
			srv := &logging.Server{Out: func(logging.Attestation) logging.Sink { return sv }}
			if err := srv.Listen(addr); err != nil {
				return "invalid"
			}
			go srv.Serve()
			p.server, p.current = srv, sv
			os.Setenv(logging.EnvService, addr)
		case "deaf":
			os.Setenv(logging.EnvService, filepath.Join(p.work, fmt.Sprintf("deaf%d.sock", p.n)))
		default:
			return "invalid"
		}
	}
	return "ok"
}

func (p *replay) stopServer() {
	if p.server != nil {
		p.server.Close()
		p.server = nil
	}
}

func (p *replay) write(st *sinkState, r logging.Record) string {
	switch s := st.sink.(type) {
	case *logging.ServiceSink:
		fb := s.Fallback.(*counting)
		before := fb.n
		if err := s.Write(r); err != nil {
			return "refused"
		}
		if fb.n > before {
			return "ok where=" + tier(fb.inner)
		}
		if st.served == nil {
			return "ok where=discard"
		}
		// The socket write succeeded, so the service will deliver; the deadline
		// only bounds a service that died between the write and the read.
		select {
		case <-st.served.ch:
			st.served.lines++
			return "ok where=service"
		case <-time.After(5 * time.Second):
			return "refused"
		}
	case *logging.FileSink:
		if err := s.Write(r); err != nil {
			return "refused"
		}
		return "ok where=file"
	}
	if err := st.sink.Write(r); err != nil {
		return "refused"
	}
	return "ok where=discard"
}

func (p *replay) held(st *sinkState) string {
	fs, ok := st.sink.(*logging.FileSink)
	if !ok {
		n := 0
		if st.served != nil {
			n = st.served.lines
		}
		return fmt.Sprintf("ok lines=%d over=0 marked=0 torn=0", n)
	}
	b, err := os.ReadFile(fs.Path())
	if err != nil {
		return "ok lines=0 over=0 marked=0 torn=0"
	}
	lines, over, marked, torn := 0, 0, 0, 0
	for _, line := range bytes.Split(bytes.TrimSuffix(b, []byte("\n")), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		lines++
		if len(line)+1 > fs.MaxLine {
			over++
		}
		if bytes.Contains(line, []byte(`"logging.truncated":"true"`)) {
			marked++
		}
		// No scenario writes a replacement character, so one in the message means
		// a character was split. The check is on the decoded message rather than
		// the bytes because an encoder may escape what it substituted, and on the
		// bytes as well because another may write the broken ones as they are.
		if !utf8.Valid(line) {
			torn++
		} else if rec, err := logging.DecodeRecord(line); err == nil && strings.ContainsRune(rec.Msg, utf8.RuneError) {
			torn++
		}
	}
	return fmt.Sprintf("ok lines=%d over=%d marked=%d torn=%d", lines, over, marked, torn)
}

// attest applies what a receiving service does when the platform's attester
// answered with that mechanism. The subject of hop 1 is the writer; the
// subject of any later hop is a relay, which is a different process.
func (p *replay) attest(alias string, r *logging.Record, by string, contradicts, relay, bind bool) string {
	srv := &logging.Server{}
	if bind {
		srv.KeyID, srv.Key = "k1", operatorKey
	}
	if by == "none" {
		*r = srv.Accept(*r, logging.Attestation{}, errors.New("benched: the platform's attester answered nothing"))
		delete(p.anchors, alias)
		return "ok"
	}
	claim, _ := r.Identity.Claimed()
	hop := len(r.Identity)
	peer := logging.Attestation{By: by, Verified: true, Host: claim.Host, User: "w",
		Exe: "/usr/bin/w", UID: 1000, GID: 1000, PID: claim.PID}
	if hop > 1 {
		peer.Exe, peer.PID = "/usr/bin/forwarder", claim.PID+hop
	}
	if relay {
		peer.Exe, peer.UID = "/usr/bin/relay", 0
	}
	if contradicts {
		peer.Host, peer.PID = "elsewhere", claim.PID+1
	}
	*r = srv.Accept(*r, peer, nil)
	anchor := r.Identity[len(r.Identity)-1]
	p.anchors[alias] = &anchor
	return "ok"
}
