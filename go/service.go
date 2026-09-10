package logging

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// EnvService points at a logging service to forward to: a local socket path.
const EnvService = "ABSTRACTION_LOG_SERVICE"

// ServiceSink forwards records to a service over a local socket.
//
// The socket is the point: the receiving end can ask the kernel who sent a
// record and get an answer the sender cannot influence. That is the difference
// between logs that can be attributed and logs that can be separated.
//
// One record per write, newline framed, the same bytes as the file sink, and
// the chain goes as it is. A sink cannot tell a stamp its own service made
// from one a hostile writer attached; the receiver can, and does.
type ServiceSink struct {
	addr string

	mu   sync.Mutex
	conn net.Conn

	// Fallback receives records when the service is unreachable. Logging must
	// not fail the caller and must not block it.
	Fallback Sink

	// Timeout bounds a single write. A wedged service must not stall the
	// program logging to it — the moment a program is logging heavily is the
	// moment something is already going wrong.
	Timeout time.Duration
}

func NewServiceSink(addr string, fallback Sink) *ServiceSink {
	if fallback == nil {
		fallback = DiscardSink{}
	}
	return &ServiceSink{addr: addr, Fallback: fallback, Timeout: 2 * time.Second}
}

func (s *ServiceSink) Separation() Separation { return SeparationPeer }

func (s *ServiceSink) Write(r Record) error {
	b, err := r.Encode()
	if err != nil {
		return err
	}
	if err := s.send(b); err != nil {
		return s.Fallback.Write(r)
	}
	return nil
}

func (s *ServiceSink) send(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		c, err := net.DialTimeout("unix", s.addr, s.timeout())
		if err != nil {
			return err
		}
		s.conn = c
	}
	s.conn.SetWriteDeadline(time.Now().Add(s.timeout()))
	if _, err := s.conn.Write(b); err != nil {
		// One reconnect, then the fallback. A service restarting is ordinary;
		// retrying forever inside a caller's log statement is not.
		s.conn.Close()
		s.conn = nil
		return err
	}
	return nil
}

func (s *ServiceSink) timeout() time.Duration {
	if s.Timeout <= 0 {
		return 2 * time.Second
	}
	return s.Timeout
}

func (s *ServiceSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		return err
	}
	return nil
}

// Auto picks the best sink this machine has: a service if one is configured,
// a file if one is configured, else a working no-op. Nothing above this call
// chooses; where a record goes is a property of the machine.
func Auto(program string) Sink {
	file := FromEnv()
	if addr := os.Getenv(EnvService); addr != "" {
		return NewServiceSink(addr, file)
	}
	return file
}

// Server is the receiving half: it accepts connections, asks the kernel who is
// on the other end, and appends that to the chain of every record from that
// connection. What arrived stays on the record, a forged stamp included; what
// it is worth is for Record.Assess, with the stamp this server appended as
// the anchor.
type Server struct {
	// Out receives attested records and is handed what the kernel said, so it
	// can route on a fact. A per-user sink here is real separation.
	Out func(Attestation) Sink

	// KeyID and Key, when set, bind every stamp this server appends to the
	// record it stamped, so a reader holding the key can verify the stamp
	// wherever the record went afterwards.
	KeyID string
	Key   []byte

	ln    net.Listener
	wg    sync.WaitGroup
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// Listen starts a server on a unix socket. A stale socket file from a killed
// process is removed first; refusing to start over a corpse is not useful
// behaviour for a logging daemon.
func (s *Server) Listen(addr string) error {
	os.Remove(addr)
	ln, err := net.Listen("unix", addr)
	if err != nil {
		return err
	}
	// Anyone may connect. Separation comes from what the kernel reports about
	// each connection, not from who may open one.
	os.Chmod(addr, 0o777)
	s.ln = ln
	return nil
}

func (s *Server) Serve() error {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return err
		}
		peer, perr := PeerOf(c)
		s.track(c, true)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.track(c, false)
			defer c.Close()
			s.handle(c, peer, perr)
		}()
	}
}

func (s *Server) track(c net.Conn, on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conns == nil {
		s.conns = map[net.Conn]struct{}{}
	}
	if on {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

// Close stops listening and closes every connection it accepted. A client
// that keeps its connection open is the normal case, so waiting for it to
// hang up would be waiting for nothing.
func (s *Server) Close() error {
	err := s.ln.Close()
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

func (s *Server) handle(c net.Conn, peer Attestation, perr error) {
	out := s.Out(peer)
	dec := newLineReader(c)
	for {
		line, err := dec.next()
		if err != nil {
			return
		}
		rec, err := DecodeRecord(line)
		if err != nil {
			continue // one malformed line must not drop a connection
		}
		out.Write(s.Accept(rec, peer, perr))
	}
}

// Accept applies what this service does to one record: append what the
// platform established about the immediate peer as a new hop, or record why
// it could not. Nothing is removed and nothing is rewritten. The stamp is the
// last hop of the result, and is the anchor to assess the record from.
func (s *Server) Accept(rec Record, peer Attestation, perr error) Record {
	if perr != nil {
		if rec.Attrs == nil {
			rec.Attrs = map[string]string{}
		}
		rec.Attrs["logging.peer_error"] = perr.Error()
		return rec
	}
	stamp := peer
	stamp.Hop = len(rec.Identity)
	rec.Identity = append(append(Identity(nil), rec.Identity...), stamp)
	if s.Key != nil {
		rec.Bind(stamp.Hop, s.KeyID, s.Key)
	}
	return rec
}

func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

var errNoPeerCreds = fmt.Errorf("logging: peer credentials are not available on this platform")
