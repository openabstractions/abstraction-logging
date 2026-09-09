package logging

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// EnvService points at a logging service to forward to. A local socket path on
// Linux/macOS; on Windows an AF_UNIX path works too, with the caveat in
// PeerOf.
const EnvService = "ABSTRACTION_LOG_SERVICE"

// ServiceSink forwards records to a service over a local socket.
//
// The socket is the point, and a file would not do. When a record arrives this
// way the receiving end can ask the KERNEL who sent it, and get an answer the
// sender cannot influence. That is the difference between logs that can be
// attributed and logs that can be separated.
//
// One record per write, newline framed, same bytes as the file sink. A service
// that dies mid-stream loses at most the line in flight, and a reader tailing
// the service's own output cannot tell the two sinks apart — which is what makes
// the tiers substitutable.
type ServiceSink struct {
	addr string

	mu   sync.Mutex
	conn net.Conn

	// Fallback receives records when the service is unreachable. Logging must
	// not fail the caller and must not block it, so an absent service degrades
	// to the next tier rather than to an error.
	Fallback Sink

	// Timeout bounds a single write. A wedged service must not stall the program
	// that is logging to it — the most likely moment to be logging heavily is
	// the moment something is already going wrong.
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
	// A client sends its claim and nothing else. Stripping any verified
	// attestation it tried to include is what stops the mechanism being theatre:
	// otherwise the sender would be asserting its own identity again, in a field
	// labelled as though someone else had checked.
	r.Identity = stripVerified(r.Identity)

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
		// One reconnect, then give up to the fallback. The service restarting
		// is ordinary; retrying forever inside a caller's log statement is not.
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

// Auto picks the best sink this machine actually has, and it is the same
// delegation chain the download layer uses:
//
//	a service, if one is configured and reachable   → identity is a fact
//	a file, if one is configured                    → identity is filesystem ownership
//	nothing                                         → a working no-op
//
// Nothing above this call chooses. An application says "log", and where that
// goes is a property of the machine it is running on — the same argument that
// says a caller does not get to pick the NAS.
func Auto(program string) Sink {
	file := FromEnv()
	if addr := os.Getenv(EnvService); addr != "" {
		return NewServiceSink(addr, file)
	}
	return file
}

// Server is the receiving half: it accepts connections, asks the kernel who is
// on the other end, and APPENDS that to the identity chain of every record from
// that connection.
//
// Appends, not overwrites. What the client claimed stays on the record even when
// the kernel contradicts it, because the contradiction is the interesting signal
// and this is not the layer with enough context to adjudicate it. What the
// client is not allowed to do is arrive with a verified attestation already
// attached — those are stripped, because only the party that ran a check may
// record that the check passed.
type Server struct {
	// Out receives attested records, and is handed what the kernel said so it can
	// route on a fact. Give it a per-user sink to get real separation rather than
	// a shared file with better labels.
	Out func(Attestation) Sink

	ln net.Listener
	wg sync.WaitGroup
}

// Listen starts a server on a unix socket. The socket file is removed first: a
// stale socket from a killed process refuses bind, and refusing to start because
// of the corpse of a previous run is not useful behaviour for a logging daemon.
func (s *Server) Listen(addr string) error {
	os.Remove(addr)
	ln, err := net.Listen("unix", addr)
	if err != nil {
		return err
	}
	// Anyone may connect. Separation comes from what the kernel reports about
	// each connection, not from who is allowed to open one — the whole design
	// assumes untrusted local callers.
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
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer c.Close()
			s.handle(c, peer, perr)
		}()
	}
}

func (s *Server) Close() error {
	err := s.ln.Close()
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
		// Append, never replace. The claim stays on the record even when it is
		// contradicted, because a lie is evidence and this is not the layer with
		// enough context to adjudicate it.
		rec.Identity = stripVerified(rec.Identity)
		if perr == nil {
			stamp := peer
			// Hop counts from the emitter, so a relay chain reads in order.
			stamp.Hop = len(rec.Identity)
			rec.Identity = append(rec.Identity, stamp)
		} else {
			// The kernel would not say. Record that explicitly rather than leaving
			// the line looking unattributed for an unknown reason.
			if rec.Attrs == nil {
				rec.Attrs = map[string]string{}
			}
			rec.Attrs["logging.peer_error"] = perr.Error()
		}
		out.Write(rec)
	}
}

func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

var errNoPeerCreds = fmt.Errorf("logging: peer credentials are not available on this platform")

// stripVerified removes attestations a sender is not entitled to make.
//
// Only the party that ran a check may record that the check passed. Without
// this, "verified" would mean "the sender wrote true", and the field would be
// worse than useless — it would look like assurance.
func stripVerified(id Identity) Identity {
	out := id[:0:0]
	for _, a := range id {
		if a.Verified {
			continue
		}
		out = append(out, a)
	}
	return out
}
