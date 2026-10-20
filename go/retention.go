package logging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	watch "github.com/openabstractions/abstraction-watch/go"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"github.com/openabstractions/abstraction-logging/go/internal/sinkclock"
)

// LogsRetention is the legacy native logs_max_use and logs_max_retention policy.
// MaxUse bounds the active file plus retained JSONL segments, in bytes. Zero
// MaxRetention leaves age unset. Configuration precedence and text parsing are
// owned by config; this native provider accepts their resolved values.
type LogsRetention struct {
	MaxUse       int64
	MaxRetention time.Duration
}

// ResolvedLogsRetention accepts config-owned, fully resolved limits. A nil
// limit is known unlimited; a non-nil zero is a bound of zero. Values are
// copied when installed, so later caller mutations do not alter the policy.
// Config owns defaults, parsing, precedence and locks.
type ResolvedLogsRetention struct {
	MaxUse       *int64
	MaxRetention *time.Duration
}

func (p LogsRetention) resolved() (ResolvedLogsRetention, error) {
	if p.MaxUse <= 0 || p.MaxRetention < 0 {
		return ResolvedLogsRetention{}, ErrRetentionPolicy
	}
	out := ResolvedLogsRetention{MaxUse: &p.MaxUse}
	if p.MaxRetention > 0 {
		out.MaxRetention = &p.MaxRetention
	}
	return out, nil
}

func (p ResolvedLogsRetention) values() (LogsRetention, error) {
	var out LogsRetention
	if p.MaxUse != nil {
		if *p.MaxUse < 0 {
			return out, ErrRetentionPolicy
		}
		out.MaxUse = *p.MaxUse
	}
	if p.MaxRetention != nil {
		if *p.MaxRetention < 0 {
			return out, ErrRetentionPolicy
		}
		out.MaxRetention = *p.MaxRetention
	}
	return out, nil
}

var (
	ErrRetentionPolicy        = errors.New("logging: invalid retention policy")
	ErrRetentionOwner         = errors.New("logging: retained history already has a writer")
	ErrRetentionRecord        = errors.New("logging: record exceeds logs_max_use")
	errRetainedHistoryChanged = errors.New("logging: retained history changed outside provider")
)

const maxLogSegment = int64(128 << 20)

// DefaultLogsRetention resolves the accepted default on path's filesystem:
// 10 percent of total capacity, capped at 4 GiB, with age unset.
// The containing directory must already exist.
func DefaultLogsRetention(path string) (LogsRetention, error) {
	total, err := logFilesystemBytes(filepath.Dir(path))
	if err != nil {
		return LogsRetention{}, err
	}
	return defaultLogsRetention(total)
}

func defaultLogsRetention(total uint64) (LogsRetention, error) {
	bytes := min(total/10, uint64(4<<30))
	if bytes == 0 {
		return LogsRetention{}, ErrRetentionPolicy
	}
	return LogsRetention{MaxUse: int64(bytes)}, nil
}

// OpenDefaultRetainedFileSink opens a service-owned sink with storage's
// accepted default policy. Config's next-version keys can replace the native
// values through SetRetention without changing a logging service contract.
func OpenDefaultRetainedFileSink(path string) (*FileSink, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	policy, err := DefaultLogsRetention(path)
	if err != nil {
		return nil, err
	}
	return OpenRetainedFileSink(path, policy)
}

type historySegment struct {
	name string
	base int64
	info os.FileInfo
	// key is the segment's file identity, read from a handle on it by the
	// identity owner (fileid.FileKeyOf); readers compare it with their own.
	key string
}

type fileRetention struct {
	policy              LogsRetention
	maxUseBounded       bool
	maxRetentionBounded bool
	root                *os.Root
	archive             *os.Root
	lock                *os.File
	name                string
	segments            []historySegment
	base                int64
	now                 func() time.Time
}

// OpenRetainedFileSink opens a service-owned, single-writer history. The active
// file stays at path; completed segments live in path+".history". The lifetime
// OS lock prevents another retained provider from rotating the same history.
// Use OpenFileSink for shared append-only files. External writers and edits are
// outside this provider's lifecycle.
func OpenRetainedFileSink(path string, policy LogsRetention) (*FileSink, error) {
	resolved, err := policy.resolved()
	if err != nil {
		return nil, err
	}
	return OpenResolvedRetainedFileSink(path, resolved)
}

// OpenResolvedRetainedFileSink opens the same service-owned retained history
// with explicit bounded or unlimited byte and age limits.
func OpenResolvedRetainedFileSink(path string, limits ResolvedLogsRetention) (*FileSink, error) {
	policy, err := limits.values()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	r := &fileRetention{policy: policy, maxUseBounded: limits.MaxUse != nil, maxRetentionBounded: limits.MaxRetention != nil, root: root, name: filepath.Base(path), now: sinkclock.Current}
	s := &FileSink{path: path, MaxLine: DefaultMaxLine, retention: r, historyGate: watch.NewGate(watch.DefaultWaiters)}
	fail := func(err error) (*FileSink, error) {
		return nil, errors.Join(err, s.Close())
	}
	archiveName := r.name + ".history"
	if err := root.Mkdir(archiveName, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return fail(err)
	}
	info, err := root.Lstat(archiveName)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fail(fmt.Errorf("logging: history directory must be a directory: %w", errors.Join(err, ErrRetentionPolicy)))
	}
	r.archive, err = root.OpenRoot(archiveName)
	if err != nil {
		return fail(err)
	}
	if info, err = r.archive.Lstat("writer.lock"); err == nil && !info.Mode().IsRegular() {
		return fail(fmt.Errorf("logging: history lock must be a regular file"))
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fail(err)
	}
	r.lock, err = r.archive.OpenFile("writer.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fail(err)
	}
	if err = lockRetainedHistory(r.lock); err != nil {
		return fail(err)
	}
	entries, err := fs.ReadDir(r.archive.FS(), ".")
	if err != nil {
		return fail(err)
	}
	for _, entry := range entries {
		base, ok := segmentBase(entry.Name())
		if !ok {
			continue
		}
		info, err := r.archive.Lstat(entry.Name())
		if err != nil || !info.Mode().IsRegular() {
			return fail(fmt.Errorf("logging: history segment must be a regular file: %w", errors.Join(err, ErrRetentionPolicy)))
		}
		if info.Size() == 0 || base > (1<<63-1)-info.Size() {
			return fail(fmt.Errorf("logging: invalid history segment %s", entry.Name()))
		}
		key, err := entryKey(r.archive, entry.Name())
		if err != nil {
			return fail(err)
		}
		r.segments = append(r.segments, historySegment{entry.Name(), base, info, key})
	}
	sort.Slice(r.segments, func(i, j int) bool { return r.segments[i].base < r.segments[j].base })
	for _, segment := range r.segments {
		if segment.base < r.base {
			return fail(fmt.Errorf("logging: overlapping history segments"))
		}
		r.base = segment.base + segment.info.Size()
	}
	if err = s.openRetainedActiveLocked(); err != nil {
		return fail(err)
	}
	if err = s.collectRetainedLocked(0); err != nil {
		return fail(err)
	}
	// Instants continue from the last one written before this Open, so a
	// restart with a slower clock cannot append lower instants (LOG-P9).
	s.lastAt = s.lastInstantLocked()
	return s, nil
}

// lastInstantLocked reads the sink instant of the newest row that has one,
// searching back through the newest nonempty segments' tails, one read each.
func (s *FileSink) lastInstantLocked() time.Time {
	segments, outcome := s.historySegmentsLocked()
	if outcome != wire.PageOutcomePage {
		return time.Time{}
	}
	for i := len(segments) - 1; i >= 0; i-- {
		size := segments[i].info.Size()
		if size == 0 {
			continue
		}
		f, err := s.openHistorySegmentLocked(segments[i], i == len(segments)-1)
		if err != nil {
			return time.Time{}
		}
		tail := min(size, int64(MaxHistoryScanBytes)+1)
		buf := make([]byte, tail)
		n, _ := f.ReadAt(buf, size-tail)
		f.Close()
		// The newest row that has an instant: a row without one, from an
		// earlier runtime, may end the tail.
		buf = bytes.TrimSuffix(buf[:n], []byte{'\n'})
		for len(buf) > 0 {
			start := bytes.LastIndexByte(buf, '\n') + 1
			if at, ok := storedAt(buf[start:]); ok {
				return at
			}
			if start == 0 {
				break
			}
			buf = buf[:start-1]
		}
	}
	return time.Time{}
}

func segmentBase(name string) (int64, bool) {
	digits, ok := strings.CutSuffix(name, ".jsonl")
	if !ok || len(digits) != 20 {
		return 0, false
	}
	base, err := strconv.ParseInt(digits, 10, 64)
	return base, err == nil && base >= 0 && fmt.Sprintf("%020d", base) == digits
}

func (r *fileRetention) close() error {
	var err error
	if r.lock != nil {
		err = errors.Join(err, r.lock.Close())
		r.lock = nil
	}
	if r.archive != nil {
		err = errors.Join(err, r.archive.Close())
		r.archive = nil
	}
	if r.root != nil {
		err = errors.Join(err, r.root.Close())
		r.root = nil
	}
	return err
}

func (s *FileSink) openRetainedActiveLocked() error {
	r := s.retention
	info, err := r.root.Lstat(r.name)
	if err == nil && !info.Mode().IsRegular() {
		return fmt.Errorf("logging: active history must be a regular file")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.f, err = r.root.OpenFile(r.name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	return err
}

// Collect enforces a service-owned sink's age and size policy, including while
// idle. The runtime collector can call it on its daily/pressure cadence. Open,
// Write and Read also enforce the policy. It never removes the active file or
// scans other storage classes. Failures are returned to the host operator.
func (s *FileSink) Collect() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.historyClosed {
		return os.ErrClosed
	}
	if s.retention == nil {
		return nil
	}
	return s.collectRetainedLocked(0)
}

// SetRetention installs resolved policy from the owning config consumer.
// A collection failure is reported and the tighter policy remains in force:
// later writes must satisfy it or return an error.
func (s *FileSink) SetRetention(policy LogsRetention) error {
	resolved, err := policy.resolved()
	if err != nil {
		return err
	}
	return s.SetResolvedRetention(resolved)
}

// SetResolvedRetention installs config-owned limits in this retained history.
// A collection failure is reported while the installed policy remains in force.
func (s *FileSink) SetResolvedRetention(limits ResolvedLogsRetention) error {
	policy, err := limits.values()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retention == nil {
		return ErrRetentionPolicy
	}
	if s.historyClosed {
		return os.ErrClosed
	}
	s.retention.policy = policy
	s.retention.maxUseBounded = limits.MaxUse != nil
	s.retention.maxRetentionBounded = limits.MaxRetention != nil
	return s.collectRetainedLocked(0)
}

// MaintainRetention supplies the logs step on the accepted daily collector
// cadence. Open/read/write also enforce limits; hosts can call Collect on
// storage pressure. The returned function joins the worker after cancellation.
func (s *FileSink) MaintainRetention(ctx context.Context, report func(error)) func() {
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTicker(24 * time.Hour)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if err := s.Collect(); err != nil && report != nil {
					report(err)
				}
			}
		}
	}()
	return func() { <-done }
}

func (s *FileSink) prepareRetainedWriteLocked(bytes int64) error {
	r := s.retention
	// A resolved zero limit is keep-nothing: the append still completes, then
	// the same collector removes it. Other policies retain size admission.
	if r.keepNothing() {
		return s.collectRetainedLocked(0)
	}
	if r.maxUseBounded && bytes > r.policy.MaxUse {
		return ErrRetentionRecord
	}
	return s.collectRetainedLocked(bytes)
}

func (r *fileRetention) keepNothing() bool {
	return (r.maxUseBounded && r.policy.MaxUse == 0) || (r.maxRetentionBounded && r.policy.MaxRetention == 0)
}

// activeRetainedInfoLocked reads the active file's FileInfo and file key from
// the writer's handle and checks that the active name still holds that file.
func (s *FileSink) activeRetainedInfoLocked() (os.FileInfo, string, error) {
	if s.f == nil {
		return nil, "", os.ErrClosed
	}
	info, key, err := heldSegment(s.f)
	if err != nil {
		return nil, "", err
	}
	pathInfo, err := s.retention.root.Lstat(s.retention.name)
	if err != nil {
		return nil, "", err
	}
	if !pathInfo.Mode().IsRegular() {
		return nil, "", errRetainedHistoryChanged
	}
	pathKey, err := entryKey(s.retention.root, s.retention.name)
	if err != nil {
		return nil, "", err
	}
	if pathKey != key {
		return nil, "", errRetainedHistoryChanged
	}
	return info, key, nil
}

func (s *FileSink) collectRetainedLocked(incoming int64) error {
	r := s.retention
	info, key, err := s.activeRetainedInfoLocked()
	if err != nil {
		return err
	}
	if info.Size() > (1<<63-1)-incoming || r.base > (1<<63-1)-info.Size()-incoming {
		return fmt.Errorf("logging: history offset overflow")
	}
	now := r.now()
	// Rotation size is an internal partition, never another policy key. Small
	// limits still accept one complete record that fits MaxUse.
	segmentLimit := maxLogSegment
	if r.maxUseBounded {
		segmentLimit = min(segmentLimit, max(int64(1), r.policy.MaxUse/8))
	}
	writeMonth := info.ModTime().UTC().Format("2006-01")
	old := r.maxRetentionBounded && (r.policy.MaxRetention == 0 || !info.ModTime().Add(r.policy.MaxRetention).After(now))
	if info.Size() > 0 && ((r.maxUseBounded && r.policy.MaxUse == 0) || info.Size() > segmentLimit-incoming || writeMonth != now.UTC().Format("2006-01") || old) {
		if err := s.rotateRetainedLocked(info, key); err != nil {
			return err
		}
		info, _, err = s.activeRetainedInfoLocked()
		if err != nil {
			return err
		}
	}
	used := info.Size() + incoming
	for _, segment := range r.segments {
		if used > (1<<63-1)-segment.info.Size() {
			return fmt.Errorf("logging: history byte count overflow")
		}
		used += segment.info.Size()
	}
	// Age checks each segment: a clock adjustment must not let a newer-looking
	// old segment shield expired segments written after it.
	for i := 0; i < len(r.segments); {
		segment := r.segments[i]
		if r.maxRetentionBounded && (r.policy.MaxRetention == 0 || !segment.info.ModTime().Add(r.policy.MaxRetention).After(now)) {
			if err := s.removeRetainedSegmentLocked(i); err != nil {
				return err
			}
			used -= segment.info.Size()
		} else {
			i++
		}
	}
	for r.maxUseBounded && used > r.policy.MaxUse && len(r.segments) > 0 {
		bytes := r.segments[0].info.Size()
		if err := s.removeRetainedSegmentLocked(0); err != nil {
			return err
		}
		used -= bytes
	}
	if r.maxUseBounded && used > r.policy.MaxUse {
		return ErrRetentionRecord
	}
	return nil
}

func (s *FileSink) removeRetainedSegmentLocked(index int) error {
	r := s.retention
	segment := r.segments[index]
	current, err := r.archive.Lstat(segment.name)
	if err != nil {
		return err
	}
	if !current.Mode().IsRegular() || current.Size() != segment.info.Size() {
		return errRetainedHistoryChanged
	}
	if key, err := entryKey(r.archive, segment.name); err != nil {
		return err
	} else if key != segment.key {
		return errRetainedHistoryChanged
	}
	if err := r.archive.Remove(segment.name); err != nil {
		return fmt.Errorf("logging: remove expired history: %w", err)
	}
	r.segments = append(r.segments[:index], r.segments[index+1:]...)
	s.notifyHistoryLocked()
	return nil
}

// rotateRetainedLocked seals the active file, whose FileInfo and file key the
// caller read from the writer's handle; a rename keeps the key.
func (s *FileSink) rotateRetainedLocked(info os.FileInfo, key string) error {
	r := s.retention
	if r.base > (1<<63-1)-info.Size() {
		return fmt.Errorf("logging: history offset overflow")
	}
	name := fmt.Sprintf("%020d.jsonl", r.base)
	if _, err := r.archive.Lstat(name); err == nil {
		return fmt.Errorf("logging: history segment already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := s.f.Close(); err != nil {
		s.f = nil
		return err
	}
	s.f = nil
	if err := r.root.Rename(r.name, filepath.Join(r.name+".history", name)); err != nil {
		return errors.Join(err, s.openRetainedActiveLocked())
	}
	// The sealed file's own modification time, read after the rename, is the
	// segment's last write for the seek (LOG-P9); an open handle's view can lag.
	if sealed, err := r.archive.Lstat(name); err == nil && sealed.Mode().IsRegular() && sealed.Size() == info.Size() {
		info = sealed
	}
	r.segments = append(r.segments, historySegment{name, r.base, info, key})
	r.base += info.Size()
	// A crash after rename leaves a completed segment and no active file;
	// Open reconstructs the offset and creates a fresh active file.
	return s.openRetainedActiveLocked()
}

func (s *FileSink) notifyHistoryLocked() {
	if !s.historyClosed {
		s.historyGate.Advance()
	}
}
