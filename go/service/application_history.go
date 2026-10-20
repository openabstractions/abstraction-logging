package service

import (
	"crypto/rand"
	cursorcodec "github.com/openabstractions/abstraction-watch/go/cursor"
	"time"

	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"errors"
	watch "github.com/openabstractions/abstraction-watch/go"

	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// ApplicationHistoryQuery is receiving evidence, never a serialized grant.
// Check admits Selection and evaluates all Emitters in one final Rights snapshot.
type ApplicationHistoryQuery struct {
	Selection wire.ApplicationLogSelection
	Target    wire.ApplicationLogResource
	Emitters  []wire.ApplicationLogResource
	// AboutCaller admits ReadAboutCaller under caller-log@1. Selection,
	// Target and Emitters are then unused: records are chosen by the private
	// subject stamp, not by emitter.
	AboutCaller bool
}
type ApplicationHistoryPolicy struct {
	Check func(context.Context, *identity.Peer, ApplicationHistoryQuery) ([]bool, error)
	// Fits budgets the complete encoded shared batch/frame, including caller and
	// selection admission. It uses the same builder as Check.
	Fits func(*identity.Peer, ApplicationHistoryQuery) bool
}
type applicationHistoryProvider interface {
	ApplicationHistoryAvailable() bool
	ScanApplicationHistory(string) (logging.HistoryScan, error)
	HistoryGate() *watch.Gate
}

// historySeeker is the owned provider's positioning by record time.
type historySeeker interface {
	SeekApplicationHistory(time.Time) (string, wire.PageOutcome)
}

type applicationCursor struct {
	Version                                 int
	Account, Caller, InputProgram, Physical string
	Selection                               wire.ApplicationLogSelection
	Target                                  wire.ApplicationLogResource
	AboutCaller                             bool   `json:",omitempty"`
	Since                                   string `json:",omitempty"`
}

const maxApplicationCursor = 128 << 10

func newApplicationSeal() (cipher.AEAD, error) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
func (h *Host) EnableApplicationHistoryPolicy(p ApplicationHistoryPolicy) error {
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	if h.serving || h.ctx.Err() != nil {
		return errors.New("logging service: configure application history before Serve")
	}
	if p.Check == nil || p.Fits == nil {
		return errors.New("logging service: complete application policy required")
	}
	h.applicationPolicy = p
	return nil
}
func (h *Host) ApplicationHistoryAvailable() bool {
	p, ok := h.out.(applicationHistoryProvider)
	return ok && p.ApplicationHistoryAvailable() && h.applicationPolicy.Check != nil && h.applicationPolicy.Fits != nil
}

func (r *receiver) applicationPeer() (*identity.Peer, string, string, error) {
	peer, err := r.call.Peer()
	if err != nil {
		return nil, "", "", &wire.ServiceError{Code: wire.ServiceErrorCodeCallerUnavailable, Message: "caller identity unavailable"}
	}
	account, program, err := applicationSubject(peer, r.owner)
	if err != nil {
		return nil, "", "", err
	}
	return peer, account, program, nil
}

// policyRefusal carries a policy decision through the shared waiter.
type policyRefusal struct{ err error }

func (p policyRefusal) Error() string { return p.err.Error() }

// applicationSubject names an application history caller and refuses in the
// order the reply reports: an unproven user, then a program SubjectOf cannot
// name, then an account that is not the owner's.
func applicationSubject(peer *identity.Peer, owner string) (string, string, error) {
	if peer == nil {
		return "", "", &wire.ServiceError{Code: wire.ServiceErrorCodeCallerUnavailable, Message: "caller identity unavailable"}
	}
	if _, err := peer.User.AtLeast(listen.Program.User); err != nil {
		return "", "", &wire.ServiceError{Code: wire.ServiceErrorCodeIdentityRequired, Message: "native account required"}
	}
	subject, verdict := identity.AdmitOwner(peer, identity.Need{User: listen.Program.User, Path: listen.Program.Path}, owner)
	switch verdict {
	case identity.OwnerAdmitted:
	case identity.OwnerOtherAccount:
		return "", "", &wire.ServiceError{Code: wire.ServiceErrorCodeWrongUser, Message: "history belongs to another user"}
	case identity.OwnerIdentityRequired:
		return "", "", &wire.ServiceError{Code: wire.ServiceErrorCodeIdentityRequired, Message: "native program required"}
	default:
		return "", "", &wire.ServiceError{Code: wire.ServiceErrorCodeCallerUnavailable, Message: "caller identity unavailable"}
	}
	return subject.Account, subject.Program, nil
}
func appPolicyOutcome(err error) wire.HistoryPageOutcome {
	if errors.Is(err, ErrHistoryPolicyUnavailable) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return wire.HistoryPageOutcomeUnavailable
	}
	return wire.HistoryPageOutcomeNotPermitted
}
func (r *receiver) openApplicationCursor(value string) (applicationCursor, error) {
	var c applicationCursor
	if value == "" || len(value) > maxApplicationCursor || r.applicationSeal == nil {
		return c, errors.New("logging: invalid cursor")
	}
	plain, err := cursorcodec.Unseal(r.applicationSeal, value, []byte(logging.ApplicationLogResourceProfile))
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(plain, &c)
	if err != nil || c.Version != 1 {
		return c, errors.New("logging: invalid cursor")
	}
	return c, nil
}
func (r *receiver) sealApplicationCursor(c applicationCursor) (string, error) {
	plain, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	result, err := cursorcodec.Seal(r.applicationSeal, plain, []byte(logging.ApplicationLogResourceProfile))
	if err != nil {
		return "", err
	}
	if len(result) > maxApplicationCursor {
		return "", errors.New("logging: cursor too large")
	}
	return result, nil
}

type applicationReceiver struct{ *receiver }

func (a *applicationReceiver) Ready() (bool, string) {
	provider, ok := a.out.(applicationHistoryProvider)
	if !ok || !provider.ApplicationHistoryAvailable() {
		return false, "owned retained application history required"
	}
	if a.applicationPolicy.Check == nil || a.applicationPolicy.Fits == nil {
		return false, "typed application policy required"
	}
	return true, ""
}

func (a *applicationReceiver) Read(target wire.ApplicationLogTarget, cursor string, maxRecords, maxBytes int64) (wire.ApplicationLogPage, error) {
	return a.applicationRead(target, cursor, maxRecords, maxBytes, 0, false)
}

// ReadAboutCaller returns only rows whose private subject stamp names the proven
// caller (LOG-P9). The bound caller is the subject; no argument names one.
func (a *applicationReceiver) ReadAboutCaller(since, cursor string, maxRecords, maxBytes int64) (wire.ApplicationLogPage, error) {
	return a.applicationReadAbout(&since, wire.ApplicationLogTarget{}, cursor, maxRecords, maxBytes, 0, false)
}
func (a *applicationReceiver) Observe(target wire.ApplicationLogTarget, cursor string, maxRecords, maxBytes, waitMS int64) (wire.ApplicationLogPage, error) {
	return a.applicationRead(target, cursor, maxRecords, maxBytes, waitMS, true)
}

func (r *receiver) applicationRead(target wire.ApplicationLogTarget, cursor string, maxRecords, maxBytes, waitMS int64, observe bool) (wire.ApplicationLogPage, error) {
	return r.applicationReadAbout(nil, target, cursor, maxRecords, maxBytes, waitMS, observe)
}

// applicationReadAbout serves Read and Observe with since nil, and
// ReadAboutCaller with since set; both share bounds, sealing and checks.
func (r *receiver) applicationReadAbout(since *string, target wire.ApplicationLogTarget, cursor string, maxRecords, maxBytes, waitMS int64, observe bool) (wire.ApplicationLogPage, error) {
	refuse := func(out wire.HistoryPageOutcome) (wire.ApplicationLogPage, error) {
		return wire.ApplicationLogPage{Outcome: out, Records: []wire.Record{}, Next: cursor}, nil
	}
	peer, account, caller, err := r.applicationPeer()
	if err != nil {
		return wire.ApplicationLogPage{}, err
	}
	policyContext := r.ctx
	if policyContext == nil {
		policyContext = context.Background()
	}
	if observe && waitMS > 0 {
		policyContext = r.call.WaitContext()
	}
	about := since != nil
	var from time.Time
	if about {
		from, err = time.Parse(time.RFC3339Nano, *since)
		if err != nil || maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 {
			return refuse(wire.HistoryPageOutcomeInvalid)
		}
	} else if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 || waitMS < 0 || waitMS > 30000 ||
		target.Selection != wire.ApplicationLogSelectionOneApp && target.Selection != wire.ApplicationLogSelectionAllApps ||
		target.Selection == wire.ApplicationLogSelectionOneApp && !identity.ValidSubjectProgram(target.Program) ||
		target.Selection == wire.ApplicationLogSelectionAllApps && target.Program != "" {
		return refuse(wire.HistoryPageOutcomeInvalid)
	}
	provider, ok := r.out.(applicationHistoryProvider)
	if !ok || !provider.ApplicationHistoryAvailable() {
		return refuse(wire.HistoryPageOutcomeUnsupported)
	}
	if r.applicationPolicy.Check == nil || r.applicationPolicy.Fits == nil {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	// A caller-log cursor records all_apps only so its sealed form stays a
	// valid selection; AboutCaller and Since decide what it reads.
	selection := target.Selection
	if about {
		selection = wire.ApplicationLogSelectionAllApps
	}
	state := applicationCursor{Version: 1, Account: account, Caller: caller, Selection: selection, InputProgram: target.Program, Physical: cursor}
	if about {
		state.AboutCaller, state.Since = true, *since
	}
	if cursor != "" && cursor != logging.EndCursor {
		state, err = r.openApplicationCursor(cursor)
		if err != nil || state.Account != account || state.Caller != caller || state.Selection != selection || state.InputProgram != target.Program ||
			state.AboutCaller != about || about && state.Since != *since {
			return refuse(wire.HistoryPageOutcomeGap)
		}
		// Resumption uses sealed captured evidence. It never asks today's filesystem
		// to reinterpret the stream's old selector or any persisted emitter.
	} else if !about && target.Selection == wire.ApplicationLogSelectionOneApp {
		state.Target, err = logging.PrepareApplicationResource(target.Program)
		if err != nil {
			return refuse(wire.HistoryPageOutcomeInvalid)
		}
	}
	query := ApplicationHistoryQuery{Selection: state.Selection, Target: state.Target}
	if about {
		query = ApplicationHistoryQuery{AboutCaller: true}
	}
	if !r.applicationPolicy.Fits(peer, query) {
		return refuse(wire.HistoryPageOutcomeInvalid)
	}
	if _, err = r.applicationPolicy.Check(policyContext, peer, query); err != nil {
		return refuse(appPolicyOutcome(err))
	}
	// A fresh caller-log read seeks to since instead of walking from the
	// earliest retained row (LOG-P9); the sealed cursor keeps the position.
	if about && cursor == "" {
		if seeker, ok := r.out.(historySeeker); ok {
			seeked, outcome := seeker.SeekApplicationHistory(from)
			if outcome != wire.PageOutcomePage {
				return refuse(historyOutcome(outcome))
			}
			state.Physical = seeked
		}
	}
	// The provider's history gate bounds waiters (WATCH-W2); the host keeps
	// no second bound here.
	var scan logging.HistoryScan
	position := state.Physical
	wait := time.Duration(0)
	if observe {
		wait = time.Duration(waitMS) * time.Millisecond
	}
	outcome, err := provider.HistoryGate().Wait(policyContext, wait, watch.Probe{
		Read: func() (bool, error) {
			next, err := provider.ScanApplicationHistory(position)
			if err != nil {
				return false, err
			}
			scan = next
			if scan.Outcome != wire.PageOutcomePage {
				return true, nil
			}
			position = scan.Next
			return len(scan.Rows) > 0 || !scan.AtEnd, nil
		},
		// The caller's selection is decided again after every read; the rows
		// themselves are decided once more below before any is returned.
		Recheck: func() error {
			if _, err := r.applicationPolicy.Check(policyContext, peer, query); err != nil {
				return policyRefusal{err}
			}
			return nil
		},
	})
	var refused policyRefusal
	switch {
	case err == nil:
	case errors.As(err, &refused):
		return refuse(appPolicyOutcome(refused.err))
	case policyContext.Err() != nil:
		return wire.ApplicationLogPage{}, err
	default:
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	if outcome == watch.Busy || outcome == watch.Failed {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	if scan.Outcome != wire.PageOutcomePage {
		return refuse(historyOutcome(scan.Outcome))
	}
	indices := make(map[string]int)
	candidateIndex := make([]int, len(scan.Rows))
	for i := range candidateIndex {
		candidateIndex[i] = -1
	}
	effectiveNext := scan.Next
	effectiveEnd := scan.AtEnd
	for i, row := range scan.Rows {
		if row.InvalidEmitter {
			return refuse(wire.HistoryPageOutcomeCorrupt)
		}
		if row.Emitter == nil || row.Emitter.Account != account {
			continue
		}
		if about {
			// Only the runtime's owned writers set a subject stamp; the caller
			// sees the rows written about itself, never another program's.
			if logging.SameCaller(row.Emitter.Subject, caller) && !recordBefore(row.Record.Time, from) {
				candidateIndex[i] = aboutCallerRow
			}
			continue
		}
		resource := row.Emitter.Resource()
		if state.Selection == wire.ApplicationLogSelectionOneApp && !logging.SameApplicationResource(state.Target, resource) {
			continue
		}
		key := string(logging.EncodeApplicationResource(resource))
		index, exists := indices[key]
		if !exists {
			proposed := query
			proposed.Emitters = append(append([]wire.ApplicationLogResource(nil), query.Emitters...), resource)
			if !r.applicationPolicy.Fits(peer, proposed) {
				if row.Before == scan.Start {
					return refuse(wire.HistoryPageOutcomeUnavailable)
				}
				effectiveNext = row.Before
				effectiveEnd = false
				break
			}
			query.Emitters = proposed.Emitters
			index = len(query.Emitters) - 1
			indices[key] = index
		}
		candidateIndex[i] = index
	}
	peer, finalAccount, finalCaller, err := r.applicationPeer()
	if err != nil {
		return wire.ApplicationLogPage{}, err
	}
	if finalAccount != account || finalCaller != caller {
		return wire.ApplicationLogPage{}, &wire.ServiceError{Code: wire.ServiceErrorCodeCallerUnavailable, Message: "caller changed"}
	}
	allowed, err := r.applicationPolicy.Check(policyContext, peer, query)
	if err != nil {
		return refuse(appPolicyOutcome(err))
	}
	if len(allowed) != len(query.Emitters) {
		return refuse(appPolicyOutcome(ErrHistoryPolicyUnavailable))
	}
	if err = policyContext.Err(); err != nil {
		return wire.ApplicationLogPage{}, err
	}
	_, finalAccount, finalCaller, err = r.applicationPeer()
	if err != nil {
		return wire.ApplicationLogPage{}, err
	}
	if finalAccount != account || finalCaller != caller {
		return wire.ApplicationLogPage{}, &wire.ServiceError{Code: wire.ServiceErrorCodeCallerUnavailable, Message: "caller changed"}
	}
	page := wire.ApplicationLogPage{Outcome: wire.HistoryPageOutcomePage, Records: []wire.Record{}, AtEnd: effectiveEnd}
	used := int64(0)
	for i, row := range scan.Rows {
		if row.Before == effectiveNext {
			break
		}
		index := candidateIndex[i]
		if index != aboutCallerRow && (index < 0 || !allowed[index]) {
			continue
		}
		size := int64(len(wire.Encode(&row.Record)))
		if int64(len(page.Records)) >= maxRecords || used+size > maxBytes {
			if len(page.Records) == 0 {
				return refuse(wire.HistoryPageOutcomeRecordTooLarge)
			}
			effectiveNext = row.Before
			page.AtEnd = false
			break
		}
		page.Records = append(page.Records, row.Record)
		used += size
	}
	state.Physical = effectiveNext
	page.Next, err = r.sealApplicationCursor(state)
	if err != nil {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	return page, nil
}

// aboutCallerRow marks a ReadAboutCaller row admitted by the caller-log check.
const aboutCallerRow = -2

// recordBefore reports whether a record's time is earlier than from. A time the
// record cannot state is treated as earlier, so it is never returned.
func recordBefore(value string, from time.Time) bool {
	at, err := time.Parse(time.RFC3339Nano, value)
	return err != nil || at.Before(from)
}
