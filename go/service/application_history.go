package service

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"

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
	WaitApplicationHistory(context.Context, <-chan struct{}, int64) error
}
type applicationCursor struct {
	Version                                 int
	Account, Caller, InputProgram, Physical string
	Selection                               wire.ApplicationLogSelection
	Target                                  wire.ApplicationLogResource
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
		return nil, "", "", &wire.ServiceError{Code: "caller_unavailable", Message: "caller identity unavailable"}
	}
	who, err := peer.User.AtLeast(listen.Program.User)
	if err != nil {
		return nil, "", "", &wire.ServiceError{Code: "identity_required", Message: "native account required"}
	}
	account := ""
	if who.Kind == "windows" {
		account = who.SID
	} else if who.Kind == "posix" {
		account = strconv.Itoa(who.UID)
	}
	program, err := identity.SubjectProgram(peer, listen.Program.Path)
	if err != nil {
		return nil, "", "", &wire.ServiceError{Code: "identity_required", Message: "native program required"}
	}
	if account == "" || account != r.owner {
		return nil, "", "", &wire.ServiceError{Code: "wrong_user", Message: "history belongs to another user"}
	}
	return peer, account, program, nil
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
	sealed, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(sealed) < r.applicationSeal.NonceSize()+r.applicationSeal.Overhead() {
		return c, errors.New("logging: invalid cursor")
	}
	n := r.applicationSeal.NonceSize()
	plain, err := r.applicationSeal.Open(nil, sealed[:n], sealed[n:], []byte(logging.ApplicationLogResourceProfile))
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
	nonce := make([]byte, r.applicationSeal.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	result := base64.RawURLEncoding.EncodeToString(r.applicationSeal.Seal(nonce, nonce, plain, []byte(logging.ApplicationLogResourceProfile)))
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
func (a *applicationReceiver) Observe(target wire.ApplicationLogTarget, cursor string, maxRecords, maxBytes, waitMS int64) (wire.ApplicationLogPage, error) {
	return a.applicationRead(target, cursor, maxRecords, maxBytes, waitMS, true)
}

func (r *receiver) applicationRead(target wire.ApplicationLogTarget, cursor string, maxRecords, maxBytes, waitMS int64, observe bool) (wire.ApplicationLogPage, error) {
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
	if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 || waitMS < 0 || waitMS > 30000 ||
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
	state := applicationCursor{Version: 1, Account: account, Caller: caller, Selection: target.Selection, InputProgram: target.Program, Physical: cursor}
	if cursor != "" && cursor != logging.EndCursor {
		state, err = r.openApplicationCursor(cursor)
		if err != nil || state.Account != account || state.Caller != caller || state.Selection != target.Selection || state.InputProgram != target.Program {
			return refuse(wire.HistoryPageOutcomeGap)
		}
		// Resumption uses sealed captured evidence. It never asks today's filesystem
		// to reinterpret the stream's old selector or any persisted emitter.
	} else if target.Selection == wire.ApplicationLogSelectionOneApp {
		state.Target, err = logging.PrepareApplicationResource(target.Program)
		if err != nil {
			return refuse(wire.HistoryPageOutcomeInvalid)
		}
	}
	query := ApplicationHistoryQuery{Selection: state.Selection, Target: state.Target}
	if !r.applicationPolicy.Fits(peer, query) {
		return refuse(wire.HistoryPageOutcomeInvalid)
	}
	if _, err = r.applicationPolicy.Check(policyContext, peer, query); err != nil {
		return refuse(appPolicyOutcome(err))
	}
	scan, err := provider.ScanApplicationHistory(state.Physical)
	if err != nil {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	if scan.Outcome != wire.PageOutcomePage {
		return refuse(historyOutcome(scan.Outcome))
	}
	if observe && waitMS > 0 && len(scan.Rows) == 0 && scan.AtEnd {
		select {
		case r.observers <- struct{}{}:
			defer func() { <-r.observers }()
		default:
			return refuse(wire.HistoryPageOutcomeUnavailable)
		}
		if err = provider.WaitApplicationHistory(policyContext, scan.Changed, waitMS); err != nil {
			if errors.Is(err, logging.ErrHistoryWaiters) {
				return refuse(wire.HistoryPageOutcomeUnavailable)
			}
			if policyContext.Err() != nil {
				return wire.ApplicationLogPage{}, err
			}
			return refuse(wire.HistoryPageOutcomeUnavailable)
		}
		scan, err = provider.ScanApplicationHistory(scan.Next)
		if err != nil {
			return refuse(wire.HistoryPageOutcomeUnavailable)
		}
		if scan.Outcome != wire.PageOutcomePage {
			return refuse(historyOutcome(scan.Outcome))
		}
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
		return wire.ApplicationLogPage{}, &wire.ServiceError{Code: "caller_unavailable", Message: "caller changed"}
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
		return wire.ApplicationLogPage{}, &wire.ServiceError{Code: "caller_unavailable", Message: "caller changed"}
	}
	page := wire.ApplicationLogPage{Outcome: wire.HistoryPageOutcomePage, Records: []wire.Record{}, AtEnd: effectiveEnd}
	used := int64(0)
	for i, row := range scan.Rows {
		if row.Before == effectiveNext {
			break
		}
		index := candidateIndex[i]
		if index < 0 || !allowed[index] {
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
