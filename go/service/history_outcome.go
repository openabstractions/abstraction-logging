package service

import wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"

// historyOutcome converts the retained/native provider vocabulary at the new
// history boundary. Published account-history providers keep PageOutcome.
func historyOutcome(outcome wire.PageOutcome) wire.HistoryPageOutcome {
	switch outcome {
	case wire.PageOutcomePage:
		return wire.HistoryPageOutcomePage
	case wire.PageOutcomeGap:
		return wire.HistoryPageOutcomeGap
	case wire.PageOutcomeUnavailable:
		return wire.HistoryPageOutcomeUnavailable
	case wire.PageOutcomeInvalidRequest:
		return wire.HistoryPageOutcomeInvalid
	case wire.PageOutcomeRecordTooLarge:
		return wire.HistoryPageOutcomeRecordTooLarge
	case wire.PageOutcomeCorrupt:
		return wire.HistoryPageOutcomeCorrupt
	case wire.PageOutcomeUnsupported:
		return wire.HistoryPageOutcomeUnsupported
	default:
		return wire.HistoryPageOutcomeUnavailable
	}
}

// providerOutcome reads the helper's page back into the internal native
// vocabulary. Policy refusals belong to the receiving boundary.
func providerOutcome(outcome wire.HistoryPageOutcome) (wire.PageOutcome, bool) {
	switch outcome {
	case wire.HistoryPageOutcomePage:
		return wire.PageOutcomePage, true
	case wire.HistoryPageOutcomeGap:
		return wire.PageOutcomeGap, true
	case wire.HistoryPageOutcomeUnavailable:
		return wire.PageOutcomeUnavailable, true
	case wire.HistoryPageOutcomeInvalid:
		return wire.PageOutcomeInvalidRequest, true
	case wire.HistoryPageOutcomeRecordTooLarge:
		return wire.PageOutcomeRecordTooLarge, true
	case wire.HistoryPageOutcomeCorrupt:
		return wire.PageOutcomeCorrupt, true
	case wire.HistoryPageOutcomeUnsupported:
		return wire.PageOutcomeUnsupported, true
	default:
		return 0, false
	}
}
