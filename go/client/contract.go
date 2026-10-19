package client

// The generated contract types this package's API reaches, re-exported so an
// application names them through this package and never imports the generated
// one. scripts/idiom_check.py refuses a reachable type this file leaves out.

import (
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

type Attestation = wire.Attestation

type Page = wire.Page

type ApplicationLogTarget = wire.ApplicationLogTarget
type ApplicationLogSelection = wire.ApplicationLogSelection
type ApplicationLogPage = wire.ApplicationLogPage
type HistoryPageOutcome = wire.HistoryPageOutcome

func HistoryPageOutcomeValues() []HistoryPageOutcome { return wire.HistoryPageOutcomeValues() }

const (
	ApplicationLogSelectionOneApp  = wire.ApplicationLogSelectionOneApp
	ApplicationLogSelectionAllApps = wire.ApplicationLogSelectionAllApps
)

func ApplicationLogSelectionValues() []ApplicationLogSelection {
	return wire.ApplicationLogSelectionValues()
}

type PageOutcome = wire.PageOutcome

const (
	PageOutcomePage           = wire.PageOutcomePage
	PageOutcomeGap            = wire.PageOutcomeGap
	PageOutcomeUnavailable    = wire.PageOutcomeUnavailable
	PageOutcomeInvalidRequest = wire.PageOutcomeInvalidRequest
	PageOutcomeRecordTooLarge = wire.PageOutcomeRecordTooLarge
	PageOutcomeCorrupt        = wire.PageOutcomeCorrupt
	PageOutcomeUnsupported    = wire.PageOutcomeUnsupported
)

// PageOutcomeValues returns every member of PageOutcome in declaration order, in a new slice.
func PageOutcomeValues() []PageOutcome { return wire.PageOutcomeValues() }

type ServiceError = wire.ServiceError

type ServiceErrorCode = wire.ServiceErrorCode

const (
	ServiceErrorCodeHandlerError      = wire.ServiceErrorCodeHandlerError
	ServiceErrorCodeInvalidResult     = wire.ServiceErrorCodeInvalidResult
	ServiceErrorCodeUnknownVersion    = wire.ServiceErrorCodeUnknownVersion
	ServiceErrorCodeUnknownService    = wire.ServiceErrorCodeUnknownService
	ServiceErrorCodeUnknownMethod     = wire.ServiceErrorCodeUnknownMethod
	ServiceErrorCodeWrongMode         = wire.ServiceErrorCodeWrongMode
	ServiceErrorCodeCallerUnavailable = wire.ServiceErrorCodeCallerUnavailable
	ServiceErrorCodeIdentityRequired  = wire.ServiceErrorCodeIdentityRequired
	ServiceErrorCodeWrongUser         = wire.ServiceErrorCodeWrongUser
	ServiceErrorCodePolicyUnavailable = wire.ServiceErrorCodePolicyUnavailable
	ServiceErrorCodeForbidden         = wire.ServiceErrorCodeForbidden
)

// ServiceErrorCodeValues returns every member of ServiceErrorCode in declaration order, in a new slice.
func ServiceErrorCodeValues() []ServiceErrorCode { return wire.ServiceErrorCodeValues() }
