// Package lifecycle defines local readiness, retention and removal decisions.
// Node owns callback serialization, mandatory fencing and resource lifetimes.
package lifecycle

import (
	"context"
)

// Hooks decide optional startup gates, retirement retention and the
// response after mandatory removal fencing. Decisions cannot relax identity,
// admission, recovery or membership checks. Calls are serialized per node and
// must honor cancellation. Read-only status calls and RequestStop are allowed;
// waiting for a Stop that must join this callback is not.
type Hooks interface {
	Readiness(context.Context, ReadinessInput) (ReadinessDecision, error)
	Retention(context.Context, RetentionInput) (RetentionDecision, error)
	Removal(context.Context, RemovalInput) (RemovalDecision, error)
}

// Ready and the following states report an open, pending or failed startup
// gate.
const (
	Ready   ReadinessState = "ready"
	Waiting ReadinessState = "waiting"
	Failed  ReadinessState = "failed"
)

// StopNode and KeepDiagnostics select teardown or continued diagnostic access
// after fencing.
const (
	StopNode        RemovalAction = "stop_node"
	KeepDiagnostics RemovalAction = "keep_diagnostics"
)

// StartupGate selects the default readiness condition.
type StartupGate string

// LocalAdmission and AllFounders select local readiness or an additional
// fresh-founding gate.
const (
	LocalAdmission StartupGate = "local_admission"
	AllFounders    StartupGate = "all_founders"
)

// RemovalMode configures the default response to fenced self-removal.
type RemovalMode string

// RemovalStopNode and RemovalKeepDiagnostics configure teardown or diagnostic
// retention.
const (
	RemovalStopNode        RemovalMode = "stop_node"
	RemovalKeepDiagnostics RemovalMode = "keep_diagnostics"
)
