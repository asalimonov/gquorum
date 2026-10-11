package policy

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// LifecyclePolicy decides optional startup gates, retirement retention and the
// response after mandatory removal fencing. Decisions cannot relax identity,
// admission, recovery or membership checks. Calls are serialized per node and
// must honor cancellation. Read-only status calls and RequestStop are allowed;
// waiting for a Stop that must join this callback is not.
type LifecyclePolicy interface {
	Readiness(context.Context, ReadinessInput) (ReadinessDecision, error)
	Retention(context.Context, RetentionInput) (RetentionDecision, error)
	Removal(context.Context, RemovalInput) (RemovalDecision, error)
}

type ReadinessInput struct {
	Now, StartedAt                   time.Time
	Identity                         topology.Identity
	FirstBootstrap                   bool
	Admitted, Recovered, SessionLive bool
	MissingFounders                  []topology.Identity
}
type ReadinessState string

const (
	Ready   ReadinessState = "ready"
	Waiting ReadinessState = "waiting"
	Failed  ReadinessState = "failed"
)

type ReadinessDecision struct {
	State  ReadinessState
	Reason string
}
type RetentionInput struct {
	Now     time.Time
	Retired storage.RetiredReplica
}
type RetentionDecision struct {
	Collect   bool
	NotBefore time.Time
	Reason    string
}
type RemovalInput struct {
	Identity topology.Identity
	Removal  topology.SelfRemovalStatus
	Fenced   bool
}
type RemovalAction string

const (
	StopNode        RemovalAction = "stop_node"
	KeepDiagnostics RemovalAction = "keep_diagnostics"
)

type RemovalDecision struct {
	Action RemovalAction
	Reason string
}
type StartupGate string

const (
	LocalAdmission StartupGate = "local_admission"
	AllFounders    StartupGate = "all_founders"
)

type RemovalMode string

const (
	RemovalStopNode        RemovalMode = "stop_node"
	RemovalKeepDiagnostics RemovalMode = "keep_diagnostics"
)
