package gquorum

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/load"
)

// GroupFactory acquires a machine for the owned, approved replica
// specification.
// On success, Node owns Application.Close. On failure, the factory releases any
// partial acquisitions. Callbacks must honor cancellation; they may inspect
// status or RequestStop, but must not wait for teardown that joins themselves.
type GroupFactory func(context.Context, load.ReplicaSpec) (load.Application, error)

// RetireReplicaFunc idempotently verifies and retires exact application state,
// including retries after prior cleanup. Return the entire matching proof with
// Verified=true. Callbacks must honor cancellation and may read status or call
// RequestStop; do not wait for their own group's lifecycle or Node.Stop.
type RetireReplicaFunc func(context.Context, load.RetirementRequest) (load.RetirementResult, error)

// LoadOptions supplies managed application factories, transport, storage and
// engine settings.
type LoadOptions struct {
	Factory                              GroupFactory
	Retire                               RetireReplicaFunc
	Transport                            load.Transport
	Storage                              LoadStorageOptions
	Engine                               load.EngineOptions
	EffectTimeout                        time.Duration
	RecoveryInterval, RetirementInterval time.Duration
}
