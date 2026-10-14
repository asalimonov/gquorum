package lifecycle

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

const defaultRetentionDuration = 7 * 24 * time.Hour

// DefaultOptions configures startup gating, retired-state retention and removal
// response.
type DefaultOptions struct {
	Startup         StartupGate
	FoundersTimeout time.Duration
	Retention       time.Duration
	RetainForever   bool
	Removal         RemovalMode
}

// DefaultHooks can be embedded in application hooks that override just
// one decision. Its immutable settings are copied at construction.
type DefaultHooks struct{ opts DefaultOptions }

// NewDefaultHooks validates and copies settings, applying the default retention
// and startup mode.
func NewDefaultHooks(opts DefaultOptions) (*DefaultHooks, error) {
	if opts.FoundersTimeout < 0 || opts.Retention < 0 || opts.RetainForever && opts.Retention != 0 {
		return nil, fault.New(fault.Invalid, "invalid lifecycle duration or retention choice", nil)
	}
	if opts.Startup == "" {
		opts.Startup = LocalAdmission
	}
	if opts.Startup != LocalAdmission && opts.Startup != AllFounders {
		return nil, fault.ErrInvalid
	}
	if opts.Startup != AllFounders && opts.FoundersTimeout != 0 {
		return nil, fault.New(fault.Invalid, "founder timeout requires all-founders startup", nil)
	}
	if opts.Removal == "" {
		opts.Removal = RemovalStopNode
	}
	if opts.Removal != RemovalStopNode && opts.Removal != RemovalKeepDiagnostics {
		return nil, fault.ErrInvalid
	}
	if opts.Retention == 0 && !opts.RetainForever {
		opts.Retention = defaultRetentionDuration
	}
	return &DefaultHooks{opts: opts}, nil
}

// Readiness requires recovered admission and a live session, with the
// configured founder gate.
func (p *DefaultHooks) Readiness(ctx context.Context, in ReadinessInput) (ReadinessDecision, error) {
	if err := ctx.Err(); err != nil {
		return ReadinessDecision{}, err
	}
	if p == nil {
		return ReadinessDecision{}, fault.ErrInvalid
	}
	admitted := in.Admitted && in.Recovered && in.SessionLive
	gate := p.opts.Startup == AllFounders && in.FirstBootstrap
	if gate && p.opts.FoundersTimeout > 0 && (!admitted || len(in.MissingFounders) > 0) &&
		!in.StartedAt.IsZero() && !in.Now.Before(in.StartedAt.Add(p.opts.FoundersTimeout)) {
		return ReadinessDecision{State: Failed, Reason: "founders_timeout"}, nil
	}
	if !admitted {
		return ReadinessDecision{State: Waiting, Reason: "admission"}, nil
	}
	if gate && len(in.MissingFounders) > 0 {
		return ReadinessDecision{State: Waiting, Reason: "founders"}, nil
	}
	return ReadinessDecision{State: Ready}, nil
}

// Retention permits collection after the configured quarantine period unless
// retention is indefinite.
func (p *DefaultHooks) Retention(ctx context.Context, in RetentionInput) (RetentionDecision, error) {
	if err := ctx.Err(); err != nil {
		return RetentionDecision{}, err
	}
	if p == nil || in.Retired.QuarantinedAt.IsZero() {
		return RetentionDecision{}, fault.ErrInvalid
	}
	if p.opts.RetainForever {
		return RetentionDecision{Reason: "retain_forever"}, nil
	}
	retention := p.opts.Retention
	if retention == 0 {
		retention = defaultRetentionDuration
	}
	at := in.Retired.QuarantinedAt.Add(retention)
	return RetentionDecision{Collect: !in.Now.Before(at), NotBefore: at}, nil
}

// Removal validates exact fenced self-removal evidence and selects the
// configured response.
func (p *DefaultHooks) Removal(ctx context.Context, in RemovalInput) (RemovalDecision, error) {
	if err := ctx.Err(); err != nil {
		return RemovalDecision{}, err
	}
	if p == nil {
		return RemovalDecision{}, fault.ErrInvalid
	}
	if !in.Fenced || !in.Removal.Fenced || in.Identity.MemberID == 0 || in.Removal.MemberID != in.Identity.MemberID ||
		in.Identity.NodeName == "" || in.Identity.ClusterID == "" || in.Identity.InstanceID == "" ||
		in.Removal.Version == 0 || in.Removal.AppliedIndex == 0 || in.Removal.Reason == "" {
		return RemovalDecision{}, fault.ErrIdentityMismatch
	}
	action := StopNode
	if p.opts.Removal == RemovalKeepDiagnostics {
		action = KeepDiagnostics
	}
	return RemovalDecision{Action: action, Reason: in.Removal.Reason}, nil
}

var _ Hooks = (*DefaultHooks)(nil)
