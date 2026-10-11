package policy

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

type DefaultLifecycleOptions struct {
	Startup         StartupGate
	FoundersTimeout time.Duration
	Retention       time.Duration
	RetainForever   bool
	Removal         RemovalMode
}

// DefaultLifecycle can be embedded in an application policy that overrides just
// one decision. Its immutable settings are copied at construction.
type DefaultLifecycle struct{ opts DefaultLifecycleOptions }

func NewDefaultLifecycle(opts DefaultLifecycleOptions) (*DefaultLifecycle, error) {
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
		opts.Retention = 7 * 24 * time.Hour
	}
	return &DefaultLifecycle{opts: opts}, nil
}

func (p *DefaultLifecycle) Readiness(ctx context.Context, in ReadinessInput) (ReadinessDecision, error) {
	if err := ctx.Err(); err != nil {
		return ReadinessDecision{}, err
	}
	if p == nil {
		return ReadinessDecision{}, fault.ErrInvalid
	}
	admitted := in.Admitted && in.Recovered && in.SessionLive
	gate := p.opts.Startup == AllFounders && in.FirstBootstrap
	if gate && p.opts.FoundersTimeout > 0 && !in.StartedAt.IsZero() && !in.Now.Before(in.StartedAt.Add(p.opts.FoundersTimeout)) && (!admitted || len(in.MissingFounders) > 0) {
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

func (p *DefaultLifecycle) Retention(ctx context.Context, in RetentionInput) (RetentionDecision, error) {
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
		retention = 7 * 24 * time.Hour
	}
	at := in.Retired.QuarantinedAt.Add(retention)
	return RetentionDecision{Collect: !in.Now.Before(at), NotBefore: at}, nil
}

func (p *DefaultLifecycle) Removal(ctx context.Context, in RemovalInput) (RemovalDecision, error) {
	if err := ctx.Err(); err != nil {
		return RemovalDecision{}, err
	}
	if p == nil {
		return RemovalDecision{}, fault.ErrInvalid
	}
	if !in.Fenced || !in.Removal.Fenced || in.Identity.MemberID == 0 || in.Removal.MemberID != in.Identity.MemberID || in.Identity.NodeName == "" || in.Identity.ClusterID == "" || in.Identity.InstanceID == "" || in.Removal.Version == 0 || in.Removal.AppliedIndex == 0 || in.Removal.Reason == "" {
		return RemovalDecision{}, fault.ErrIdentityMismatch
	}
	action := StopNode
	if p.opts.Removal == RemovalKeepDiagnostics {
		action = KeepDiagnostics
	}
	return RemovalDecision{Action: action, Reason: in.Removal.Reason}, nil
}

var _ LifecyclePolicy = (*DefaultLifecycle)(nil)
