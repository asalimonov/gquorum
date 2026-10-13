package operator

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/topology"
)

const (
	defaultControllerInterval = time.Second
	defaultMaxRetryInterval   = 30 * time.Second
	// defaultProposalTimeout bounds one replicated execution-record write. A
	// committed proposal applies in milliseconds; ten seconds is far past any
	// healthy commit yet far below the joiner's boot catch-up budget, so a lost
	// proposal costs one bounded wait instead of wedging the engine.
	defaultProposalTimeout = 10 * time.Second
)

type controllerClock interface {
	Now() time.Time
	NewTimer(time.Duration) controllerTimer
}

type controllerTimer interface {
	C() <-chan time.Time
	Reset(time.Duration)
	Stop()
}

type systemControllerClock struct{}

func (systemControllerClock) Now() time.Time { return time.Now() }

func (systemControllerClock) NewTimer(delay time.Duration) controllerTimer {
	return &systemControllerTimer{timer: time.NewTimer(delay)}
}

type systemControllerTimer struct{ timer *time.Timer }

func (t *systemControllerTimer) C() <-chan time.Time { return t.timer.C }

func (t *systemControllerTimer) Reset(delay time.Duration) {
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	t.timer.Reset(delay)
}

func (t *systemControllerTimer) Stop() { t.timer.Stop() }

type controllerRun struct {
	cancel   context.CancelFunc
	done     chan struct{}
	stopping bool
}

type controllerOutcome uint8

const (
	controllerIdle controllerOutcome = iota
	controllerProgressed
	controllerWaiting
)

// ControllerEngine executes one durably authorized topology step per tick.
// Two instances may observe the same execution: ownsGroup gives _meta and data
// groups disjoint owners, while replicated transitions provide the handoff.
type Controller struct {
	store           Store
	rules           Rules
	coordinator     OperatorCoordinator
	authority       AuthorityCoordinator
	groups          GroupEffects
	membership      MembershipCoordinator
	ownsGroup       func(groupID string) bool
	interval        time.Duration
	maxRetry        time.Duration
	proposalTimeout time.Duration
	clock           controllerClock
	finalValidator  func(context.Context, Document, Document) error
	planValidator   func(context.Context, Record) error

	mu     sync.Mutex
	run    *controllerRun
	wake   chan struct{}
	tickMu sync.Mutex
}

type Config struct {
	Store            Store
	Rules            Rules
	Coordinator      OperatorCoordinator
	Authority        AuthorityCoordinator
	Groups           GroupEffects
	Membership       MembershipCoordinator
	OwnsGroup        func(groupID string) bool
	Interval         time.Duration
	MaxRetryInterval time.Duration
	// ProposalTimeout bounds each replicated execution-record write the engine
	// makes (transition/fail/supersede). Zero selects the default. See the
	// deadline rationale on ControllerEngine.transition.
	ProposalTimeout time.Duration
	FinalValidator  func(context.Context, Document, Document) error
	// PlanValidator checks the immutable suffix and retained cleanup identities
	// against confirmed state after revocation, before any effect or activation.
	PlanValidator func(context.Context, Record) error

	clock controllerClock
}

func New(cfg Config) (*Controller, error) {
	if err := cfg.Rules.Limits.Validate(); err != nil || cfg.Rules.SchemaVersion != 1 {
		return nil, fault.New(fault.Invalid, "operator requires explicit valid cluster rules", err)
	}
	if cfg.Store == nil || cfg.Coordinator == nil || cfg.Groups == nil || cfg.Authority == nil {
		return nil, fault.New(fault.Invalid, "operator requires store, operator/authority coordinators and group effects", nil)
	}
	if cfg.OwnsGroup == nil {
		return nil, fault.New(fault.Invalid, "operator requires an immutable owns-group predicate", nil)
	}
	if cfg.PlanValidator == nil {
		return nil, fault.New(fault.Invalid, "operator requires a post-fence plan validator", nil)
	}
	if cfg.Interval < 0 || cfg.MaxRetryInterval < 0 || cfg.ProposalTimeout < 0 {
		return nil, fault.New(fault.Invalid, "negative operator timing", nil)
	}
	if cfg.Interval == 0 {
		cfg.Interval = defaultControllerInterval
	}
	if cfg.MaxRetryInterval == 0 {
		cfg.MaxRetryInterval = defaultMaxRetryInterval
	}
	if cfg.MaxRetryInterval < cfg.Interval {
		cfg.MaxRetryInterval = cfg.Interval
	}
	if cfg.ProposalTimeout == 0 {
		cfg.ProposalTimeout = defaultProposalTimeout
	}
	if cfg.clock == nil {
		cfg.clock = systemControllerClock{}
	}
	if cfg.Membership == nil {
		cfg.Membership = NewFixedMembershipCoordinator(cfg.Groups)
	}
	return &Controller{
		store: cfg.Store, rules: cfg.Rules, coordinator: cfg.Coordinator, groups: cfg.Groups, authority: cfg.Authority,
		membership: cfg.Membership, ownsGroup: cfg.OwnsGroup,
		interval: cfg.Interval, maxRetry: cfg.MaxRetryInterval,
		proposalTimeout: cfg.ProposalTimeout, clock: cfg.clock,
		finalValidator: cfg.FinalValidator,
		planValidator:  cfg.PlanValidator,
		wake:           make(chan struct{}, 1),
	}, nil
}

// SetFinalValidator installs the last-moment safety check. It is synchronized
// because server wiring may complete after the engine has been constructed.
func (e *Controller) SetFinalValidator(validator func(context.Context, Document, Document) error) {
	e.mu.Lock()
	e.finalValidator = validator
	e.mu.Unlock()
}

// Signal schedules a prompt pass without blocking Apply or accumulating work.
func (e *Controller) Signal() {
	if e == nil {
		return
	}
	e.mu.Lock()
	running := e.run != nil
	e.mu.Unlock()
	if !running {
		return
	}
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Controller) Start(parent context.Context) {
	if e == nil {
		return
	}
	e.mu.Lock()
	if e.run != nil {
		e.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	run := &controllerRun{cancel: cancel, done: make(chan struct{})}
	e.run = run
	e.mu.Unlock()

	go e.loop(ctx, run)
}

func (e *Controller) Stop() {
	if e == nil {
		return
	}
	e.mu.Lock()
	run := e.run
	if run != nil {
		run.stopping = true
	}
	e.mu.Unlock()
	if run == nil {
		return
	}
	run.cancel()
	<-run.done

	e.mu.Lock()
	if e.run == run {
		e.run = nil
	}
	e.mu.Unlock()
}

func (e *Controller) loop(ctx context.Context, run *controllerRun) {
	defer func() {
		close(run.done)
		e.mu.Lock()
		if e.run == run && !run.stopping {
			e.run = nil
		}
		e.mu.Unlock()
	}()
	timer := e.clock.NewTimer(0)
	defer timer.Stop()
	retryDelay := e.interval
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C():
			outcome := e.tick(ctx)
			retryDelay = e.nextDelay(outcome, retryDelay)
			timer.Reset(retryDelay)
		case <-e.wake:
			outcome := e.tick(ctx)
			retryDelay = e.nextDelay(outcome, retryDelay)
			timer.Reset(retryDelay)
		}
	}
}

func (e *Controller) nextDelay(outcome controllerOutcome, previous time.Duration) time.Duration {
	if outcome != controllerWaiting {
		return e.interval
	}
	if previous < e.interval {
		return e.interval
	}
	if previous >= e.maxRetry/2 {
		return e.maxRetry
	}
	return previous * 2
}

// Tick observes and advances at most one exact step. It deliberately returns no
// error: every retryable or terminal outcome is part of the replicated
// execution record, while loss of leadership simply leaves the record intact.
func (e *Controller) Tick(ctx context.Context) {
	e.tick(ctx)
}

func (e *Controller) tick(ctx context.Context) controllerOutcome {
	e.tickMu.Lock()
	defer e.tickMu.Unlock()
	if ctx.Err() != nil || !e.coordinator.IsMetaLeader() {
		return controllerIdle
	}
	if err := e.authority.ReconcileFences(ctx); err != nil {
		return controllerWaiting
	}
	operator, ok := e.coordinator.CurrentOperator(ctx)
	if !ok {
		return controllerIdle
	}
	if operator.State.Terminal() {
		return controllerIdle
	}
	target, err := e.store.Target(ctx)
	if err != nil {
		return controllerWaiting
	}
	if target.Version != operator.Version {
		if target.Version > operator.Version {
			return e.supersede(ctx, operator)
		}
		return controllerIdle
	}
	active, err := e.store.Active(ctx)
	if err != nil {
		return controllerWaiting
	}
	if err := e.planValidator(ctx, operator); err != nil {
		step := Step{}
		if len(operator.Steps) != 0 {
			seq := max(operator.CurrentStep, 1)
			if seq > len(operator.Steps) {
				return controllerIdle
			}
			step = operator.Steps[seq-1]
		}
		return e.recordError(ctx, operator, step, err)
	}
	if len(operator.Steps) == 0 {
		return e.finish(ctx, operator, target, nil)
	}
	seq := operator.CurrentStep
	if seq == 0 {
		seq = 1
	}
	if seq < 1 || seq > len(operator.Steps) {
		e.fail(ctx, operator, Step{Seq: seq}, fmt.Errorf("%w: execution cursor is outside its exact plan", ErrOperatorAuthorization))
		return controllerProgressed
	}
	step := operator.Steps[seq-1]
	if !e.ownsGroup(step.GroupID) {
		return controllerIdle
	}

	status, statusErr := e.groups.Status(ctx, step.GroupID)
	if statusErr != nil {
		if canResumeFounding(step, active, target, statusErr) {
			// Founding fans out the same deterministic voter set to every host. If
			// leadership or an RPC is interrupted after the first host starts, that
			// partial group cannot elect a leader and Status remains unavailable.
			// Re-run the authorized idempotent fan-out so the remaining hosts can
			// install the group; established groups never take this path.
			status = GroupStatus{GroupID: step.GroupID}
		} else if errors.Is(statusErr, ErrGroupUnavailable) && active.Group(step.GroupID) != nil && target.Group(step.GroupID) == nil &&
			(step.Kind == StepStopReplica || step.Kind == StepDeleteGroupState) {
			// Whole-group deletion deliberately stops every runtime without first
			// changing Raft membership. Once enough hosts have acknowledged their
			// stop, the group cannot have a leader; the remaining exact host-local
			// stops and shared-state deletion must still proceed. Their scoped RPCs
			// remain the reachability and authorization boundary.
			status = GroupStatus{GroupID: step.GroupID, Found: step.Kind == StepStopReplica}
		} else {
			return e.recordError(ctx, operator, step, statusErr)
		}
	}
	if err := e.validateStep(ctx, operator, active, target, step, status); err != nil {
		if errors.Is(err, ErrOperatorSuperseded) {
			return e.supersede(ctx, operator)
		}
		return e.recordError(ctx, operator, step, err)
	}
	if err := e.ensureRunning(ctx, operator, step); err != nil {
		return controllerIdle
	}
	if step.Kind == StepWaitCaughtUp && e.effectSatisfied(step, target, status) {
		return e.advance(ctx, operator, target, step)
	}
	if err := e.revalidateExact(ctx, operator, step); err != nil {
		if errors.Is(err, ErrOperatorSuperseded) {
			return e.supersede(ctx, operator)
		}
		e.fail(ctx, operator, step, err)
		return controllerProgressed
	}
	if err := e.runStep(ctx, step, status); err != nil {
		if ctx.Err() == nil && e.coordinator.IsMetaLeader() && e.operatorIsCurrent(ctx, operator, step) {
			return e.recordError(ctx, operator, step, err)
		}
		return controllerIdle
	}
	// A submitted ConfChange is never rolled back or overlapped. Its call is
	// allowed to finish, then leadership and the exact replicated boundary are
	// observed again before this worker records progress.
	if ctx.Err() != nil || !e.coordinator.IsMetaLeader() {
		return controllerIdle
	}
	if err := e.revalidateExact(ctx, operator, step); err != nil {
		if errors.Is(err, ErrOperatorSuperseded) {
			return e.supersede(ctx, operator)
		}
		return controllerIdle
	}
	// Membership/create/transfer acknowledgements are verified by a fresh
	// global observation. Host-local lifecycle callbacks attest their verified
	// exact identity result as part of the GroupEffects contract.
	if requiresObservedVerification(step.Kind) {
		observed, err := e.groups.Status(ctx, step.GroupID)
		if err != nil {
			return e.recordError(ctx, operator, step, err)
		}
		if observed.GroupID != step.GroupID {
			return e.recordError(ctx, operator, step, ErrOperatorAuthorization)
		}
		if !e.effectSatisfied(step, target, observed) {
			return e.recordError(ctx, operator, step, fmt.Errorf("%w: acknowledged effect is not observed for exact replica %d", ErrReplicaNotReady, step.ReplicaID))
		}
	}
	return e.advance(ctx, operator, target, step)
}

func canResumeFounding(step Step, active, target Document, err error) bool {
	return step.Kind == StepCreateFoundingGroup && errors.Is(err, ErrGroupUnavailable) &&
		active.Group(step.GroupID) == nil && target.Group(step.GroupID) != nil
}

func (e *Controller) ensureRunning(ctx context.Context, ex Record, step Step) error {
	if ex.CurrentStep == step.Seq && ex.State == OperatorRunning {
		return nil
	}
	return e.transition(ctx, ex, step, OperatorRunning, "", "")
}

func (e *Controller) advance(ctx context.Context, ex Record, target Document, step Step) controllerOutcome {
	if step.Seq == len(ex.Steps) {
		return e.finish(ctx, ex, target, &step)
	}
	next := ex.Steps[step.Seq]
	if err := e.transition(ctx, ex, next, OperatorRunning, "", ""); err == nil {
		return controllerProgressed
	}
	return controllerIdle
}

func (e *Controller) finish(ctx context.Context, ex Record, target Document, last *Step) controllerOutcome {
	if err := target.Validate(e.rules); err != nil {
		step := transitionStep(ex, last)
		e.fail(ctx, ex, step, fmt.Errorf("final Target validation: %w", err))
		return controllerProgressed
	}
	for _, groupID := range affectedGroupIDs(ex.Steps) {
		status, err := e.groups.Status(ctx, groupID)
		if err != nil {
			if errors.Is(err, ErrGroupUnavailable) && target.Group(groupID) == nil {
				status = GroupStatus{GroupID: groupID}
			} else {
				return e.recordError(ctx, ex, transitionStep(ex, last), err)
			}
		}
		if !groupMatchesTarget(target.Group(groupID), status) {
			return e.recordError(ctx, ex, transitionStep(ex, last),
				fmt.Errorf("%w: group %q does not match Target after final step", ErrReplicaNotReady, groupID))
		}
		if target.Group(groupID) != nil {
			native, err := e.groups.ReadNative(ctx, groupID)
			if err != nil {
				return e.recordError(ctx, ex, transitionStep(ex, last), err)
			}
			if native.GroupID != groupID || !groupMatchesTarget(target.Group(groupID), GroupStatus{GroupID: groupID, Found: true, ConfState: native.ConfState}) {
				return e.recordError(ctx, ex, transitionStep(ex, last),
					fmt.Errorf("%w: group %q applied membership does not match Target after final step", ErrReplicaNotReady, groupID))
			}
		}
	}
	active, err := e.store.Active(ctx)
	if err != nil {
		return e.recordError(ctx, ex, transitionStep(ex, last), err)
	}
	e.mu.Lock()
	validator := e.finalValidator
	e.mu.Unlock()
	if validator != nil {
		if err := validator(ctx, active, target); err != nil {
			e.fail(ctx, ex, transitionStep(ex, last), fmt.Errorf("final drain safety validation: %w", err))
			return controllerProgressed
		}
	}
	step := transitionStep(ex, last)
	if err := e.transition(ctx, ex, step, OperatorSucceeded, "", ""); err == nil {
		return controllerProgressed
	}
	return controllerIdle
}

func transitionStep(ex Record, last *Step) Step {
	if last != nil {
		return *last
	}
	if len(ex.Steps) == 0 {
		return Step{}
	}
	return ex.Steps[len(ex.Steps)-1]
}

func (e *Controller) runStep(ctx context.Context, step Step, status GroupStatus) error {
	if ctx.Err() != nil || !e.coordinator.IsMetaLeader() {
		return ErrNotMetaLeader
	}
	cmd := authorizedStepForCurrent(e.coordinator, ctx, step)
	if cmd.OperatorID == "" {
		return fmt.Errorf("%w: current execution changed before side effect", ErrOperatorAuthorization)
	}
	if step.Kind == StepWaitCaughtUp {
		reason := "learner has not reached the fixed catch-up threshold"
		if _, why := caughtUpStatus(status, step.ReplicaID); why != "" {
			reason = why
		}
		return fmt.Errorf("%w: %s", ErrReplicaNotReady, reason)
	}
	grant, err := e.authority.BeginEffect(ctx, cmd)
	if err != nil {
		return err
	}
	if grant.Validate() != nil || grant.Action == nil || *grant.Action != cmd {
		return ErrOperatorAuthorization
	}
	recorded, err := e.authority.EffectReceipts(ctx, grant)
	if err != nil {
		return err
	}
	complete, err := receiptSetComplete(grant, recorded)
	if err != nil {
		return err
	}
	if complete && (step.Kind != StepTransferLeader || status.LeaderID == step.ReplicaID) {
		return nil
	}
	if authorizedStepForCurrent(e.coordinator, ctx, step) != cmd {
		return ErrOperatorAuthorization
	}
	receipts, effectErr := e.dispatchEffect(ctx, step, grant)
	for _, receipt := range receipts {
		if receipt.Validate(grant) != nil {
			return ErrOperatorAuthorization
		}
		if err := e.authority.RecordEffect(ctx, receipt); err != nil {
			return err
		}
	}
	if effectErr != nil {
		return effectErr
	}
	// The replicated catalog owns receipt accumulation across partial attempts
	// and leader changes. A successful RPC alone cannot authorize progress.
	recorded, err = e.authority.EffectReceipts(ctx, grant)
	if err != nil {
		return err
	}
	complete, err = receiptSetComplete(grant, recorded)
	if err != nil {
		return err
	}
	if !complete {
		return fault.ErrAuthorityPending
	}
	return nil
}

func (e *Controller) dispatchEffect(ctx context.Context, step Step, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	switch step.Kind {
	case StepCreateFoundingGroup:
		return e.groups.CreateGroup(ctx, grant)
	case StepPrepareReplica:
		return e.groups.PrepareReplica(ctx, grant)
	case StepAddLearner, StepPromoteVoter, StepDemoteVoter, StepRemoveReplica:
		return e.membership.Apply(ctx, grant)
	case StepTransferLeader:
		return e.groups.TransferLeader(ctx, grant)
	case StepStopReplica:
		return e.groups.StopReplica(ctx, grant)
	case StepCleanupReplicaState:
		return e.groups.CleanupReplicaState(ctx, grant)
	case StepDeleteGroupState:
		return e.groups.DeleteGroupState(ctx, grant)
	default:
		return nil, fmt.Errorf("%w: unknown step kind %q", ErrOperatorAuthorization, step.Kind)
	}
}

func authorizedStepForCurrent(coordinator OperatorCoordinator, ctx context.Context, step Step) AuthorizedStep {
	ex, ok := coordinator.CurrentOperator(ctx)
	if !ok || ex.State.Terminal() || step.Seq < 1 || step.Seq > len(ex.Steps) ||
		ex.CurrentStep != step.Seq || !ex.Steps[step.Seq-1].Equal(step) {
		return AuthorizedStep{}
	}
	return AuthorizedStep{
		OperatorID: ex.ID, Version: ex.Version, Attempt: ex.Attempt,
		StepSeq: step.Seq, Kind: step.Kind, GroupID: step.GroupID,
		NodeName: step.NodeName, ReplicaID: step.ReplicaID,
		TransferNodeName: step.TransferNodeName, TransferReplicaID: step.TransferReplicaID,
	}
}

func (e *Controller) revalidateExact(ctx context.Context, expected Record, step Step) error {
	if ctx.Err() != nil || !e.coordinator.IsMetaLeader() {
		return ErrNotMetaLeader
	}
	current, ok := e.coordinator.CurrentOperator(ctx)
	if !ok || current.ID != expected.ID || current.Version != expected.Version || current.Attempt != expected.Attempt ||
		current.State.Terminal() || current.CurrentStep != step.Seq || step.Seq > len(current.Steps) ||
		!current.Steps[step.Seq-1].Equal(step) {
		return fmt.Errorf("%w: exact execution step changed", ErrOperatorAuthorization)
	}
	target, err := e.store.Target(ctx)
	if err != nil {
		return err
	}
	if target.Version != expected.Version {
		return fmt.Errorf("%w: Target version changed", ErrOperatorSuperseded)
	}
	return nil
}

func (e *Controller) transition(ctx context.Context, ex Record, step Step, state OperatorState, waiting, failure string) error {
	if ctx.Err() != nil || !e.coordinator.IsMetaLeader() {
		return ErrNotMetaLeader
	}
	// Deadline rationale: the engine's loop context is cancel-only, and a raft
	// proposal can be lost SILENTLY — appended at term N, then truncated by the
	// term-N+1 log after a leadership transfer — in which case it never applies
	// and the store's pending-waiter never resolves (only an explicit
	// ErrProposalDropped resolves it early). Unbounded, one such loss parks this
	// goroutine inside tick() forever (tickMu held, no further ticks): the exact
	// execution freeze the meta-leader-change cluster test reproduces. Bounded,
	// the loss costs one timeout; the next tick re-proposes under a fresh
	// proposal ID and the record converges.
	wctx, cancel := context.WithTimeout(ctx, e.proposalTimeout)
	defer cancel()
	return e.coordinator.RecordOperatorTransition(wctx, OperatorTransition{
		OperatorID: ex.ID, Version: ex.Version, Attempt: ex.Attempt,
		StepSeq: step.Seq, State: state, GroupID: step.GroupID,
		NodeName: step.NodeName, WaitingReason: waiting, Error: failure,
		At: operatorTime(e.clock.Now().UTC(), ex.UpdatedAt),
	})
}

func (e *Controller) recordError(ctx context.Context, ex Record, step Step, err error) controllerOutcome {
	if ctx.Err() != nil || !e.coordinator.IsMetaLeader() || !e.operatorIsCurrent(ctx, ex, step) {
		return controllerIdle
	}
	state, reason, retry := classifyStepError(step, err)
	if retry {
		if e.transition(ctx, ex, step, state, reason, "") == nil {
			return controllerWaiting
		}
		return controllerIdle
	}
	e.fail(ctx, ex, step, err)
	return controllerProgressed
}

func (e *Controller) fail(ctx context.Context, ex Record, step Step, err error) {
	reason := fmt.Sprintf("failed-at-step-%d: %v", step.Seq, err)
	if e.transition(ctx, ex, step, OperatorFailed, "", reason) == nil {
	}
}

func (e *Controller) supersede(ctx context.Context, ex Record) controllerOutcome {
	step := Step{}
	if ex.CurrentStep > 0 && ex.CurrentStep <= len(ex.Steps) {
		step = ex.Steps[ex.CurrentStep-1]
	} else if len(ex.Steps) > 0 {
		step = ex.Steps[0]
	}
	if err := e.transition(ctx, ex, step, OperatorSuperseded, "", ""); err == nil {
		return controllerProgressed
	}
	return controllerIdle
}

func (e *Controller) operatorIsCurrent(ctx context.Context, expected Record, step Step) bool {
	current, ok := e.coordinator.CurrentOperator(ctx)
	if len(expected.Steps) == 0 && step.Seq == 0 {
		return ok && current.ID == expected.ID && current.Version == expected.Version && current.Attempt == expected.Attempt && !current.State.Terminal() && current.CurrentStep == 0 && len(current.Steps) == 0
	}
	cursorMatches := current.CurrentStep == step.Seq || current.CurrentStep == 0 && step.Seq == 1
	return ok && current.ID == expected.ID && current.Version == expected.Version && current.Attempt == expected.Attempt &&
		!current.State.Terminal() && cursorMatches && step.Seq >= 1 && step.Seq <= len(current.Steps) &&
		current.Steps[step.Seq-1].Equal(step)
}

func classifyStepError(step Step, err error) (OperatorState, string, bool) {
	if errors.Is(err, ErrReplicaNotReady) {
		if step.Kind == StepWaitCaughtUp || step.Kind == StepPromoteVoter {
			return OperatorWaitingCatchUp, err.Error(), true
		}
		return OperatorWaitingReplica, err.Error(), true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, ErrGroupUnavailable) || errors.Is(err, ErrNoQuorum) || errors.Is(err, ErrNotMetaLeader) || errors.Is(err, fault.ErrAuthorityPending) || errors.Is(err, fault.ErrOverloaded) {
		if step.Kind == StepPrepareReplica {
			return OperatorWaitingReplica, err.Error(), true
		}
		return OperatorWaitingLeader, err.Error(), true
	}
	var refusal *membership.SafetyRefusal
	if errors.As(err, &refusal) {
		if refusal.Reason == "replica is not a current learner; add it as a learner first" {
			return OperatorFailed, "", false
		}
		if step.Kind == StepPromoteVoter || step.Kind == StepWaitCaughtUp {
			return OperatorWaitingCatchUp, err.Error(), true
		}
		if refusal.Reason == "replica liveness is unknown" || refusal.Reason == "post-change live voter quorum would be unavailable" {
			return OperatorWaitingReplica, err.Error(), true
		}
	} else if errors.Is(err, ErrSafetyRefused) {
		// Caller transports should preserve SafetyRefusal. Without its reason,
		// waiting on promotion remains conservative; destructive refusals fail.
		if step.Kind == StepPromoteVoter || step.Kind == StepWaitCaughtUp {
			return OperatorWaitingCatchUp, err.Error(), true
		}
	}

	return OperatorFailed, "", false
}

// fixedMembershipCoordinator keeps controller on the scoped GroupEffects
// path. The receiving group leader owns the concrete membership.Coordinator;
// CaughtUp applies the same fixed neutral status rule before promotion.
type fixedMembershipCoordinator struct{ groups GroupEffects }

func NewFixedMembershipCoordinator(groups GroupEffects) MembershipCoordinator {
	return &fixedMembershipCoordinator{groups: groups}
}

func (c *fixedMembershipCoordinator) Apply(ctx context.Context, cmd topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return c.groups.ApplyMembership(ctx, cmd)
}

func (*fixedMembershipCoordinator) CaughtUp(status GroupStatus, replicaID uint64) bool {
	ok, _ := caughtUpStatus(status, replicaID)
	return ok
}

// caughtUpStatus is CaughtUp plus the SPECIFIC reason it is not yet satisfied, so
// the reason reaches the step's WaitingReason instead of a fixed constant: a
// learner stuck "not recently active" (a transient idle-leader window) is
// otherwise indistinguishable from one that is genuinely lagging.
func caughtUpStatus(status GroupStatus, replicaID uint64) (bool, string) {
	if !containsID(status.ConfState.Learners, replicaID) {
		return false, "replica is not a current learner"
	}
	if status.Commit == 0 {
		return false, "group commit index is zero"
	}
	progress, ok := status.Progress[replicaID]
	if !ok {
		return false, "learner progress is unavailable"
	}
	// RecentActive gates only a LAGGING learner. The bit is reset by every
	// CheckQuorum pass and set again on the next response, so on an idle group
	// a caught-up learner (match == commit) samples false in a window every
	// election timeout — the transient idle-leader false negative this reason
	// string exists to name. A learner at the commit index has nothing left to
	// prove by activity; one still behind must also be reachable.
	if !progress.RecentActive && progress.Match < status.Commit {
		return false, "learner is not recently active"
	}
	if progress.Match < status.Commit && status.Commit-progress.Match > membership.PromotionLagLimit {
		return false, fmt.Sprintf("learner lag %d exceeds limit %d", status.Commit-progress.Match, membership.PromotionLagLimit)
	}
	return true, ""
}

func (e *Controller) effectSatisfied(step Step, target Document, status GroupStatus) bool {
	switch step.Kind {
	case StepCreateFoundingGroup:
		return status.Found && foundingMatchesTarget(target.Group(step.GroupID), status)
	case StepPrepareReplica:
		return containsID(status.ConfState.Learners, step.ReplicaID) || containsID(status.ConfState.Voters, step.ReplicaID)
	case StepAddLearner:
		return containsID(status.ConfState.Learners, step.ReplicaID) || containsID(status.ConfState.Voters, step.ReplicaID)
	case StepWaitCaughtUp:
		return containsID(status.ConfState.Voters, step.ReplicaID) || e.membership.CaughtUp(status, step.ReplicaID)
	case StepPromoteVoter:
		return containsID(status.ConfState.Voters, step.ReplicaID)
	case StepDemoteVoter:
		return containsID(status.ConfState.Learners, step.ReplicaID)
	case StepTransferLeader:
		return status.LeaderID == step.ReplicaID
	case StepRemoveReplica:
		return !containsID(status.ConfState.Voters, step.ReplicaID) && !containsID(status.ConfState.Learners, step.ReplicaID)
	case StepStopReplica, StepCleanupReplicaState:
		return forcedRetirementStep(target, step)
	default:
		return false
	}
}

func forcedRetirementStep(target Document, step Step) bool {
	audit := target.Audit
	return audit != nil && audit.Forced && audit.Operation == AuditOperationForceRetireNode &&
		audit.TargetNode == step.NodeName
}

func (e *Controller) validateStep(ctx context.Context, ex Record, active, target Document, step Step, status GroupStatus) error {
	if err := target.Validate(e.rules); err != nil {
		return err
	}
	if err := e.revalidateExactBeforeStart(ctx, ex, step); err != nil {
		return err
	}
	if status.GroupID != "" && status.GroupID != step.GroupID {
		return fmt.Errorf("%w: status group %q does not match %q", ErrOperatorAuthorization, status.GroupID, step.GroupID)
	}
	targetReplica := documentReplicaByID(target, step.GroupID, step.ReplicaID)
	activeReplica := documentReplicaByID(active, step.GroupID, step.ReplicaID)
	switch step.Kind {
	case StepCreateFoundingGroup:
		if active.Group(step.GroupID) != nil || target.Group(step.GroupID) == nil {
			return fmt.Errorf("%w: founding group is not fresh", ErrOperatorAuthorization)
		}
		if status.Found && !foundingMatchesTarget(target.Group(step.GroupID), status) {
			return fmt.Errorf("%w: founding group has conflicting membership", ErrOperatorAuthorization)
		}
	case StepPrepareReplica:
		if targetReplica == nil || active.Group(step.GroupID) == nil && (!freshFoundingPlan(ex, step) || targetReplica.Role != ReplicaLearner) {
			return fmt.Errorf("%w: prepare identity is not an established Target replica", ErrOperatorAuthorization)
		}
	case StepAddLearner:
		if targetReplica == nil {
			return fmt.Errorf("%w: learner is absent from Target", ErrOperatorAuthorization)
		}
	case StepWaitCaughtUp, StepPromoteVoter:
		if targetReplica == nil || targetReplica.Role != ReplicaVoter {
			return fmt.Errorf("%w: promotion target is not a desired voter", ErrOperatorAuthorization)
		}
	case StepDemoteVoter:
		if targetReplica == nil || targetReplica.Role != ReplicaLearner {
			return fmt.Errorf("%w: demotion target is not a desired learner", ErrOperatorAuthorization)
		}
	case StepTransferLeader:
		if targetReplica == nil || targetReplica.Role != ReplicaVoter {
			return fmt.Errorf("%w: leadership target is not a desired voter", ErrOperatorAuthorization)
		}
	case StepRemoveReplica, StepStopReplica:
		if targetReplica != nil || activeReplica == nil || activeReplica.NodeName != step.NodeName {
			return fmt.Errorf("%w: removal does not name an exact former replica", ErrOperatorAuthorization)
		}
	case StepCleanupReplicaState:
		if step.GroupID == MetaGroupID || targetReplica != nil || activeReplica == nil ||
			step.Seq < 3 || ex.Steps[step.Seq-2].Kind != StepStopReplica || ex.Steps[step.Seq-3].Kind != StepRemoveReplica {
			return fmt.Errorf("%w: cleanup is not authorized after exact data replica removal and stop", ErrOperatorAuthorization)
		}
	case StepDeleteGroupState:
		if step.GroupID == MetaGroupID || target.Group(step.GroupID) != nil || active.Group(step.GroupID) == nil {
			return fmt.Errorf("%w: delete does not name an exact removed data group", ErrOperatorAuthorization)
		}
	default:
		return fmt.Errorf("%w: unknown step kind %q", ErrOperatorAuthorization, step.Kind)
	}
	deletingGroupStop := step.Kind == StepStopReplica && active.Group(step.GroupID) != nil && target.Group(step.GroupID) == nil
	if step.Kind != StepCreateFoundingGroup && step.Kind != StepDeleteGroupState && !deletingGroupStop && !status.Found {
		return fmt.Errorf("%w: group %q is not globally present", ErrGroupUnavailable, step.GroupID)
	}
	if (step.Kind == StepAddLearner || step.Kind == StepPromoteVoter || step.Kind == StepDemoteVoter ||
		step.Kind == StepRemoveReplica || step.Kind == StepTransferLeader) && status.LeaderID == 0 {
		return fmt.Errorf("%w: group %q has no leader", ErrGroupUnavailable, step.GroupID)
	}
	return nil
}

func (e *Controller) revalidateExactBeforeStart(ctx context.Context, ex Record, step Step) error {
	current, ok := e.coordinator.CurrentOperator(ctx)
	if !ok || current.ID != ex.ID || current.Version != ex.Version || current.Attempt != ex.Attempt || current.State.Terminal() {
		return fmt.Errorf("%w: execution changed before validation", ErrOperatorAuthorization)
	}
	if step.Seq > len(current.Steps) || !current.Steps[step.Seq-1].Equal(step) {
		return fmt.Errorf("%w: exact step changed before validation", ErrOperatorAuthorization)
	}
	return nil
}

func groupMatchesTarget(group *Group, status GroupStatus) bool {
	if group == nil {
		return !status.Found
	}
	if !status.Found {
		return false
	}
	if status.ConfState.AutoLeave || len(status.ConfState.VotersOutgoing) != 0 || len(status.ConfState.LearnersNext) != 0 {
		return false
	}
	var voters, learners []uint64
	for _, replica := range group.Replicas {
		if replica.Role == ReplicaVoter {
			voters = append(voters, replica.ReplicaID)
		} else {
			learners = append(learners, replica.ReplicaID)
		}
	}
	sort.Slice(voters, func(i, j int) bool { return voters[i] < voters[j] })
	sort.Slice(learners, func(i, j int) bool { return learners[i] < learners[j] })
	actualVoters := append([]uint64(nil), status.ConfState.Voters...)
	actualLearners := append([]uint64(nil), status.ConfState.Learners...)
	sort.Slice(actualVoters, func(i, j int) bool { return actualVoters[i] < actualVoters[j] })
	sort.Slice(actualLearners, func(i, j int) bool { return actualLearners[i] < actualLearners[j] })
	return equalUint64s(voters, actualVoters) && equalUint64s(learners, actualLearners)
}

func affectedGroupIDs(steps []Step) []string {
	seen := make(map[string]struct{})
	for _, step := range steps {
		seen[step.GroupID] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func documentReplicaByID(doc Document, groupID string, replicaID uint64) *Replica {
	group := doc.Group(groupID)
	if group == nil {
		return nil
	}
	for i := range group.Replicas {
		if group.Replicas[i].ReplicaID == replicaID {
			return &group.Replicas[i]
		}
	}
	return nil
}

func containsID(ids []uint64, target uint64) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func operatorTime(now, previous time.Time) time.Time {
	if now.Before(previous) {
		return previous
	}
	return now
}

func requiresObservedVerification(kind StepKind) bool {
	switch kind {
	case StepCreateFoundingGroup, StepAddLearner, StepPromoteVoter, StepDemoteVoter, StepRemoveReplica, StepTransferLeader:
		return true
	}
	return false
}

// Fresh groups seed only their declared voters. Intentional learners enter
// through subsequent prepared/add-learner steps in the same authorized block.
func foundingMatchesTarget(group *Group, status GroupStatus) bool {
	if group == nil || !status.Found || status.ConfState.AutoLeave || len(status.ConfState.VotersOutgoing) > 0 || len(status.ConfState.LearnersNext) > 0 {
		return false
	}
	expected := *group
	expected.Replicas = nil
	learners := map[uint64]bool{}
	for _, r := range group.Replicas {
		if r.Role == ReplicaVoter {
			expected.Replicas = append(expected.Replicas, r)
		} else {
			learners[r.ReplicaID] = true
		}
	}
	voters := status
	voters.ConfState.Learners = nil
	if !groupMatchesTarget(&expected, voters) {
		return false
	}
	seen := map[uint64]bool{}
	for _, id := range status.ConfState.Learners {
		if id == 0 || seen[id] || !learners[id] {
			return false
		}
		seen[id] = true
	}
	return true
}
func freshFoundingPlan(ex Record, step Step) bool {
	for _, before := range ex.Steps {
		if before.Seq >= step.Seq {
			break
		}
		if before.GroupID == step.GroupID && before.Kind == StepCreateFoundingGroup {
			return true
		}
	}
	return false
}
