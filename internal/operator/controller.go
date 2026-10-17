package operator

import (
	"context"
	"errors"
	"fmt"
	"slices"

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
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	stopping bool
	dispatch *controllerDispatch
}

type controllerOutcome uint8

const (
	controllerIdle controllerOutcome = iota
	controllerProgressed
	controllerWaiting
)

// Controller dispatches bounded, independently progressing approved group work.
// Replicated transitions and exact receipts preserve ownership across retries.
type Controller struct {
	rules           Rules
	scoped          Coordinator
	scopedPlan      ScopedValidator
	scopedFinal     ScopedValidator
	authority       AuthorityCoordinator
	groups          GroupEffects
	membership      MembershipCoordinator
	ownsGroup       func(groupID string) bool
	interval        time.Duration
	maxRetry        time.Duration
	proposalTimeout time.Duration
	clock           controllerClock

	mu     sync.Mutex
	run    *controllerRun
	wake   chan struct{}
	tickMu sync.Mutex
}

// Config supplies approved topology, exact effects, validators and execution
// timing.
type Config struct {
	Rules                Rules
	Coordinator          Coordinator
	ScopedPlanValidator  ScopedValidator
	ScopedFinalValidator ScopedValidator
	Authority            AuthorityCoordinator
	Groups               GroupEffects
	Membership           MembershipCoordinator
	OwnsGroup            func(groupID string) bool
	Interval             time.Duration
	MaxRetryInterval     time.Duration
	// ProposalTimeout bounds each replicated execution-record write the engine
	// makes, including progress, waits and failure. Zero selects the default.
	ProposalTimeout time.Duration

	clock controllerClock
}

// New validates execution dependencies and constructs an inactive topology
// controller.
func New(cfg Config) (*Controller, error) {
	if err := cfg.Rules.Limits.Validate(); err != nil || cfg.Rules.SchemaVersion != 1 {
		return nil, fault.New(fault.Invalid, "operator requires explicit valid cluster rules", err)
	}
	if cfg.Groups == nil || cfg.Authority == nil {
		return nil, fault.New(fault.Invalid,
			"operator requires authority coordinator and group effects", nil)
	}
	if cfg.OwnsGroup == nil {
		return nil, fault.New(fault.Invalid, "operator requires an immutable owns-group predicate", nil)
	}
	if err := validateCoordinatorConfig(cfg); err != nil {
		return nil, err
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
		rules: cfg.Rules, scoped: cfg.Coordinator,
		scopedPlan: cfg.ScopedPlanValidator, scopedFinal: cfg.ScopedFinalValidator,
		groups: cfg.Groups, authority: cfg.Authority,
		membership: cfg.Membership, ownsGroup: cfg.OwnsGroup,
		interval: cfg.Interval, maxRetry: cfg.MaxRetryInterval,
		proposalTimeout: cfg.ProposalTimeout, clock: cfg.clock,
		wake: make(chan struct{}, 1),
	}, nil
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

// Start begins the serialized execution loop under the supplied lifetime
// context.
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
	run := &controllerRun{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	run.dispatch = &controllerDispatch{}
	e.run = run
	e.mu.Unlock()

	go e.loop(ctx, run)
}

// Stop cancels the execution loop and waits for its current run to join.
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
	if run.dispatch != nil {
		run.dispatch.close()
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
		if run.dispatch != nil {
			run.dispatch.close()
			run.dispatch.workers.Wait()
		}
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
			outcome := e.runTick(ctx)
			retryDelay = e.nextDelay(outcome, retryDelay)
			timer.Reset(retryDelay)
		case <-e.wake:
			outcome := e.runTick(ctx)
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

// Tick selects bounded work. Failures and waits are recorded per group;
// leadership loss leaves the replicated work available for its next owner.
func (e *Controller) Tick(ctx context.Context) {
	e.runTick(ctx)
}

func (e *Controller) runTick(ctx context.Context) controllerOutcome {
	e.tickMu.Lock()
	defer e.tickMu.Unlock()
	if ctx.Err() != nil || !e.scoped.IsMetaLeader() {
		return controllerIdle
	}
	return e.runScopedTick(ctx)
}

type stepErrorOutcome struct {
	state  topology.OperationState
	reason string
	retry  bool
}

func classifyStepError(step Step, err error) stepErrorOutcome {
	if errors.Is(err, ErrReplicaNotReady) {
		if step.Kind == StepWaitCaughtUp || step.Kind == StepPromoteVoter {
			return stepErrorOutcome{state: topology.OperationWaitingForCatchUp, reason: err.Error(), retry: true}
		}
		return stepErrorOutcome{state: topology.OperationWaitingForReplica, reason: err.Error(), retry: true}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, ErrGroupUnavailable) ||
		errors.Is(err, ErrNoQuorum) || errors.Is(err, ErrNotMetaLeader) || errors.Is(err, fault.ErrAuthorityPending) ||
		errors.Is(err, fault.ErrOverloaded) {
		if step.Kind == StepPrepareReplica {
			return stepErrorOutcome{state: topology.OperationWaitingForReplica, reason: err.Error(), retry: true}
		}
		return stepErrorOutcome{state: topology.OperationWaitingForLeader, reason: err.Error(), retry: true}
	}
	return classifySafetyStepError(step, err)
}

func classifySafetyStepError(step Step, err error) stepErrorOutcome {
	var refusal *membership.SafetyRefusal
	if errors.As(err, &refusal) {
		if refusal.Reason == "replica is not a current learner; add it as a learner first" {
			return stepErrorOutcome{state: topology.OperationFailed}
		}
		if step.Kind == StepPromoteVoter || step.Kind == StepWaitCaughtUp {
			return stepErrorOutcome{state: topology.OperationWaitingForCatchUp, reason: err.Error(), retry: true}
		}
		if refusal.Reason == "replica liveness is unknown" ||
			refusal.Reason == "post-change live voter quorum would be unavailable" {
			return stepErrorOutcome{state: topology.OperationWaitingForReplica, reason: err.Error(), retry: true}
		}
	} else if errors.Is(err, ErrSafetyRefused) {
		// Caller transports should preserve SafetyRefusal. Without its reason,
		// waiting on promotion remains conservative; destructive refusals fail.
		if step.Kind == StepPromoteVoter || step.Kind == StepWaitCaughtUp {
			return stepErrorOutcome{state: topology.OperationWaitingForCatchUp, reason: err.Error(), retry: true}
		}
	}

	return stepErrorOutcome{state: topology.OperationFailed}
}

// fixedMembershipCoordinator keeps controller on the scoped GroupEffects
// path. The receiving group leader owns the concrete membership.Coordinator;
// CaughtUp applies the same fixed neutral status rule before promotion.
type fixedMembershipCoordinator struct{ groups GroupEffects }

// NewFixedMembershipCoordinator adapts group effects to the fixed membership
// safety coordinator.
func NewFixedMembershipCoordinator(groups GroupEffects) MembershipCoordinator {
	return &fixedMembershipCoordinator{groups: groups}
}

func (c *fixedMembershipCoordinator) Apply(ctx context.Context,
	cmd topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return c.groups.ApplyMembership(ctx, cmd)
}

func (*fixedMembershipCoordinator) CaughtUp(status GroupStatus, replicaID uint64) bool {
	ok, _ := caughtUpStatus(status, replicaID)
	return ok
}

// caughtUpStatus is CaughtUp plus the SPECIFIC reason it is not yet satisfied,
// so
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
		return false, fmt.Sprintf("learner lag %d exceeds limit %d", status.Commit-progress.Match,
			membership.PromotionLagLimit)
	}
	return true, ""
}

func (e *Controller) nativeEffectSatisfied(step Step, group *Group, status GroupStatus) bool {
	switch step.Kind {
	case StepCreateFoundingGroup:
		return status.Found && foundingMatchesTarget(group, status)
	case StepPrepareReplica, StepAddLearner:
		return containsID(status.ConfState.Learners, step.ReplicaID) ||
			containsID(status.ConfState.Voters, step.ReplicaID)
	case StepWaitCaughtUp:
		return containsID(status.ConfState.Voters, step.ReplicaID) || e.membership.CaughtUp(status, step.ReplicaID)
	case StepPromoteVoter:
		return containsID(status.ConfState.Voters, step.ReplicaID)
	case StepDemoteVoter:
		return containsID(status.ConfState.Learners, step.ReplicaID)
	case StepTransferLeader:
		return status.LeaderID == step.ReplicaID
	case StepRemoveReplica:
		return !containsID(status.ConfState.Voters, step.ReplicaID) &&
			!containsID(status.ConfState.Learners, step.ReplicaID)
	default:
		return false
	}
}

func groupMatchesTarget(group *Group, status GroupStatus) bool {
	if group == nil {
		return !status.Found
	}
	if !status.Found {
		return false
	}
	if status.ConfState.AutoLeave || len(status.ConfState.VotersOutgoing) != 0 ||
		len(status.ConfState.LearnersNext) != 0 {
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
	slices.Sort(voters)
	slices.Sort(learners)
	actualVoters := append([]uint64(nil), status.ConfState.Voters...)
	actualLearners := append([]uint64(nil), status.ConfState.Learners...)
	slices.Sort(actualVoters)
	slices.Sort(actualLearners)
	return equalUint64s(voters, actualVoters) && equalUint64s(learners, actualLearners)
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
	case StepCreateFoundingGroup,
		StepAddLearner,
		StepPromoteVoter,
		StepDemoteVoter,
		StepRemoveReplica,
		StepTransferLeader:
		return true
	}
	return false
}

// Fresh groups seed only their declared voters. Intentional learners enter
// through subsequent prepared/add-learner steps in the same authorized block.
func foundingMatchesTarget(group *Group, status GroupStatus) bool {
	if group == nil || !status.Found || status.ConfState.AutoLeave || len(status.ConfState.VotersOutgoing) > 0 ||
		len(status.ConfState.LearnersNext) > 0 {
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
