package convergence

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

type convergenceStore struct {
	mu             sync.Mutex
	latest         Document
	target         Document
	active         Document
	activationErr  error
	activationRuns int
}

func (s *convergenceStore) Current(context.Context) (Document, error) {
	return s.Latest(context.Background())
}
func (s *convergenceStore) Latest(context.Context) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest.Clone(), nil
}
func (s *convergenceStore) Target(context.Context) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.target.Clone(), nil
}
func (s *convergenceStore) Active(context.Context) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active.Clone(), nil
}
func (s *convergenceStore) History(context.Context, int, int) ([]Document, error) { return nil, nil }
func (s *convergenceStore) Get(_ context.Context, version uint64) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest.Version != version {
		return Document{}, ErrVersionUnknown
	}
	return s.latest.Clone(), nil
}
func (s *convergenceStore) Propose(context.Context, Document, uint64) (Document, error) {
	return Document{}, ErrNotImplemented
}
func (s *convergenceStore) Watch(context.Context) (<-chan Document, error) {
	ch := make(chan Document)
	close(ch)
	return ch, nil
}
func (s *convergenceStore) WatchActive(ctx context.Context) (<-chan Document, error) {
	return s.Watch(ctx)
}
func (s *convergenceStore) Close() error { return nil }

func (s *convergenceStore) activate() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activationRuns++
	if s.activationErr != nil {
		return s.activationErr
	}
	s.active = s.target.Clone()
	return nil
}

type convergenceCoordinator struct {
	mu            sync.Mutex
	leader        bool
	store         *convergenceStore
	execution     ExecutionRecord
	hasExecution  bool
	transitionErr error
	transitions   []ExecutionTransition
}

func (c *convergenceCoordinator) IsMetaLeader() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}
func (c *convergenceCoordinator) BeginExecution(_ context.Context, version uint64, plan Plan) (ExecutionID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hasExecution && c.execution.Version == version && !c.execution.State.Terminal() {
		return c.execution.ID, nil
	}
	attempt := uint32(1)
	if c.hasExecution && c.execution.Version == version {
		attempt = c.execution.Attempt + 1
	}
	c.execution = ExecutionRecord{
		ID: ExecutionIDFor(version, attempt), Version: version, Attempt: attempt,
		State: ExecutionPending, Steps: append([]Step(nil), plan.Steps...),
	}
	c.hasExecution = true
	c.store.mu.Lock()
	c.store.target = c.store.latest.Clone()
	c.store.mu.Unlock()
	return c.execution.ID, nil
}
func (c *convergenceCoordinator) CommitRepairExecution(context.Context, Document, uint64, Plan) (Document, ExecutionID, error) {
	return Document{}, "", ErrNotImplemented
}
func (c *convergenceCoordinator) Execution(_ context.Context, id ExecutionID) (ExecutionStatus, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hasExecution || c.execution.ID != id {
		return ExecutionStatus{}, false
	}
	return executionStatusFromRecord(c.execution), true
}
func (c *convergenceCoordinator) CurrentExecution(context.Context) (ExecutionRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneExecution(c.execution), c.hasExecution
}
func (c *convergenceCoordinator) RecordExecutionTransition(_ context.Context, transition ExecutionTransition) error {
	c.mu.Lock()
	if c.transitionErr != nil {
		err := c.transitionErr
		c.mu.Unlock()
		return err
	}
	if !c.hasExecution || transition.ExecutionID != c.execution.ID || transition.Version != c.execution.Version ||
		transition.Attempt != c.execution.Attempt {
		c.mu.Unlock()
		return ErrExecutionAuthorization
	}
	c.execution.State = transition.State
	c.execution.CurrentStep = transition.StepSeq
	c.execution.GroupID = transition.GroupID
	c.execution.NodeName = transition.NodeName
	c.execution.WaitingReason = transition.WaitingReason
	c.execution.Error = transition.Error
	c.transitions = append(c.transitions, transition)
	c.mu.Unlock()
	if transition.State == ExecutionSucceeded {
		return c.store.activate()
	}
	return nil
}
func (*convergenceCoordinator) MetaConfState() (raftpb.ConfState, bool) {
	return raftpb.ConfState{}, false
}
func (*convergenceCoordinator) MetaLeader() uint64                               { return 1 }
func (*convergenceCoordinator) TransferMetaLeader(context.Context, uint64) error { return nil }
func (*convergenceCoordinator) RemoveMetaVoter(context.Context, uint64) error    { return nil }

func cloneExecution(in ExecutionRecord) ExecutionRecord {
	in.Steps = append([]Step(nil), in.Steps...)
	return in
}

func executionStatusFromRecord(in ExecutionRecord) ExecutionStatus {
	return ExecutionStatus{
		ID: in.ID, Version: in.Version, Attempt: in.Attempt, State: in.State,
		CurrentStep: in.CurrentStep, TotalSteps: len(in.Steps),
		GroupID: in.GroupID, NodeName: in.NodeName, WaitingReason: in.WaitingReason, Error: in.Error,
	}
}

type convergenceGroups struct {
	mu       sync.Mutex
	store    *convergenceStore
	statuses map[string]GroupStatus
	errs     map[string]error
	calls    []StepKind
	before   func(context.Context, AuthorizedStep) error
	after    func(AuthorizedStep)
}

func (g *convergenceGroups) Status(_ context.Context, groupID string) (GroupStatus, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.errs[groupID]; err != nil {
		return GroupStatus{}, err
	}
	status := g.statuses[groupID]
	status.ConfState.Voters = append([]uint64(nil), status.ConfState.Voters...)
	status.ConfState.Learners = append([]uint64(nil), status.ConfState.Learners...)
	return status, nil
}
func (g *convergenceGroups) CreateGroup(ctx context.Context, cmd AuthorizedStep) error {
	if err := g.beforeCall(ctx, cmd); err != nil {
		return err
	}
	g.record(cmd.Kind)
	target, _ := g.store.Target(context.Background())
	group := target.Group(cmd.GroupID)
	status := GroupStatus{GroupID: cmd.GroupID, HostNode: "n1", Found: true, LeaderID: 1}
	for _, replica := range group.Replicas {
		if replica.Role == ReplicaVoter {
			status.ConfState.Voters = append(status.ConfState.Voters, replica.ReplicaID)
		}
	}
	g.setStatus(cmd.GroupID, status)
	g.afterCall(cmd)
	return nil
}
func (g *convergenceGroups) PrepareReplica(ctx context.Context, cmd AuthorizedStep) error {
	if err := g.beforeCall(ctx, cmd); err != nil {
		return err
	}
	g.record(cmd.Kind)
	g.afterCall(cmd)
	return nil
}
func (g *convergenceGroups) ApplyMembership(ctx context.Context, cmd AuthorizedStep) error {
	if err := g.beforeCall(ctx, cmd); err != nil {
		return err
	}
	g.record(cmd.Kind)
	g.mu.Lock()
	status := g.statuses[cmd.GroupID]
	switch cmd.Kind {
	case StepAddLearner:
		status.ConfState.Learners = addID(status.ConfState.Learners, cmd.ReplicaID)
	case StepPromoteVoter:
		status.ConfState.Learners = removeID(status.ConfState.Learners, cmd.ReplicaID)
		status.ConfState.Voters = addID(status.ConfState.Voters, cmd.ReplicaID)
	case StepDemoteVoter:
		status.ConfState.Voters = removeID(status.ConfState.Voters, cmd.ReplicaID)
		status.ConfState.Learners = addID(status.ConfState.Learners, cmd.ReplicaID)
	case StepRemoveReplica:
		status.ConfState.Voters = removeID(status.ConfState.Voters, cmd.ReplicaID)
		status.ConfState.Learners = removeID(status.ConfState.Learners, cmd.ReplicaID)
	}
	g.statuses[cmd.GroupID] = status
	g.mu.Unlock()
	g.afterCall(cmd)
	return nil
}
func (g *convergenceGroups) TransferLeader(ctx context.Context, cmd AuthorizedStep) error {
	if err := g.beforeCall(ctx, cmd); err != nil {
		return err
	}
	g.record(cmd.Kind)
	g.mu.Lock()
	status := g.statuses[cmd.GroupID]
	status.LeaderID = cmd.ReplicaID
	g.statuses[cmd.GroupID] = status
	g.mu.Unlock()
	g.afterCall(cmd)
	return nil
}
func (g *convergenceGroups) StopReplica(ctx context.Context, cmd AuthorizedStep) error {
	if err := g.beforeCall(ctx, cmd); err != nil {
		return err
	}
	g.record(cmd.Kind)
	g.afterCall(cmd)
	return nil
}
func (g *convergenceGroups) CleanupReplicaState(ctx context.Context, cmd AuthorizedStep) error {
	if err := g.beforeCall(ctx, cmd); err != nil {
		return err
	}
	g.record(cmd.Kind)
	g.afterCall(cmd)
	return nil
}
func (g *convergenceGroups) DeleteGroupState(ctx context.Context, cmd AuthorizedStep) error {
	if err := g.beforeCall(ctx, cmd); err != nil {
		return err
	}
	g.record(cmd.Kind)
	g.setStatus(cmd.GroupID, GroupStatus{GroupID: cmd.GroupID})
	g.afterCall(cmd)
	return nil
}
func (g *convergenceGroups) beforeCall(ctx context.Context, cmd AuthorizedStep) error {
	g.mu.Lock()
	hook := g.before
	g.mu.Unlock()
	if hook != nil {
		return hook(ctx, cmd)
	}
	return nil
}
func (g *convergenceGroups) afterCall(cmd AuthorizedStep) {
	g.mu.Lock()
	hook := g.after
	g.mu.Unlock()
	if hook != nil {
		hook(cmd)
	}
}
func (g *convergenceGroups) record(kind StepKind) {
	g.mu.Lock()
	g.calls = append(g.calls, kind)
	g.mu.Unlock()
}
func (g *convergenceGroups) setStatus(id string, status GroupStatus) {
	g.mu.Lock()
	g.statuses[id] = status
	g.mu.Unlock()
}

func addID(ids []uint64, id uint64) []uint64 {
	if containsID(ids, id) {
		return ids
	}
	return append(ids, id)
}
func removeID(ids []uint64, id uint64) []uint64 {
	out := ids[:0]
	for _, current := range ids {
		if current != id {
			out = append(out, current)
		}
	}
	return out
}

type convergenceFixture struct {
	store  *convergenceStore
	coord  *convergenceCoordinator
	groups *convergenceGroups
	engine *ConvergenceEngine
}

type fakeConvergenceClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeConvergenceTimer
}

func newFakeConvergenceClock() *fakeConvergenceClock {
	return &fakeConvergenceClock{now: time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeConvergenceClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeConvergenceClock) NewTimer(delay time.Duration) convergenceTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeConvergenceTimer{clock: c, ch: make(chan time.Time, 1), resets: []time.Duration{delay}}
	c.timers = append(c.timers, timer)
	return timer
}

func (c *fakeConvergenceClock) timerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

func (c *fakeConvergenceClock) timer(index int) *fakeConvergenceTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timers[index]
}

type fakeConvergenceTimer struct {
	mu      sync.Mutex
	clock   *fakeConvergenceClock
	ch      chan time.Time
	resets  []time.Duration
	stopped bool
}

func (t *fakeConvergenceTimer) C() <-chan time.Time { return t.ch }

func (t *fakeConvergenceTimer) Reset(delay time.Duration) {
	t.mu.Lock()
	t.resets = append(t.resets, delay)
	t.mu.Unlock()
}

func (t *fakeConvergenceTimer) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
}

func (t *fakeConvergenceTimer) fire() {
	t.clock.mu.Lock()
	t.clock.now = t.clock.now.Add(time.Second)
	now := t.clock.now
	t.clock.mu.Unlock()
	t.ch <- now
}

func (t *fakeConvergenceTimer) snapshot() ([]time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]time.Duration(nil), t.resets...), t.stopped
}

func newConvergenceFixture(active, target Document, steps []Step, current int, state ExecutionState) *convergenceFixture {
	store := &convergenceStore{latest: target.Clone(), target: target.Clone(), active: active.Clone()}
	coord := &convergenceCoordinator{leader: true, store: store, hasExecution: true}
	coord.execution = ExecutionRecord{
		ID: ExecutionIDFor(target.Version, 1), Version: target.Version, Attempt: 1,
		State: state, Steps: append([]Step(nil), steps...), CurrentStep: current,
	}
	groups := &convergenceGroups{store: store, statuses: map[string]GroupStatus{}, errs: map[string]error{}}
	engine := NewConvergenceEngine(ConvergenceConfig{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: store, Coordinator: coord, Groups: groups,
		OwnsGroup: func(groupID string) bool { return groupID != MetaGroupID },
	})
	return &convergenceFixture{store: store, coord: coord, groups: groups, engine: engine}
}

func growDocuments() (Document, Document, Replica) {
	target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
	target.Version = 2
	active := target.Clone()
	active.Version = 1
	active.Group("shard0").Replicas = active.Group("shard0").Replicas[:1]
	return active, target, target.Group("shard0").Replicas[1]
}

func statusFor(group string, voters, learners []uint64, leader uint64) GroupStatus {
	return GroupStatus{GroupID: group, HostNode: "n1", Found: true, LeaderID: leader,
		Commit: 100, ConfState: raftpb.ConfState{Voters: voters, Learners: learners},
		Progress: map[uint64]ReplicaProgress{2: {Match: 100, RecentActive: true}}}
}

func TestConvergence_LearnerFirstSteps(t *testing.T) {
	active, target, replica := growDocuments()
	tests := []struct {
		name    string
		steps   []Step
		current int
		state   ExecutionState
		status  GroupStatus
		want    StepKind
		wantSeq int
		final   bool
	}{
		{"prepare", []Step{{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}, {Seq: 2, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}}, 0, ExecutionPending, statusFor("shard0", []uint64{1}, nil, 1), StepPrepareReplica, 2, false},
		{"add learner", []Step{{Seq: 1, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}, {Seq: 2, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}}, 0, ExecutionPending, statusFor("shard0", []uint64{1}, nil, 1), StepAddLearner, 2, false},
		{"wait caught up", []Step{{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}, {Seq: 2, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}}, 0, ExecutionPending, statusFor("shard0", []uint64{1}, []uint64{2}, 1), "", 2, false},
		{"promote", []Step{{Seq: 1, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}}, 0, ExecutionPending, statusFor("shard0", []uint64{1}, []uint64{2}, 1), StepPromoteVoter, 1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newConvergenceFixture(active, target, tc.steps, tc.current, tc.state)
			f.groups.statuses["shard0"] = tc.status
			f.engine.Tick(context.Background())
			ex, _ := f.coord.CurrentExecution(context.Background())
			if tc.final {
				assert.Equal(t, ExecutionSucceeded, ex.State)
			} else {
				assert.Equal(t, tc.wantSeq, ex.CurrentStep)
			}
			if tc.want == "" {
				assert.Empty(t, f.groups.calls)
			} else {
				assert.Equal(t, []StepKind{tc.want}, f.groups.calls)
			}
		})
	}
}

func TestConvergence_FinalValidatorFailureLeavesActiveUnchanged(t *testing.T) {
	active, target, replica := growDocuments()
	step := Step{Seq: 1, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newConvergenceFixture(active, target, []Step{step}, 0, ExecutionPending)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, []uint64{2}, 1)
	f.engine.SetFinalValidator(func(context.Context, Document, Document) error {
		return errors.New("meta health changed")
	})
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentExecution(context.Background())
	if ex.State != ExecutionFailed {
		t.Fatalf("execution state = %q, want failed", ex.State)
	}
	got, _ := f.store.Active(context.Background())
	if got.Version != active.Version {
		t.Fatalf("Active advanced to %d after final safety failure", got.Version)
	}
}

func TestConvergence_ForcedRetirementWaivesOnlyDeadNodeTeardown(t *testing.T) {
	active := threeNodeDoc()
	target := active.Clone()
	target.Version = 2
	target.Node("n3").State = NodeExcluded
	removeReplicasOf(&target, "n3")
	removeMetaReplicaOf(&target, "n3")
	target.Audit = &ChangeAudit{Operation: AuditOperationForceRetireNode, TargetNode: "n3", Forced: true,
		Reason: "host lost", StagedBy: "admin", StagedAt: time.Now().UTC()}
	steps := []Step{
		{Seq: 1, Kind: StepRemoveReplica, GroupID: "shard0", NodeName: "n3", ReplicaID: 3},
		{Seq: 2, Kind: StepStopReplica, GroupID: "shard0", NodeName: "n3", ReplicaID: 3},
		{Seq: 3, Kind: StepCleanupReplicaState, GroupID: "shard0", NodeName: "n3", ReplicaID: 3},
	}
	f := newConvergenceFixture(active, target, steps, 2, ExecutionRunning)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1, 2}, nil, 1)
	f.engine.Tick(context.Background())
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentExecution(context.Background())
	if ex.State != ExecutionSucceeded {
		t.Fatalf("forced execution state = %q, want succeeded", ex.State)
	}
	if len(f.groups.calls) != 0 {
		t.Fatalf("unreachable target teardown was called: %v", f.groups.calls)
	}
}

func TestConvergence_DestructiveAndLifecycleSteps(t *testing.T) {
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
	target := active.Clone()
	target.Version = 2
	target.Group("shard0").Replicas = target.Group("shard0").Replicas[:1]
	removed := active.Group("shard0").Replicas[1]
	remove := Step{Seq: 1, Kind: StepRemoveReplica, GroupID: "shard0", NodeName: removed.NodeName, ReplicaID: removed.ReplicaID}
	stop := Step{Seq: 2, Kind: StepStopReplica, GroupID: "shard0", NodeName: removed.NodeName, ReplicaID: removed.ReplicaID}
	cleanup := Step{Seq: 3, Kind: StepCleanupReplicaState, GroupID: "shard0", NodeName: removed.NodeName, ReplicaID: removed.ReplicaID}

	f := newConvergenceFixture(active, target, []Step{remove, stop, cleanup}, 0, ExecutionPending)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1, 2}, nil, 1)
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentExecution(context.Background())
	require.Equal(t, 2, ex.CurrentStep)
	f.engine.Tick(context.Background())
	ex, _ = f.coord.CurrentExecution(context.Background())
	require.Equal(t, 3, ex.CurrentStep)
	f.engine.Tick(context.Background())
	ex, _ = f.coord.CurrentExecution(context.Background())
	assert.Equal(t, ExecutionSucceeded, ex.State)
	assert.Equal(t, []StepKind{StepRemoveReplica, StepStopReplica, StepCleanupReplicaState}, f.groups.calls)
}

func TestConvergence_DeleteShardStopsEveryHostBeforeSharedDelete(t *testing.T) {
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard9"})
	target := active.Clone()
	target.Version = 2
	target.Groups = target.Groups[:2]
	steps := []Step{
		{Seq: 1, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n2", ReplicaID: 2},
		{Seq: 3, Kind: StepDeleteGroupState, GroupID: "shard9"},
	}
	f := newConvergenceFixture(active, target, steps, 0, ExecutionPending)
	f.groups.statuses["shard9"] = statusFor("shard9", []uint64{1, 2}, nil, 1)
	f.groups.after = func(cmd AuthorizedStep) {
		if cmd.Kind == StepStopReplica && cmd.NodeName == "n2" {
			f.groups.setStatus("shard9", GroupStatus{GroupID: "shard9"})
		}
	}
	for range 4 {
		f.engine.Tick(context.Background())
	}
	ex, _ := f.coord.CurrentExecution(context.Background())
	if ex.State != ExecutionSucceeded {
		t.Fatalf("delete execution = %+v", ex)
	}
	assert.Equal(t, []StepKind{StepStopReplica, StepStopReplica, StepDeleteGroupState}, f.groups.calls)
}

func TestConvergence_DeleteShardWaitsForUnreachableHost(t *testing.T) {
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard9"})
	target := active.Clone()
	target.Version = 2
	target.Groups = target.Groups[:2]
	steps := []Step{
		{Seq: 1, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n2", ReplicaID: 2},
		{Seq: 3, Kind: StepDeleteGroupState, GroupID: "shard9"},
	}
	f := newConvergenceFixture(active, target, steps, 2, ExecutionRunning)
	f.groups.errs["shard9"] = fmtError(ErrGroupUnavailable, "n2 unreachable")
	f.groups.before = func(_ context.Context, cmd AuthorizedStep) error {
		if cmd.Kind == StepStopReplica && cmd.NodeName == "n2" {
			return fmtError(ErrGroupUnavailable, "n2 unreachable")
		}
		return nil
	}
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentExecution(context.Background())
	if ex.CurrentStep != 2 || ex.State.Terminal() {
		t.Fatalf("unreachable stop advanced execution: %+v", ex)
	}
	assert.NotContains(t, f.groups.calls, StepDeleteGroupState)
}

func TestConvergence_DeleteShardCompletesAfterGroupLosesLeader(t *testing.T) {
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard9"})
	target := active.Clone()
	target.Version = 2
	target.Groups = target.Groups[:2]
	steps := []Step{
		{Seq: 1, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n2", ReplicaID: 2},
		{Seq: 3, Kind: StepDeleteGroupState, GroupID: "shard9"},
	}
	f := newConvergenceFixture(active, target, steps, 2, ExecutionRunning)
	f.groups.errs["shard9"] = fmtError(ErrGroupUnavailable, "no leader")
	for range 3 {
		f.engine.Tick(context.Background())
	}
	ex, _ := f.coord.CurrentExecution(context.Background())
	assert.Equal(t, ExecutionSucceeded, ex.State)
	assert.Equal(t, []StepKind{StepStopReplica, StepDeleteGroupState}, f.groups.calls)
}

func TestConvergence_DeleteShardContinuesAfterGroupBecomesAbsent(t *testing.T) {
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard9"})
	target := active.Clone()
	target.Version = 2
	target.Groups = target.Groups[:2]
	steps := []Step{
		{Seq: 1, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n2", ReplicaID: 2},
		{Seq: 3, Kind: StepDeleteGroupState, GroupID: "shard9"},
	}
	f := newConvergenceFixture(active, target, steps, 2, ExecutionRunning)
	f.groups.statuses["shard9"] = GroupStatus{GroupID: "shard9"}
	for range 3 {
		f.engine.Tick(context.Background())
	}
	ex, _ := f.coord.CurrentExecution(context.Background())
	assert.Equal(t, ExecutionSucceeded, ex.State)
	assert.Equal(t, []StepKind{StepStopReplica, StepDeleteGroupState}, f.groups.calls)
}

func TestConvergence_FoundingDemotionTransferAndDelete(t *testing.T) {
	base := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
	t.Run("founding", func(t *testing.T) {
		target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard1"})
		target.Version = 2
		step := Step{Seq: 1, Kind: StepCreateFoundingGroup, GroupID: "shard1"}
		f := newConvergenceFixture(base, target, []Step{step}, 0, ExecutionPending)
		f.groups.statuses["shard1"] = GroupStatus{GroupID: "shard1"}
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionSucceeded, ex.State)
	})
	t.Run("founding resumes when partial group has no leader", func(t *testing.T) {
		target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard1"})
		target.Version = 2
		step := Step{Seq: 1, Kind: StepCreateFoundingGroup, GroupID: "shard1"}
		f := newConvergenceFixture(base, target, []Step{step}, 1, ExecutionWaitingLeader)
		f.groups.errs["shard1"] = fmtError(ErrGroupUnavailable, "partial founding has no leader")
		f.groups.before = func(_ context.Context, cmd AuthorizedStep) error {
			f.groups.mu.Lock()
			delete(f.groups.errs, cmd.GroupID)
			f.groups.mu.Unlock()
			return nil
		}

		f.engine.Tick(context.Background())

		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionSucceeded, ex.State)
		assert.Equal(t, []StepKind{StepCreateFoundingGroup}, f.groups.calls)
	})
	t.Run("demotion", func(t *testing.T) {
		active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
		target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
		target.Version = 2
		replica := &target.Group("shard0").Replicas[1]
		replica.Role = ReplicaLearner
		step := Step{Seq: 1, Kind: StepDemoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		f := newConvergenceFixture(active, target, []Step{step}, 0, ExecutionPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1, 2}, nil, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionSucceeded, ex.State)
	})
	t.Run("transfer", func(t *testing.T) {
		target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
		target.Version = 2
		active := target.Clone()
		active.Version = 1
		replica := target.Group("shard0").Replicas[1]
		step := Step{Seq: 1, Kind: StepTransferLeader, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		f := newConvergenceFixture(active, target, []Step{step}, 0, ExecutionPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1, 2}, nil, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionSucceeded, ex.State)
	})
	t.Run("delete", func(t *testing.T) {
		active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard1"})
		target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
		target.Version = 2
		step := Step{Seq: 1, Kind: StepDeleteGroupState, GroupID: "shard1"}
		f := newConvergenceFixture(active, target, []Step{step}, 0, ExecutionPending)
		f.groups.statuses["shard1"] = statusFor("shard1", []uint64{1}, nil, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionSucceeded, ex.State)
	})
}

func TestConvergence_WaitFailureOwnershipAndCancellation(t *testing.T) {
	active, target, replica := growDocuments()
	prepare := Step{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	t.Run("unreachable prepare waits", func(t *testing.T) {
		f := newConvergenceFixture(active, target, []Step{prepare}, 0, ExecutionPending)
		f.groups.errs["shard0"] = fmtError(ErrGroupUnavailable, "target unreachable")
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionWaitingReplica, ex.State)
	})
	t.Run("catch up waits", func(t *testing.T) {
		wait := Step{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		promote := Step{Seq: 2, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		f := newConvergenceFixture(active, target, []Step{wait, promote}, 0, ExecutionPending)
		status := statusFor("shard0", []uint64{1}, []uint64{2}, 1)
		status.Progress[2] = ReplicaProgress{Match: 50, RecentActive: false}
		f.groups.statuses["shard0"] = status
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionWaitingCatchUp, ex.State)
	})
	t.Run("identity mismatch fails", func(t *testing.T) {
		bad := prepare
		bad.ReplicaID = 99
		f := newConvergenceFixture(active, target, []Step{bad}, 0, ExecutionPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionFailed, ex.State)
	})
	t.Run("other plane does not transition", func(t *testing.T) {
		meta := prepare
		meta.GroupID = MetaGroupID
		f := newConvergenceFixture(active, target, []Step{meta}, 0, ExecutionPending)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionPending, ex.State)
		assert.Zero(t, ex.CurrentStep)
	})
	t.Run("meta leadership loss does not transition", func(t *testing.T) {
		f := newConvergenceFixture(active, target, []Step{prepare}, 0, ExecutionPending)
		f.coord.leader = false
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionPending, ex.State)
		assert.Zero(t, ex.CurrentStep)
	})
	t.Run("idempotent replay advances without side effect", func(t *testing.T) {
		add := Step{Seq: 1, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		wait := Step{Seq: 2, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		f := newConvergenceFixture(active, target, []Step{add, wait}, 0, ExecutionPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, []uint64{2}, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, 2, ex.CurrentStep)
		assert.Empty(t, f.groups.calls)
	})
	t.Run("cleanup without exact remove and stop fails", func(t *testing.T) {
		cleanup := Step{Seq: 1, Kind: StepCleanupReplicaState, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}
		f := newConvergenceFixture(active, target, []Step{cleanup}, 0, ExecutionPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionFailed, ex.State)
		assert.Empty(t, f.groups.calls)
	})
	t.Run("cancelled tick does nothing", func(t *testing.T) {
		f := newConvergenceFixture(active, target, []Step{prepare}, 0, ExecutionPending)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		f.engine.Tick(ctx)
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionPending, ex.State)
	})
}

func TestConvergence_EmptyPlanAndActivationFailure(t *testing.T) {
	target := clusterDoc(1, 1, []string{"n1"}, []string{"shard0"})
	target.Version = 2
	active := target.Clone()
	active.Version = 1
	f := newConvergenceFixture(active, target, nil, 0, ExecutionPending)
	f.store.activationErr = errors.New("activation failed")
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentExecution(context.Background())
	assert.Equal(t, ExecutionSucceeded, ex.State, "success transition committed before activation proposal failed")
	got, _ := f.store.Active(context.Background())
	assert.Equal(t, uint64(1), got.Version, "Active must not advance when activation fails")
}

type countingSignal struct{ calls int }

func (s *countingSignal) Signal() { s.calls++ }

func TestConvergence_FailoverResumesExactStep(t *testing.T) {
	active, target, replica := growDocuments()
	tests := []struct {
		name      string
		steps     []Step
		wantCalls int
		wantState ExecutionState
		wantStep  int
	}{
		{
			name: "prepare is replayed idempotently",
			steps: []Step{
				{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
				{Seq: 2, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
			},
			wantCalls: 2, wantState: ExecutionRunning, wantStep: 2,
		},
		{
			name: "learner addition is observed live",
			steps: []Step{
				{Seq: 1, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
				{Seq: 2, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
			},
			wantCalls: 1, wantState: ExecutionRunning, wantStep: 2,
		},
		{
			name: "promotion is observed live",
			steps: []Step{
				{Seq: 1, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
			},
			wantCalls: 1, wantState: ExecutionSucceeded, wantStep: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newConvergenceFixture(active, target, tc.steps, 0, ExecutionPending)
			status := statusFor("shard0", []uint64{1}, nil, 1)
			if tc.steps[0].Kind == StepPromoteVoter {
				status.ConfState.Learners = []uint64{replica.ReplicaID}
			}
			f.groups.statuses["shard0"] = status
			var once sync.Once
			f.groups.after = func(AuthorizedStep) {
				once.Do(func() {
					f.coord.mu.Lock()
					f.coord.leader = false
					f.coord.mu.Unlock()
				})
			}

			f.engine.Tick(context.Background())
			ex, _ := f.coord.CurrentExecution(context.Background())
			assert.Equal(t, ExecutionRunning, ex.State)
			assert.Equal(t, 1, ex.CurrentStep)

			f.coord.mu.Lock()
			f.coord.leader = true
			f.coord.mu.Unlock()
			f.engine.Tick(context.Background())
			ex, _ = f.coord.CurrentExecution(context.Background())
			assert.Equal(t, tc.wantState, ex.State)
			assert.Equal(t, tc.wantStep, ex.CurrentStep)
			assert.Len(t, f.groups.calls, tc.wantCalls)
		})
	}
}

func TestConvergence_SupersedesOnlyAtStepBoundary(t *testing.T) {
	active, target, replica := growDocuments()
	add := Step{Seq: 1, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}

	t.Run("newer target supersedes before work", func(t *testing.T) {
		f := newConvergenceFixture(active, target, []Step{add}, 0, ExecutionPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
		f.store.mu.Lock()
		f.store.target.Version++
		f.store.mu.Unlock()

		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionSuperseded, ex.State)
		assert.Empty(t, f.groups.calls)
	})

	t.Run("in flight membership change finishes without advancing old execution", func(t *testing.T) {
		f := newConvergenceFixture(active, target, []Step{add}, 0, ExecutionPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
		started := make(chan struct{})
		release := make(chan struct{})
		f.groups.after = func(AuthorizedStep) {
			close(started)
			<-release
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			f.engine.Tick(context.Background())
		}()
		<-started

		newTarget := target.Clone()
		newTarget.Version = 3
		f.store.mu.Lock()
		f.store.latest = newTarget.Clone()
		f.store.target = newTarget.Clone()
		f.store.mu.Unlock()
		f.coord.mu.Lock()
		f.coord.execution = ExecutionRecord{
			ID: ExecutionIDFor(3, 1), Version: 3, Attempt: 1, State: ExecutionPending,
			Steps: []Step{{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}},
		}
		f.coord.mu.Unlock()
		close(release)
		<-done

		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, ExecutionIDFor(3, 1), ex.ID)
		assert.Equal(t, ExecutionPending, ex.State)
		assert.Zero(t, ex.CurrentStep)
		assert.Equal(t, []StepKind{StepAddLearner}, f.groups.calls)
	})
}

func TestConvergence_ResumePreservesDurableWaiting(t *testing.T) {
	active, target, replica := growDocuments()
	wait := Step{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	promote := Step{Seq: 2, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newConvergenceFixture(active, target, []Step{wait, promote}, 1, ExecutionWaitingCatchUp)
	status := statusFor("shard0", []uint64{1}, []uint64{2}, 1)
	status.Progress[2] = ReplicaProgress{Match: 1, RecentActive: false}
	f.groups.statuses["shard0"] = status

	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentExecution(context.Background())
	require.Equal(t, ExecutionWaitingCatchUp, ex.State)
	require.Equal(t, 1, ex.CurrentStep)

	// A replacement process reconstructs only the engine. Replicated waiting
	// state and the exact cursor remain authoritative; no local timer state is
	// needed to resume live observation.
	f.engine = NewConvergenceEngine(ConvergenceConfig{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: f.store, Coordinator: f.coord, Groups: f.groups,
		OwnsGroup: func(groupID string) bool { return groupID != MetaGroupID },
	})
	status.Progress[2] = ReplicaProgress{Match: 100, RecentActive: true}
	f.groups.setStatus("shard0", status)
	f.engine.Tick(context.Background())
	ex, _ = f.coord.CurrentExecution(context.Background())
	assert.Equal(t, ExecutionRunning, ex.State)
	assert.Equal(t, 2, ex.CurrentStep)
}

func TestConvergence_ResumeMixedPlaneOwnership(t *testing.T) {
	target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
	target.Version = 2
	active := target.Clone()
	active.Version = 1
	active.Group(MetaGroupID).Replicas = active.Group(MetaGroupID).Replicas[:1]
	active.Group("shard0").Replicas = active.Group("shard0").Replicas[:1]
	steps := []Step{
		{Seq: 1, Kind: StepAddLearner, GroupID: MetaGroupID, NodeName: "n2", ReplicaID: 2},
		{Seq: 2, Kind: StepAddLearner, GroupID: "shard0", NodeName: "n2", ReplicaID: 2},
		{Seq: 3, Kind: StepWaitCaughtUp, GroupID: MetaGroupID, NodeName: "n2", ReplicaID: 2},
		{Seq: 4, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: "n2", ReplicaID: 2},
		{Seq: 5, Kind: StepPromoteVoter, GroupID: MetaGroupID, NodeName: "n2", ReplicaID: 2},
		{Seq: 6, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: "n2", ReplicaID: 2},
	}
	f := newConvergenceFixture(active, target, steps, 0, ExecutionPending)
	f.groups.statuses[MetaGroupID] = statusFor(MetaGroupID, []uint64{1}, nil, 1)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
	metaEngine := NewConvergenceEngine(ConvergenceConfig{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: f.store, Coordinator: f.coord, Groups: f.groups,
		OwnsGroup: func(groupID string) bool { return groupID == MetaGroupID },
	})
	dataEngine := f.engine

	for wantStep := 2; wantStep <= len(steps); wantStep++ {
		before, _ := f.coord.CurrentExecution(context.Background())
		stepIndex := before.CurrentStep - 1
		if stepIndex < 0 {
			stepIndex = 0
		}
		if steps[stepIndex].GroupID == MetaGroupID {
			dataEngine.Tick(context.Background())
			unchanged, _ := f.coord.CurrentExecution(context.Background())
			assert.Equal(t, before.CurrentStep, unchanged.CurrentStep)
			metaEngine.Tick(context.Background())
		} else {
			metaEngine.Tick(context.Background())
			unchanged, _ := f.coord.CurrentExecution(context.Background())
			assert.Equal(t, before.CurrentStep, unchanged.CurrentStep)
			dataEngine.Tick(context.Background())
		}
		ex, _ := f.coord.CurrentExecution(context.Background())
		assert.Equal(t, wantStep, ex.CurrentStep)
	}
	dataEngine.Tick(context.Background())
	ex, _ := f.coord.CurrentExecution(context.Background())
	assert.Equal(t, ExecutionSucceeded, ex.State)
}

func TestConvergence_ShutdownCancelsBlockedWorkAndIsIdempotent(t *testing.T) {
	active, target, replica := growDocuments()
	prepare := Step{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newConvergenceFixture(active, target, []Step{prepare}, 0, ExecutionPending)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
	clock := newFakeConvergenceClock()
	f.engine = NewConvergenceEngine(ConvergenceConfig{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: f.store, Coordinator: f.coord, Groups: f.groups, Interval: time.Second,
		OwnsGroup: func(groupID string) bool { return groupID != MetaGroupID }, clock: clock,
	})
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	f.groups.before = func(ctx context.Context, _ AuthorizedStep) error {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return ctx.Err()
	}

	f.engine.Start(context.Background())
	f.engine.Start(context.Background())
	require.Eventually(t, func() bool { return clock.timerCount() == 1 }, time.Second, time.Millisecond)
	clock.timer(0).fire()
	<-entered
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		f.engine.Stop()
	}()
	<-cancelled
	f.engine.Start(context.Background())
	assert.Equal(t, 1, clock.timerCount(), "Start during Stop must not create a second run")
	close(release)
	<-stopped
	f.engine.Stop()
	_, timerStopped := clock.timer(0).snapshot()
	assert.True(t, timerStopped)

	f.coord.mu.Lock()
	transitionsAfterStop := len(f.coord.transitions)
	f.coord.mu.Unlock()
	f.engine.Signal()
	time.Sleep(5 * time.Millisecond)
	f.coord.mu.Lock()
	assert.Len(t, f.coord.transitions, transitionsAfterStop)
	f.coord.mu.Unlock()

	f.engine.Start(context.Background())
	require.Eventually(t, func() bool { return clock.timerCount() == 2 }, time.Second, time.Millisecond)
	f.engine.Stop()
	_, secondStopped := clock.timer(1).snapshot()
	assert.True(t, secondStopped)
}

func TestConvergence_ShutdownReusesBoundedRetryTimer(t *testing.T) {
	active, target, replica := growDocuments()
	prepare := Step{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newConvergenceFixture(active, target, []Step{prepare}, 0, ExecutionPending)
	f.groups.errs["shard0"] = ErrGroupUnavailable
	clock := newFakeConvergenceClock()
	f.engine = NewConvergenceEngine(ConvergenceConfig{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: f.store, Coordinator: f.coord, Groups: f.groups,
		OwnsGroup: func(groupID string) bool { return groupID != MetaGroupID },
		Interval:  time.Second, MaxRetryInterval: 5 * time.Second, clock: clock,
	})
	f.engine.Start(context.Background())
	require.Eventually(t, func() bool { return clock.timerCount() == 1 }, time.Second, time.Millisecond)
	timer := clock.timer(0)
	for wantResets := 2; wantResets <= 5; wantResets++ {
		timer.fire()
		require.Eventually(t, func() bool {
			resets, _ := timer.snapshot()
			return len(resets) == wantResets
		}, time.Second, time.Millisecond)
	}
	f.engine.Stop()
	resets, stopped := timer.snapshot()
	assert.Equal(t, []time.Duration{0, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}, resets)
	assert.True(t, stopped)
	assert.Equal(t, 1, clock.timerCount())
}

func TestConvergence_ResumeSerializesConcurrentTicks(t *testing.T) {
	active, target, replica := growDocuments()
	prepare := Step{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newConvergenceFixture(active, target, []Step{prepare}, 0, ExecutionPending)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	activeCalls, maxActive, totalCalls := 0, 0, 0
	f.groups.before = func(context.Context, AuthorizedStep) error {
		mu.Lock()
		activeCalls++
		totalCalls++
		if activeCalls > maxActive {
			maxActive = activeCalls
		}
		if totalCalls == 1 {
			close(entered)
		}
		mu.Unlock()
		<-release
		mu.Lock()
		activeCalls--
		mu.Unlock()
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		f.engine.Tick(context.Background())
	}()
	<-entered
	go func() {
		defer wg.Done()
		f.engine.Tick(context.Background())
	}()
	time.Sleep(5 * time.Millisecond)
	mu.Lock()
	assert.Equal(t, 1, totalCalls)
	mu.Unlock()
	close(release)
	wg.Wait()
	mu.Lock()
	assert.Equal(t, 1, maxActive)
	mu.Unlock()
}

func fmtError(base error, detail string) error { return errors.Join(base, errors.New(detail)) }

// blockingConvergenceCoordinator simulates a SILENTLY-lost raft proposal: the
// entry was appended but truncated by a leadership change, so it never applies
// and the store-side waiter resolves only when the caller's context does.
type blockingConvergenceCoordinator struct {
	*convergenceCoordinator
	entered chan struct{}
	sawErr  chan error
}

func (c *blockingConvergenceCoordinator) RecordExecutionTransition(ctx context.Context, _ ExecutionTransition) error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-ctx.Done() // never applies; only the caller's deadline frees the waiter
	select {
	case c.sawErr <- ctx.Err():
	default:
	}
	return ctx.Err()
}

// TestConvergence_TickSurvivesLostProposal pins the meta-leader-change freeze:
// a transition proposal lost across a term boundary must cost the engine one
// bounded wait, not park the tick goroutine forever (tickMu held, no further
// ticks — the exact stall TestClusterAPI_ExecutionResumesAfterMetaLeaderChange
// reproduces). The engine's loop context is cancel-only, so without the
// per-proposal deadline this test hangs.
func TestConvergence_TickSurvivesLostProposal(t *testing.T) {
	active, target, replica := growDocuments()
	steps := []Step{
		{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
		{Seq: 2, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
	}
	// waiting_for_catch_up at step 1: the first write any tick makes is
	// ensureRunning's waiting→running transition — the call that froze.
	fixture := newConvergenceFixture(active, target, steps, 1, ExecutionWaitingCatchUp)
	blocking := &blockingConvergenceCoordinator{
		convergenceCoordinator: fixture.coord,
		entered:                make(chan struct{}, 1),
		sawErr:                 make(chan error, 1),
	}
	engine := NewConvergenceEngine(ConvergenceConfig{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: fixture.store, Coordinator: blocking, Groups: fixture.groups,
		OwnsGroup:       func(groupID string) bool { return groupID != MetaGroupID },
		ProposalTimeout: 50 * time.Millisecond,
	})
	fixture.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, []uint64{2}, 1)

	done := make(chan struct{})
	go func() {
		engine.Tick(context.Background())
		close(done)
	}()

	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("tick never reached the transition proposal")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tick is wedged on a lost proposal — per-proposal deadline missing")
	}
	select {
	case err := <-blocking.sawErr:
		require.ErrorIs(t, err, context.DeadlineExceeded, "the waiter must be freed by the proposal deadline, not cancellation")
	case <-time.After(time.Second):
		t.Fatal("blocked proposal never observed a context error")
	}

	// The engine must remain serviceable: a second tick acquires tickMu and
	// reaches the coordinator again (fresh proposal, fresh deadline).
	done2 := make(chan struct{})
	go func() {
		engine.Tick(context.Background())
		close(done2)
	}()
	select {
	case <-done2:
	case <-time.After(5 * time.Second):
		t.Fatal("second tick blocked — tickMu was not released after the lost proposal")
	}
}
