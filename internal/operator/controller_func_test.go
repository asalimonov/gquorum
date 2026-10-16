//go:build functional

package operator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

type controllerStore struct {
	mu             sync.Mutex
	latest         Document
	target         Document
	active         Document
	activationErr  error
	activationRuns int
}

func (s *controllerStore) Current(context.Context) (Document, error) {
	return s.Latest(context.Background())
}
func (s *controllerStore) Latest(context.Context) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest.Clone(), nil
}
func (s *controllerStore) Target(context.Context) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.target.Clone(), nil
}
func (s *controllerStore) Active(context.Context) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active.Clone(), nil
}
func (_ *controllerStore) History(context.Context, int, int) ([]Document, error) { return nil, nil }
func (s *controllerStore) Get(_ context.Context, version uint64) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest.Version != version {
		return Document{}, ErrVersionUnknown
	}
	return s.latest.Clone(), nil
}
func (_ *controllerStore) Propose(context.Context, Document, uint64) (Document, error) {
	return Document{}, ErrNotImplemented
}
func (_ *controllerStore) Watch(context.Context) (<-chan Document, error) {
	ch := make(chan Document)
	close(ch)
	return ch, nil
}
func (s *controllerStore) WatchActive(ctx context.Context) (<-chan Document, error) {
	return s.Watch(ctx)
}
func (_ *controllerStore) Close() error { return nil }

func (s *controllerStore) activate() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activationRuns++
	if s.activationErr != nil {
		return s.activationErr
	}
	s.active = s.target.Clone()
	return nil
}

type controllerCoordinator struct {
	mu            sync.Mutex
	leader        bool
	store         *controllerStore
	operator      Record
	hasOperator   bool
	transitionErr error
	transitions   []Transition
	grants        map[AuthorizedStep]topology.AuthorityGrant
	receipts      map[topology.AuthorityKey][]topology.EffectReceipt
	reconcileErr  error
	receiptErr    error
	beginErr      error
}

func (c *controllerCoordinator) IsMetaLeader() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}
func (c *controllerCoordinator) BeginOperator(_ context.Context, version uint64, plan Plan) (ID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hasOperator && c.operator.Version == version && !c.operator.State.Terminal() {
		return c.operator.ID, nil
	}
	attempt := uint32(1)
	if c.hasOperator && c.operator.Version == version {
		attempt = c.operator.Attempt + 1
	}
	c.operator = Record{
		ID: IDFor(version, attempt), Version: version, Attempt: attempt,
		State: OperatorPending, Steps: append([]Step(nil), plan.Steps...),
	}
	c.hasOperator = true
	c.store.mu.Lock()
	c.store.target = c.store.latest.Clone()
	c.store.mu.Unlock()
	return c.operator.ID, nil
}
func (_ *controllerCoordinator) CommitRepairOperator(context.Context, Document, uint64, Plan) (Document, ID, error) {
	return Document{}, "", ErrNotImplemented
}
func (c *controllerCoordinator) Operator(_ context.Context, id ID) (Status, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hasOperator || c.operator.ID != id {
		return Status{}, false
	}
	return operatorStatusFromRecord(c.operator), true
}
func (c *controllerCoordinator) CurrentOperator(context.Context) (Record, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneOperator(c.operator), c.hasOperator
}
func (c *controllerCoordinator) RecordOperatorTransition(_ context.Context, transition Transition) error {
	c.mu.Lock()
	if c.transitionErr != nil {
		err := c.transitionErr
		c.mu.Unlock()
		return err
	}
	if !c.hasOperator || transition.OperatorID != c.operator.ID || transition.Version != c.operator.Version ||
		transition.Attempt != c.operator.Attempt {
		c.mu.Unlock()
		return ErrOperationAuthorization
	}
	c.operator.State = transition.State
	c.operator.CurrentStep = transition.StepSeq
	c.operator.GroupID = transition.GroupID
	c.operator.NodeName = transition.NodeName
	c.operator.WaitingReason = transition.WaitingReason
	c.operator.Error = transition.Error
	c.transitions = append(c.transitions, transition)
	c.mu.Unlock()
	if transition.State == OperatorSucceeded {
		return c.store.activate()
	}
	return nil
}
func (*controllerCoordinator) MetaConfState() (raftpb.ConfState, bool) {
	return raftpb.ConfState{}, false
}
func (*controllerCoordinator) MetaLeader() uint64                               { return 1 }
func (*controllerCoordinator) TransferMetaLeader(context.Context, uint64) error { return nil }
func (*controllerCoordinator) RemoveMetaVoter(context.Context, uint64) error    { return nil }

func cloneOperator(in Record) Record {
	in.Steps = append([]Step(nil), in.Steps...)
	return in
}

func operatorStatusFromRecord(in Record) Status {
	return Status{
		ID: in.ID, Version: in.Version, Attempt: in.Attempt, State: in.State,
		CurrentStep: in.CurrentStep, TotalSteps: len(in.Steps),
		GroupID: in.GroupID, NodeName: in.NodeName, WaitingReason: in.WaitingReason, Error: in.Error,
	}
}

type controllerGroups struct {
	mu       sync.Mutex
	store    *controllerStore
	statuses map[string]GroupStatus
	errs     map[string]error
	calls    []StepKind
	before   func(context.Context, AuthorizedStep) error
	after    func(AuthorizedStep)
}

func (g *controllerGroups) Status(_ context.Context, groupID string) (GroupStatus, error) {
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
func (g *controllerGroups) ReadNative(ctx context.Context, groupID string, _ ...storage.Identity) (topology.NativeState, error) {
	status, err := g.Status(ctx, groupID)
	return topology.NativeState{GroupID: groupID, ReplicaID: 1, Applied: 1, ConfState: status.ConfState}, err
}
func (g *controllerGroups) CreateGroup(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	cmd := *grant.Action
	if err := g.beforeCall(ctx, cmd); err != nil {
		return nil, err
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
	return controllerReceipts(grant), nil
}
func (g *controllerGroups) PrepareReplica(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	cmd := *grant.Action
	if err := g.beforeCall(ctx, cmd); err != nil {
		return nil, err
	}
	g.record(cmd.Kind)
	g.afterCall(cmd)
	return controllerReceipts(grant), nil
}
func (g *controllerGroups) ApplyMembership(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	cmd := *grant.Action
	if err := g.beforeCall(ctx, cmd); err != nil {
		return nil, err
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
	return controllerReceipts(grant), nil
}
func (g *controllerGroups) TransferLeader(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	cmd := *grant.Action
	if err := g.beforeCall(ctx, cmd); err != nil {
		return nil, err
	}
	g.record(cmd.Kind)
	g.mu.Lock()
	status := g.statuses[cmd.GroupID]
	status.LeaderID = cmd.ReplicaID
	g.statuses[cmd.GroupID] = status
	g.mu.Unlock()
	g.afterCall(cmd)
	return controllerReceipts(grant), nil
}
func (g *controllerGroups) StopReplica(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	cmd := *grant.Action
	if err := g.beforeCall(ctx, cmd); err != nil {
		return nil, err
	}
	g.record(cmd.Kind)
	g.afterCall(cmd)
	return controllerReceipts(grant), nil
}
func (g *controllerGroups) CleanupReplicaState(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	cmd := *grant.Action
	if err := g.beforeCall(ctx, cmd); err != nil {
		return nil, err
	}
	g.record(cmd.Kind)
	g.afterCall(cmd)
	return controllerReceipts(grant), nil
}
func (g *controllerGroups) DeleteGroupState(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	cmd := *grant.Action
	if err := g.beforeCall(ctx, cmd); err != nil {
		return nil, err
	}
	g.record(cmd.Kind)
	g.setStatus(cmd.GroupID, GroupStatus{GroupID: cmd.GroupID})
	g.afterCall(cmd)
	return controllerReceipts(grant), nil
}
func (g *controllerGroups) beforeCall(ctx context.Context, cmd AuthorizedStep) error {
	g.mu.Lock()
	hook := g.before
	g.mu.Unlock()
	if hook != nil {
		return hook(ctx, cmd)
	}
	return nil
}
func (g *controllerGroups) afterCall(cmd AuthorizedStep) {
	g.mu.Lock()
	hook := g.after
	g.mu.Unlock()
	if hook != nil {
		hook(cmd)
	}
}
func (g *controllerGroups) record(kind StepKind) {
	g.mu.Lock()
	g.calls = append(g.calls, kind)
	g.mu.Unlock()
}
func (g *controllerGroups) setStatus(id string, status GroupStatus) {
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

type controllerFixture struct {
	store  *controllerStore
	coord  *controllerCoordinator
	groups *controllerGroups
	engine *Controller
}

type fakeControllerClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeControllerTimer
}

func newFakeControllerClock() *fakeControllerClock {
	return &fakeControllerClock{now: time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeControllerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeControllerClock) NewTimer(delay time.Duration) controllerTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeControllerTimer{clock: c, ch: make(chan time.Time, 1), resets: []time.Duration{delay}}
	c.timers = append(c.timers, timer)
	return timer
}

func (c *fakeControllerClock) timerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

func (c *fakeControllerClock) timer(index int) *fakeControllerTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timers[index]
}

type fakeControllerTimer struct {
	mu      sync.Mutex
	clock   *fakeControllerClock
	ch      chan time.Time
	resets  []time.Duration
	stopped bool
}

func (t *fakeControllerTimer) C() <-chan time.Time { return t.ch }

func (t *fakeControllerTimer) Reset(delay time.Duration) {
	t.mu.Lock()
	t.resets = append(t.resets, delay)
	t.mu.Unlock()
}

func (t *fakeControllerTimer) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
}

func (t *fakeControllerTimer) fire() {
	t.clock.mu.Lock()
	t.clock.now = t.clock.now.Add(time.Second)
	now := t.clock.now
	t.clock.mu.Unlock()
	t.ch <- now
}

func (t *fakeControllerTimer) snapshot() ([]time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]time.Duration(nil), t.resets...), t.stopped
}

func newControllerFixture(active, target Document, steps []Step, current int, state State) *controllerFixture {
	store := &controllerStore{latest: target.Clone(), target: target.Clone(), active: active.Clone()}
	coord := &controllerCoordinator{leader: true, store: store, hasOperator: true}
	coord.operator = Record{
		ID: IDFor(target.Version, 1), Version: target.Version, Attempt: 1,
		State: state, Steps: append([]Step(nil), steps...), CurrentStep: current,
	}
	groups := &controllerGroups{store: store, statuses: map[string]GroupStatus{}, errs: map[string]error{}}
	engine := mustController(Config{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: store, LegacyCoordinator: coord, Groups: groups,
		OwnsGroup: func(groupID string) bool { return groupID != MetaGroupID },
	})
	return &controllerFixture{store: store, coord: coord, groups: groups, engine: engine}
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

func TestController_LearnerFirstSteps(t *testing.T) {
	t.Parallel()
	active, target, replica := growDocuments()
	tests := []struct {
		name    string
		steps   []Step
		current int
		state   State
		status  GroupStatus
		want    StepKind
		wantSeq int
		final   bool
	}{
		{"prepare", []Step{{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}, {Seq: 2, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}}, 0, OperatorPending, statusFor("shard0", []uint64{1}, nil, 1), StepPrepareReplica, 2, false},
		{"add learner", []Step{{Seq: 1, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}, {Seq: 2, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}}, 0, OperatorPending, statusFor("shard0", []uint64{1}, nil, 1), StepAddLearner, 2, false},
		{"wait caught up", []Step{{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}, {Seq: 2, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}}, 0, OperatorPending, statusFor("shard0", []uint64{1}, []uint64{2}, 1), "", 2, false},
		{"promote", []Step{{Seq: 1, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}}, 0, OperatorPending, statusFor("shard0", []uint64{1}, []uint64{2}, 1), StepPromoteVoter, 1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newControllerFixture(active, target, tc.steps, tc.current, tc.state)
			f.groups.statuses["shard0"] = tc.status
			f.engine.Tick(context.Background())
			ex, _ := f.coord.CurrentOperator(context.Background())
			if tc.final {
				assert.Equal(t, OperatorSucceeded, ex.State)
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

func TestController_FinalValidatorFailureLeavesActiveUnchanged(t *testing.T) {
	t.Parallel()
	active, target, replica := growDocuments()
	step := Step{Seq: 1, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newControllerFixture(active, target, []Step{step}, 0, OperatorPending)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, []uint64{2}, 1)
	f.engine.SetFinalValidator(func(context.Context, Document, Document) error {
		return errors.New("meta health changed")
	})
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentOperator(context.Background())
	if ex.State != OperatorFailed {
		t.Fatalf("execution state = %q, want failed", ex.State)
	}
	got, _ := f.store.Active(context.Background())
	if got.Version != active.Version {
		t.Fatalf("Active advanced to %d after final safety failure", got.Version)
	}
}

func TestController_ForcedRetirementRequiresHostReceipts(t *testing.T) {
	t.Parallel()
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
	f := newControllerFixture(active, target, steps, 2, OperatorRunning)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1, 2}, nil, 1)
	attempts := 0
	f.groups.before = func(context.Context, AuthorizedStep) error { attempts++; return fault.ErrUnavailable }
	f.engine.Tick(context.Background())
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentOperator(context.Background())
	if ex.State.Terminal() || ex.CurrentStep != 2 {
		t.Fatalf("unreachable host advanced without receipts: %+v", ex)
	}
	if attempts != 2 || len(f.groups.calls) != 0 {
		t.Fatalf("host retries = %d, completed effects = %v", attempts, f.groups.calls)
	}

}

func TestController_DestructiveAndLifecycleSteps(t *testing.T) {
	t.Parallel()
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
	target := active.Clone()
	target.Version = 2
	target.Group("shard0").Replicas = target.Group("shard0").Replicas[:1]
	removed := active.Group("shard0").Replicas[1]
	remove := Step{Seq: 1, Kind: StepRemoveReplica, GroupID: "shard0", NodeName: removed.NodeName, ReplicaID: removed.ReplicaID}
	stop := Step{Seq: 2, Kind: StepStopReplica, GroupID: "shard0", NodeName: removed.NodeName, ReplicaID: removed.ReplicaID}
	cleanup := Step{Seq: 3, Kind: StepCleanupReplicaState, GroupID: "shard0", NodeName: removed.NodeName, ReplicaID: removed.ReplicaID}

	f := newControllerFixture(active, target, []Step{remove, stop, cleanup}, 0, OperatorPending)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1, 2}, nil, 1)
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentOperator(context.Background())
	require.Equal(t, 2, ex.CurrentStep)
	f.engine.Tick(context.Background())
	ex, _ = f.coord.CurrentOperator(context.Background())
	require.Equal(t, 3, ex.CurrentStep)
	f.engine.Tick(context.Background())
	ex, _ = f.coord.CurrentOperator(context.Background())
	assert.Equal(t, OperatorSucceeded, ex.State)
	assert.Equal(t, []StepKind{StepRemoveReplica, StepStopReplica, StepCleanupReplicaState}, f.groups.calls)
}

func TestController_DeleteShardStopsEveryHostBeforeSharedDelete(t *testing.T) {
	t.Parallel()
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard9"})
	target := active.Clone()
	target.Version = 2
	target.Groups = target.Groups[:2]
	steps := []Step{
		{Seq: 1, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n2", ReplicaID: 2},
		{Seq: 3, Kind: StepDeleteGroupState, GroupID: "shard9"},
	}
	f := newControllerFixture(active, target, steps, 0, OperatorPending)
	f.groups.statuses["shard9"] = statusFor("shard9", []uint64{1, 2}, nil, 1)
	f.groups.after = func(cmd AuthorizedStep) {
		if cmd.Kind == StepStopReplica && cmd.NodeName == "n2" {
			f.groups.setStatus("shard9", GroupStatus{GroupID: "shard9"})
		}
	}
	for range 4 {
		f.engine.Tick(context.Background())
	}
	ex, _ := f.coord.CurrentOperator(context.Background())
	if ex.State != OperatorSucceeded {
		t.Fatalf("delete execution = %+v", ex)
	}
	assert.Equal(t, []StepKind{StepStopReplica, StepStopReplica, StepDeleteGroupState}, f.groups.calls)
}

func TestController_DeleteShardWaitsForUnreachableHost(t *testing.T) {
	t.Parallel()
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard9"})
	target := active.Clone()
	target.Version = 2
	target.Groups = target.Groups[:2]
	steps := []Step{
		{Seq: 1, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n2", ReplicaID: 2},
		{Seq: 3, Kind: StepDeleteGroupState, GroupID: "shard9"},
	}
	f := newControllerFixture(active, target, steps, 2, OperatorRunning)
	f.groups.errs["shard9"] = fmtError(ErrGroupUnavailable, "n2 unreachable")
	f.groups.before = func(_ context.Context, cmd AuthorizedStep) error {
		if cmd.Kind == StepStopReplica && cmd.NodeName == "n2" {
			return fmtError(ErrGroupUnavailable, "n2 unreachable")
		}
		return nil
	}
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentOperator(context.Background())
	if ex.CurrentStep != 2 || ex.State.Terminal() {
		t.Fatalf("unreachable stop advanced execution: %+v", ex)
	}
	assert.NotContains(t, f.groups.calls, StepDeleteGroupState)
}

func TestController_DeleteShardCompletesAfterGroupLosesLeader(t *testing.T) {
	t.Parallel()
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard9"})
	target := active.Clone()
	target.Version = 2
	target.Groups = target.Groups[:2]
	steps := []Step{
		{Seq: 1, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n2", ReplicaID: 2},
		{Seq: 3, Kind: StepDeleteGroupState, GroupID: "shard9"},
	}
	f := newControllerFixture(active, target, steps, 2, OperatorRunning)
	f.groups.errs["shard9"] = fmtError(ErrGroupUnavailable, "no leader")
	for range 3 {
		f.engine.Tick(context.Background())
	}
	ex, _ := f.coord.CurrentOperator(context.Background())
	assert.Equal(t, OperatorSucceeded, ex.State)
	assert.Equal(t, []StepKind{StepStopReplica, StepDeleteGroupState}, f.groups.calls)
}

func TestController_DeleteShardContinuesAfterGroupBecomesAbsent(t *testing.T) {
	t.Parallel()
	active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard9"})
	target := active.Clone()
	target.Version = 2
	target.Groups = target.Groups[:2]
	steps := []Step{
		{Seq: 1, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: StepStopReplica, GroupID: "shard9", NodeName: "n2", ReplicaID: 2},
		{Seq: 3, Kind: StepDeleteGroupState, GroupID: "shard9"},
	}
	f := newControllerFixture(active, target, steps, 2, OperatorRunning)
	f.groups.statuses["shard9"] = GroupStatus{GroupID: "shard9"}
	for range 3 {
		f.engine.Tick(context.Background())
	}
	ex, _ := f.coord.CurrentOperator(context.Background())
	assert.Equal(t, OperatorSucceeded, ex.State)
	assert.Equal(t, []StepKind{StepStopReplica, StepDeleteGroupState}, f.groups.calls)
}

func TestController_FoundingDemotionTransferAndDelete(t *testing.T) {
	t.Parallel()
	base := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
	t.Run("founding", func(t *testing.T) {
		target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard1"})
		target.Version = 2
		step := Step{Seq: 1, Kind: StepCreateFoundingGroup, GroupID: "shard1"}
		f := newControllerFixture(base, target, []Step{step}, 0, OperatorPending)
		f.groups.statuses["shard1"] = GroupStatus{GroupID: "shard1"}
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorSucceeded, ex.State)
	})
	t.Run("founding resumes when partial group has no leader", func(t *testing.T) {
		target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard1"})
		target.Version = 2
		step := Step{Seq: 1, Kind: StepCreateFoundingGroup, GroupID: "shard1"}
		f := newControllerFixture(base, target, []Step{step}, 1, OperatorWaitingLeader)
		f.groups.errs["shard1"] = fmtError(ErrGroupUnavailable, "partial founding has no leader")
		f.groups.before = func(_ context.Context, cmd AuthorizedStep) error {
			f.groups.mu.Lock()
			delete(f.groups.errs, cmd.GroupID)
			f.groups.mu.Unlock()
			return nil
		}

		f.engine.Tick(context.Background())

		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorSucceeded, ex.State)
		assert.Equal(t, []StepKind{StepCreateFoundingGroup}, f.groups.calls)
	})
	t.Run("demotion", func(t *testing.T) {
		active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
		target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
		target.Version = 2
		replica := &target.Group("shard0").Replicas[1]
		replica.Role = ReplicaLearner
		step := Step{Seq: 1, Kind: StepDemoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		f := newControllerFixture(active, target, []Step{step}, 0, OperatorPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1, 2}, nil, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorSucceeded, ex.State)
	})
	t.Run("transfer", func(t *testing.T) {
		target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
		target.Version = 2
		active := target.Clone()
		active.Version = 1
		replica := target.Group("shard0").Replicas[1]
		step := Step{Seq: 1, Kind: StepTransferLeader, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		f := newControllerFixture(active, target, []Step{step}, 0, OperatorPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1, 2}, nil, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorSucceeded, ex.State)
	})
	t.Run("delete", func(t *testing.T) {
		active := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0", "shard1"})
		target := clusterDoc(1, 2, []string{"n1", "n2"}, []string{"shard0"})
		target.Version = 2
		step := Step{Seq: 1, Kind: StepDeleteGroupState, GroupID: "shard1"}
		f := newControllerFixture(active, target, []Step{step}, 0, OperatorPending)
		f.groups.statuses["shard1"] = statusFor("shard1", []uint64{1}, nil, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorSucceeded, ex.State)
	})
}

func TestController_WaitFailureOwnershipAndCancellation(t *testing.T) {
	t.Parallel()
	active, target, replica := growDocuments()
	prepare := Step{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	t.Run("unreachable prepare waits", func(t *testing.T) {
		f := newControllerFixture(active, target, []Step{prepare}, 0, OperatorPending)
		f.groups.errs["shard0"] = fmtError(ErrGroupUnavailable, "target unreachable")
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorWaitingReplica, ex.State)
	})
	t.Run("catch up waits", func(t *testing.T) {
		wait := Step{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		promote := Step{Seq: 2, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		f := newControllerFixture(active, target, []Step{wait, promote}, 0, OperatorPending)
		status := statusFor("shard0", []uint64{1}, []uint64{2}, 1)
		status.Progress[2] = ReplicaProgress{Match: 50, RecentActive: false}
		f.groups.statuses["shard0"] = status
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorWaitingCatchUp, ex.State)
	})
	t.Run("identity mismatch fails", func(t *testing.T) {
		bad := prepare
		bad.ReplicaID = 99
		f := newControllerFixture(active, target, []Step{bad}, 0, OperatorPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorFailed, ex.State)
	})
	t.Run("other plane does not transition", func(t *testing.T) {
		meta := prepare
		meta.GroupID = MetaGroupID
		f := newControllerFixture(active, target, []Step{meta}, 0, OperatorPending)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorPending, ex.State)
		assert.Zero(t, ex.CurrentStep)
	})
	t.Run("meta leadership loss does not transition", func(t *testing.T) {
		f := newControllerFixture(active, target, []Step{prepare}, 0, OperatorPending)
		f.coord.leader = false
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorPending, ex.State)
		assert.Zero(t, ex.CurrentStep)
	})
	t.Run("already satisfied membership still obtains receipt", func(t *testing.T) {
		add := Step{Seq: 1, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		wait := Step{Seq: 2, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
		f := newControllerFixture(active, target, []Step{add, wait}, 0, OperatorPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, []uint64{2}, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, 2, ex.CurrentStep)
		assert.Equal(t, []StepKind{StepAddLearner}, f.groups.calls)
	})
	t.Run("cleanup without exact remove and stop fails", func(t *testing.T) {
		cleanup := Step{Seq: 1, Kind: StepCleanupReplicaState, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}
		f := newControllerFixture(active, target, []Step{cleanup}, 0, OperatorPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorFailed, ex.State)
		assert.Empty(t, f.groups.calls)
	})
	t.Run("cancelled tick does nothing", func(t *testing.T) {
		f := newControllerFixture(active, target, []Step{prepare}, 0, OperatorPending)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		f.engine.Tick(ctx)
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorPending, ex.State)
	})
}

func TestController_EmptyPlanAndActivationFailure(t *testing.T) {
	t.Parallel()
	target := clusterDoc(1, 1, []string{"n1"}, []string{"shard0"})
	target.Version = 2
	active := target.Clone()
	active.Version = 1
	f := newControllerFixture(active, target, nil, 0, OperatorPending)
	f.store.activationErr = errors.New("activation failed")
	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentOperator(context.Background())
	assert.Equal(t, OperatorSucceeded, ex.State, "success transition committed before activation proposal failed")
	got, _ := f.store.Active(context.Background())
	assert.Equal(t, uint64(1), got.Version, "Active must not advance when activation fails")
}

func TestController_FailoverResumesExactStep(t *testing.T) {
	t.Parallel()
	active, target, replica := growDocuments()
	tests := []struct {
		name      string
		steps     []Step
		wantCalls int
		wantState State
		wantStep  int
	}{
		{
			name: "prepare receipt survives failover",
			steps: []Step{
				{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
				{Seq: 2, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
			},
			wantCalls: 1, wantState: OperatorRunning, wantStep: 2,
		},
		{
			name: "learner addition is observed live",
			steps: []Step{
				{Seq: 1, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
				{Seq: 2, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
			},
			wantCalls: 1, wantState: OperatorRunning, wantStep: 2,
		},
		{
			name: "promotion is observed live",
			steps: []Step{
				{Seq: 1, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
			},
			wantCalls: 1, wantState: OperatorSucceeded, wantStep: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newControllerFixture(active, target, tc.steps, 0, OperatorPending)
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
			ex, _ := f.coord.CurrentOperator(context.Background())
			assert.Equal(t, OperatorRunning, ex.State)
			assert.Equal(t, 1, ex.CurrentStep)

			f.coord.mu.Lock()
			f.coord.leader = true
			f.coord.mu.Unlock()
			f.engine.Tick(context.Background())
			ex, _ = f.coord.CurrentOperator(context.Background())
			assert.Equal(t, tc.wantState, ex.State)
			assert.Equal(t, tc.wantStep, ex.CurrentStep)
			assert.Len(t, f.groups.calls, tc.wantCalls)
		})
	}
}

func TestController_SupersedesOnlyAtStepBoundary(t *testing.T) {
	t.Parallel()
	active, target, replica := growDocuments()
	add := Step{Seq: 1, Kind: StepAddLearner, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}

	t.Run("newer target supersedes before work", func(t *testing.T) {
		f := newControllerFixture(active, target, []Step{add}, 0, OperatorPending)
		f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
		f.store.mu.Lock()
		f.store.target.Version++
		f.store.mu.Unlock()

		f.engine.Tick(context.Background())
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, OperatorSuperseded, ex.State)
		assert.Empty(t, f.groups.calls)
	})

	t.Run("in flight membership change finishes without advancing old execution", func(t *testing.T) {
		f := newControllerFixture(active, target, []Step{add}, 0, OperatorPending)
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
		f.coord.operator = Record{
			ID: IDFor(3, 1), Version: 3, Attempt: 1, State: OperatorPending,
			Steps: []Step{{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}},
		}
		f.coord.mu.Unlock()
		close(release)
		<-done

		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, IDFor(3, 1), ex.ID)
		assert.Equal(t, OperatorPending, ex.State)
		assert.Zero(t, ex.CurrentStep)
		assert.Equal(t, []StepKind{StepAddLearner}, f.groups.calls)
	})
}

func TestController_ResumePreservesDurableWaiting(t *testing.T) {
	t.Parallel()
	active, target, replica := growDocuments()
	wait := Step{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	promote := Step{Seq: 2, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newControllerFixture(active, target, []Step{wait, promote}, 1, OperatorWaitingCatchUp)
	status := statusFor("shard0", []uint64{1}, []uint64{2}, 1)
	status.Progress[2] = ReplicaProgress{Match: 1, RecentActive: false}
	f.groups.statuses["shard0"] = status

	f.engine.Tick(context.Background())
	ex, _ := f.coord.CurrentOperator(context.Background())
	require.Equal(t, OperatorWaitingCatchUp, ex.State)
	require.Equal(t, 1, ex.CurrentStep)

	// A replacement process reconstructs only the engine. Replicated waiting
	// state and the exact cursor remain authoritative; no local timer state is
	// needed to resume live observation.
	f.engine = mustController(Config{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: f.store, LegacyCoordinator: f.coord, Groups: f.groups,
		OwnsGroup: func(groupID string) bool { return groupID != MetaGroupID },
	})
	status.Progress[2] = ReplicaProgress{Match: 100, RecentActive: true}
	f.groups.setStatus("shard0", status)
	f.engine.Tick(context.Background())
	ex, _ = f.coord.CurrentOperator(context.Background())
	assert.Equal(t, OperatorRunning, ex.State)
	assert.Equal(t, 2, ex.CurrentStep)
}

func TestControllerRechecksPreparatoryLearnerBeforeDestruction(t *testing.T) {
	t.Parallel()
	active := clusterDoc(1, 3, []string{"n1", "n2", "n3"}, []string{"shard0"})
	active.Group("shard0").Replicas[1].Role = ReplicaLearner
	target := active.Clone()
	target.Version = 2
	target.Group("shard0").Replicas[1].Role = ReplicaVoter
	target.Group("shard0").Replicas = target.Group("shard0").Replicas[:2]
	steps := []Step{
		{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: "n2", ReplicaID: 2},
		{Seq: 2, Kind: StepRemoveReplica, GroupID: "shard0", NodeName: "n3", ReplicaID: 3},
		{Seq: 3, Kind: StepStopReplica, GroupID: "shard0", NodeName: "n3", ReplicaID: 3},
		{Seq: 4, Kind: StepCleanupReplicaState, GroupID: "shard0", NodeName: "n3", ReplicaID: 3},
		{Seq: 5, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: "n2", ReplicaID: 2},
		{Seq: 6, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: "n2", ReplicaID: 2},
	}
	f := newControllerFixture(active, target, steps, 2, OperatorRunning)
	status := statusFor("shard0", []uint64{1, 3}, []uint64{2}, 1)
	status.Progress[2] = ReplicaProgress{Match: 1, RecentActive: false}
	if err := f.engine.validateStep(t.Context(), f.coord.operator, stepValidationInput{active, target, steps[1], status}); !errors.Is(err, ErrReplicaNotReady) {
		t.Fatalf("stale preparatory wait authorized destruction: %v", err)
	}
	status.Progress[2] = ReplicaProgress{Match: status.Commit, RecentActive: true}
	if err := f.engine.validateStep(t.Context(), f.coord.operator, stepValidationInput{active, target, steps[1], status}); err != nil {
		t.Fatalf("freshly caught-up learner blocked destruction: %v", err)
	}
	status.ConfState.Voters = []uint64{1} // exact already-applied removal does not need an old prerequisite
	status.Progress[2] = ReplicaProgress{Match: 1, RecentActive: false}
	if err := f.engine.validateStep(t.Context(), f.coord.operator, stepValidationInput{active, target, steps[1], status}); err != nil {
		t.Fatalf("completed removal demanded obsolete readiness: %v", err)
	}
}

func TestController_ResumeMixedPlaneOwnership(t *testing.T) {
	t.Parallel()
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
	f := newControllerFixture(active, target, steps, 0, OperatorPending)
	f.groups.statuses[MetaGroupID] = statusFor(MetaGroupID, []uint64{1}, nil, 1)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
	metaEngine := mustController(Config{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: f.store, LegacyCoordinator: f.coord, Groups: f.groups,
		OwnsGroup: func(groupID string) bool { return groupID == MetaGroupID },
	})
	dataEngine := f.engine

	for wantStep := 2; wantStep <= len(steps); wantStep++ {
		before, _ := f.coord.CurrentOperator(context.Background())
		stepIndex := before.CurrentStep - 1
		if stepIndex < 0 {
			stepIndex = 0
		}
		if steps[stepIndex].GroupID == MetaGroupID {
			dataEngine.Tick(context.Background())
			unchanged, _ := f.coord.CurrentOperator(context.Background())
			assert.Equal(t, before.CurrentStep, unchanged.CurrentStep)
			metaEngine.Tick(context.Background())
		} else {
			metaEngine.Tick(context.Background())
			unchanged, _ := f.coord.CurrentOperator(context.Background())
			assert.Equal(t, before.CurrentStep, unchanged.CurrentStep)
			dataEngine.Tick(context.Background())
		}
		ex, _ := f.coord.CurrentOperator(context.Background())
		assert.Equal(t, wantStep, ex.CurrentStep)
	}
	dataEngine.Tick(context.Background())
	ex, _ := f.coord.CurrentOperator(context.Background())
	assert.Equal(t, OperatorSucceeded, ex.State)
}

func TestController_ShutdownCancelsBlockedWorkAndIsIdempotent(t *testing.T) {
	t.Parallel()
	active, target, replica := growDocuments()
	prepare := Step{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newControllerFixture(active, target, []Step{prepare}, 0, OperatorPending)
	f.groups.statuses["shard0"] = statusFor("shard0", []uint64{1}, nil, 1)
	clock := newFakeControllerClock()
	f.engine = mustController(Config{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: f.store, LegacyCoordinator: f.coord, Groups: f.groups, Interval: time.Second,
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

func TestController_ShutdownReusesBoundedRetryTimer(t *testing.T) {
	t.Parallel()
	active, target, replica := growDocuments()
	prepare := Step{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newControllerFixture(active, target, []Step{prepare}, 0, OperatorPending)
	f.groups.errs["shard0"] = ErrGroupUnavailable
	clock := newFakeControllerClock()
	f.engine = mustController(Config{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: f.store, LegacyCoordinator: f.coord, Groups: f.groups,
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

func TestController_ResumeSerializesConcurrentTicks(t *testing.T) {
	t.Parallel()
	active, target, replica := growDocuments()
	prepare := Step{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
	f := newControllerFixture(active, target, []Step{prepare}, 0, OperatorPending)
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

// blockingControllerCoordinator simulates a SILENTLY-lost raft proposal: the
// entry was appended but truncated by a leadership change, so it never applies
// and the store-side waiter resolves only when the caller's context does.
type blockingControllerCoordinator struct {
	*controllerCoordinator
	entered chan struct{}
	sawErr  chan error
}

func (c *blockingControllerCoordinator) RecordOperatorTransition(ctx context.Context, _ Transition) error {
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

// TestController_TickSurvivesLostProposal pins the meta-leader-change freeze:
// a transition proposal lost across a term boundary must cost the engine one
// bounded wait, not park the tick goroutine forever (tickMu held, no further
// ticks — the exact stall TestClusterAPI_ExecutionResumesAfterMetaLeaderChange
// reproduces). The engine's loop context is cancel-only, so without the
// per-proposal deadline this test hangs.
func TestController_TickSurvivesLostProposal(t *testing.T) {
	t.Parallel()
	active, target, replica := growDocuments()
	steps := []Step{
		{Seq: 1, Kind: StepWaitCaughtUp, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
		{Seq: 2, Kind: StepPromoteVoter, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID},
	}
	// waiting_for_catch_up at step 1: the first write any tick makes is
	// ensureRunning's waiting→running transition — the call that froze.
	fixture := newControllerFixture(active, target, steps, 1, OperatorWaitingCatchUp)
	blocking := &blockingControllerCoordinator{
		controllerCoordinator: fixture.coord,
		entered:               make(chan struct{}, 1),
		sawErr:                make(chan error, 1),
	}
	engine := mustController(Config{Rules: Rules{SchemaVersion: 1, Limits: Limits{MaxNodes: 3, MaxReplicas: 5}},
		Store: fixture.store, LegacyCoordinator: blocking, Groups: fixture.groups,
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

func mustController(cfg Config) *Controller {
	if cfg.Authority == nil {
		cfg.Authority = cfg.LegacyCoordinator.(AuthorityCoordinator)
	}
	if cfg.PlanValidator == nil {
		// These protocol doubles intentionally exercise individual steps. Full
		// remaining-plan validation has its own tests and service integration.
		cfg.PlanValidator = func(context.Context, Record) error { return nil }
	}
	c, err := New(cfg)
	if err != nil {
		panic(err)
	}
	return c
}

func (*controllerGroups) DispatchEffect(context.Context, topology.AuthorityGrant, topology.AuthorityResource, topology.AuthorityTarget) (topology.EffectReceipt, error) {
	panic("legacy fixture used for scoped dispatch")
}
