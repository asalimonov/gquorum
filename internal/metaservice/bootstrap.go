package metaservice

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

const sessionRenewalDivisor = 3

func (g *Group) run() {
	defer func() {
		g.wg.Done()
		if g.autoStopNeeded() {
			g.requestOwnerStop()
		}
	}()
	ticker := time.NewTicker(g.opts.Timing.TickInterval)
	defer ticker.Stop()
	var state bootstrapTickState
	for {
		select {
		case <-g.workCtx.Done():
			return
		case <-ticker.C:
		}
		if g.bootstrapTick(&state) == bootstrapStop {
			return
		}
	}
}

func (g *Group) bootstrap(ctx context.Context) error {
	machine := g.state()
	identity := g.currentIdentity()
	if machine.Latest().Version == 0 {
		if err := g.seedTopology(ctx, identity); err != nil {
			return err
		}
	}
	latest := machine.Latest()
	if latest.Origin != topology.OriginFounding || latest.Version != 1 {
		return nil
	}
	approved, err := machine.Approval(latest.Version)
	if err != nil {
		return err
	}
	if approved == "" {
		_, err = g.applyOperationLocal(ctx, applyVersionRequest{Kind: "approve", Version: latest.Version})
		if err != nil {
			return err
		}
	}
	return nil
}

// Activate establishes or renews this participant's session under serialized
// session ownership.
func (g *Group) Activate(ctx context.Context) (topology.Session, error) {
	return g.activateSession(ctx, manualSessionActivation)
}

type sessionActivationMode uint8

const (
	manualSessionActivation sessionActivationMode = iota
	automaticSessionActivation
)

func (g *Group) activateSession(ctx context.Context, mode sessionActivationMode) (topology.Session, error) {
	if err := g.ensureRunning(ctx); err != nil {
		return topology.Session{}, err
	}
	if err := g.takeSessionToken(ctx); err != nil {
		return topology.Session{}, err
	}
	defer g.releaseSessionToken()
	g.sessionMu.Lock()
	state := sessionActivation{id: g.ownSession, deadline: g.ownDeadline,
		paused: g.sessionPaused, previous: g.lastDeactivated}
	g.sessionMu.Unlock()
	if mode == automaticSessionActivation && state.paused {
		return topology.Session{}, fault.ErrUnavailable
	}
	state.identity, state.machine = g.currentIdentity(), g.state()
	if err := g.validateSessionTopology(ctx, state); err != nil {
		return topology.Session{}, err
	}
	if err := selectSessionIdentity(&state); err != nil {
		return topology.Session{}, err
	}
	if err := g.rememberOwnSession(ctx, state.id); err != nil {
		return topology.Session{}, err
	}
	ttl, err := g.grantSession(ctx, state)
	if err != nil {
		return topology.Session{}, err
	}
	state.ttl = ttl
	if err := g.refreshSessionDeadline(ctx, state); err != nil {
		return topology.Session{}, err
	}
	return topology.Session{ID: state.id, NodeName: state.identity.NodeName,
		InstanceID: state.identity.InstanceID, TTL: ttl}, nil
}

type bootstrapOutcome uint8

const (
	bootstrapProceed bootstrapOutcome = iota
	bootstrapWait
	bootstrapStop
)

func (g *Group) joinForTick() bootstrapOutcome {
	if !g.opts.Join || g.joinDone {
		return bootstrapProceed
	}
	ctx, cancel := context.WithTimeout(g.workCtx, 5*time.Second)
	err := g.tryJoin(ctx)
	cancel()
	if err != nil {
		if g.workCtx.Err() != nil {
			return bootstrapStop
		}
		var fatal *fatalAdmissionError
		if errors.As(err, &fatal) {
			g.fail(fatal.error)
			return bootstrapStop
		}
		if errors.Is(err, fault.ErrReadmissionRequired) {
			g.mu.Lock()
			g.terminal = err
			g.lifecycle = "blocked"
			g.mu.Unlock()
			g.readyOnce.Do(func() { close(g.ready) })
			return bootstrapStop
		}
		if g.workCtx.Err() != nil {
			return bootstrapStop
		}
		g.mu.Lock()
		g.lifecycle = "parked"
		g.mu.Unlock()
		g.opts.Logger.Debug("meta join awaiting admission", "error", err)
		return bootstrapWait
	}
	return bootstrapProceed
}

func (g *Group) joinReadinessForTick() error {
	if g.opts.Join && !g.joinReady {
		ctx, cancel := context.WithTimeout(g.workCtx, 2*time.Second)
		err := g.acknowledgeJoin(ctx)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Group) runtimeTerminated(engine *raftengine.Group) bool {
	if err := engine.Error(); err != nil {
		if g.workCtx.Err() == nil {
			g.fail(err)
		}
		return true
	}
	if err := g.transport.Error(); err != nil {
		g.fail(err)
		return true
	}
	return false
}

func (g *Group) activateLeaderForTick() {
	ctx, cancel := context.WithTimeout(g.workCtx, 5*time.Second)
	err := g.bootstrap(ctx)
	cancel()
	if err != nil && g.workCtx.Err() == nil {
		g.opts.Logger.Debug("meta bootstrap or activation retry", "error", err)
	}
}

func (g *Group) expireLeasesForTick() time.Time {
	ctx, cancel := context.WithTimeout(g.workCtx, 5*time.Second)
	if err := g.expireSessions(ctx, time.Now()); err != nil && g.workCtx.Err() == nil {
		g.opts.Logger.Debug("meta lease expiry retry", "error", err)
	}
	cancel()
	return time.Now().Add(g.opts.SessionTTL / sessionRenewalDivisor)
}

func (g *Group) activateLocalForTick(engine *raftengine.Group) {
	identity := g.currentIdentity()
	status := engine.Status()
	members := make(map[uint64]bool)
	for _, id := range status.ConfState.Voters {
		members[id] = true
	}
	for _, id := range status.ConfState.Learners {
		members[id] = true
	}
	g.transport.Prune(members)
	if !members[identity.MemberID] {
		return
	}
	ctx, cancel := context.WithTimeout(g.workCtx, 5*time.Second)
	_, err := g.activateSession(ctx, automaticSessionActivation)
	cancel()
	if err != nil {
		return
	}
	g.mu.Lock()
	if !g.quiescing && g.selfRemoval == nil && g.workCtx.Err() == nil {
		g.lifecycle = "ready"
	}
	g.mu.Unlock()
	g.readyOnce.Do(func() { close(g.ready) })
}

func (g *Group) seedTopology(ctx context.Context, identity topology.Identity) error {
	if len(g.opts.Founders) == 0 {
		return fault.ErrUnavailable
	}
	now := time.Now().UTC()
	voterConfig := g.opts.freshVoterConfig()
	doc := topology.Document{
		ClusterID: identity.ClusterID,
		Placement: g.opts.Placement,
		ChangedBy: identity.NodeName,
		Origin:    topology.OriginFounding,
		CreatedAt: now,
		Groups: []topology.Group{
			{
				ID:            topology.MetaGroupID,
				NextReplicaID: 1,
				VoterConfig:   &voterConfig,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
		},
	}
	roles := g.foundingRoles
	if roles == nil {
		var err error
		roles, err = g.opts.selectFoundingRoles()
		if err != nil {
			return err
		}
	}
	for _, founder := range g.opts.Founders {
		node := founder.Node
		node.CreatedAt = now
		node.UpdatedAt = now
		doc.Nodes = append(doc.Nodes, node)
		if err := doc.SetMemberID(node.Name, founder.MemberID, now); err != nil {
			return err
		}
		replica := &doc.MetaGroup().Replicas[len(doc.MetaGroup().Replicas)-1]
		replica.Role, replica.PinnedLearner = roles[node.Name], founder.PinnedLearner
	}
	if _, err := g.propose(ctx, metaengine.ProposeDocument{Doc: doc, ExpectedVersion: 0}); err != nil {
		return err
	}
	return nil
}

type sessionActivation struct {
	id, previous string
	deadline     time.Time
	paused, held bool
	identity     topology.Identity
	machine      *metaengine.Machine
	ttl          time.Duration
}

func (g *Group) validateSessionTopology(ctx context.Context, state sessionActivation) error {
	identity, machine := state.identity, state.machine
	if !containsMember(g.engine.Load().Status().ConfState, identity.MemberID) {
		return fault.ErrUnavailable
	}
	founding, err := sessionAdmissionBasis(machine, identity)
	if err != nil || founding == "" {
		return err
	}
	_, err = g.propose(ctx,
		metaengine.BindAdmissionProposal{Admission: metaengine.AdmissionRecord{NodeName: identity.NodeName,
			InstanceID: identity.InstanceID, MemberID: identity.MemberID,
			OperationID: topology.OperationID(founding)}, Founding: true, At: time.Now().UTC(),
			ProposalID: newProposalID()})
	return err
}

func sessionAdmissionBasis(machine *metaengine.Machine, identity topology.Identity) (topology.OperationID, error) {
	founding, ready := machine.AdmissionBasis(identity)
	if !ready {
		return "", fault.ErrUnavailable
	}
	if machine.HasActivated(identity.NodeName) || machine.HasCompletedAdmission(identity.NodeName, identity.MemberID) {
		return "", nil
	}
	if founding == "" {
		return "", fault.ErrUnavailable
	}
	return founding, nil
}

func selectSessionIdentity(state *sessionActivation) error {
	identity, machine, previous := state.identity, state.machine, state.previous
	holder, held := machine.LockSession("node/" + identity.NodeName)
	if held && (holder.NodeName != identity.NodeName || holder.InstanceID != identity.InstanceID) {
		return fault.New(fault.IdentityMismatch,
			"node name is held by another process instance", nil)
	}
	if held {
		// A follower may still have the just-revoked session locally. Do not
		// regrant its identity while the revocation is catching up.
		if holder.SessionID == previous {
			return fault.ErrUnavailable
		}
		state.id = holder.SessionID
	}
	if state.id == "" {
		// Keep this identity across uncertain grant retries. A later activation
		// after successful deactivation needs a fresh ID to fence delayed
		// revokes.
		var nonce [24]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		state.id = "session-" + hex.EncodeToString(nonce[:])
	}
	state.held = held
	return nil
}

func (g *Group) rememberOwnSession(ctx context.Context, sessionID string) error {
	g.sessionMu.Lock()
	if err := g.ensureRunning(ctx); err != nil {
		g.sessionMu.Unlock()
		return err
	}
	g.ownSession = sessionID
	g.sessionMu.Unlock()
	return nil
}

func (g *Group) grantSession(ctx context.Context, state sessionActivation) (time.Duration, error) {
	identity, machine, sessionID, held := state.identity, state.machine, state.id, state.held
	if _, exists := machine.Session(sessionID); !exists {
		_, sessionErr := g.propose(ctx,
			metaengine.GrantSession{SessionID: sessionID, NodeName: identity.NodeName,
				InstanceID: identity.InstanceID, TTLSeconds: int(g.opts.SessionTTL / time.Second),
				ProposalID: newProposalID()})
		if sessionErr != nil {
			return 0, sessionErr
		}
	}
	if !held {
		_, lockErr := g.propose(ctx, metaengine.AcquireLock{Key: "node/" + identity.NodeName,
			SessionID: sessionID, ProposalID: newProposalID()})
		if lockErr != nil {
			return 0, lockErr
		}
	}
	ttl := g.opts.SessionTTL
	if record, exists := machine.Session(sessionID); exists {
		session, err := sessionFromRecord(record)
		if err != nil {
			return 0, err
		}
		ttl = session.TTL
	}
	return ttl, nil
}

func (g *Group) refreshSessionDeadline(ctx context.Context, state sessionActivation) error {
	identity, sessionID, paused, deadline, ttl := state.identity, state.id, state.paused, state.deadline, state.ttl
	if paused || time.Until(deadline) <= ttl-ttl/sessionRenewalDivisor {
		deadline, err := g.renewSession(ctx, sessionID, identity.InstanceID, ttl)
		if err != nil {
			return err
		}
		g.sessionMu.Lock()
		if err := g.ensureRunning(ctx); err != nil {
			g.sessionMu.Unlock()
			return err
		}
		g.ownDeadline = deadline
		g.sessionPaused = false
		g.sessionMu.Unlock()
	}
	return nil
}

type bootstrapTickState struct {
	nextExpiry time.Time
	wasLeader  bool
}

func (g *Group) bootstrapTick(state *bootstrapTickState) bootstrapOutcome {
	outcome := g.joinForTick()
	if outcome != bootstrapProceed {
		return outcome
	}
	engine := g.engine.Load()
	if engine == nil {
		return bootstrapWait
	}
	if err := g.joinReadinessForTick(); err != nil {
		return bootstrapWait
	}
	if g.runtimeTerminated(engine) {
		return bootstrapStop
	}

	status := engine.Status()
	g.observeRemoval(g.currentIdentity(), g.state().Target(), status.ConfState, status.Applied)
	if g.workCtx.Err() != nil {
		return bootstrapStop
	}
	g.maintainLeaderTick(state)

	g.activateLocalForTick(engine)
	return bootstrapProceed
}

func (g *Group) maintainLeaderTick(state *bootstrapTickState) {
	leader := g.IsLeader()
	if leader && !state.wasLeader {
		g.seedLeases(time.Now(), resetLeases)
	}
	state.wasLeader = leader
	if leader {
		g.activateLeaderForTick()
	}
	if leader && !time.Now().Before(state.nextExpiry) {
		state.nextExpiry = g.expireLeasesForTick()
	}
}
