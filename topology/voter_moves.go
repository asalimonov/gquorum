package topology

import "slices"

// voterPlanState is symbolic: a wait makes a future voter conditionally ready,
// but never changes an observation or proves the member live at execution time.
type voterPlanState struct {
	base              PlanInput
	desired           Document
	groupID           string
	voters, learners  map[uint64]struct{}
	observedLive      map[uint64]bool
	conditionalReady  map[uint64]int
	healthKnown       bool
	leader, preferred uint64
	maximum, voterMax int
	steps             []Step
}

type voterPlanTopology struct {
	base    PlanInput
	desired Document
	groupID string
}

func planVoterMoves(view voterPlanTopology, status GroupStatus, options PlanOptions) ([]Step, error) {
	base, desired, groupID := view.base, view.desired, view.groupID
	rules, preferred := options.Rules, options.PreferredMetaTransfer
	group := desired.Group(groupID)
	if group == nil {
		return nil, invalid("unknown group for voter planning")
	}
	_, voterMax := planGroupBounds(&desired, groupID, rules)
	if p := group.VoterConfig; p != nil && p.TargetVoters+p.MaxSurgeVoters < voterMax {
		voterMax = p.TargetVoters + p.MaxSurgeVoters
	}
	s := voterPlanState{base: base, desired: desired, groupID: groupID,
		voters: idSet(status.ConfState.Voters), learners: idSet(status.ConfState.Learners),
		observedLive: status.LiveVoters, conditionalReady: make(map[uint64]int),
		healthKnown: status.HealthKnown, leader: status.LeaderID, preferred: preferred,
		maximum: rules.Limits.MaxReplicas, voterMax: voterMax}
	if groupID == MetaGroupID {
		s.maximum = rules.Limits.MaxNodes
	}
	if err := s.checkInitial(); err != nil {
		return nil, err
	}
	moves := s.classifyMoves(group)
	s.retireStale(moves, status)
	if err := s.replaceOutgoing(view, group, &moves); err != nil {
		return nil, err
	}
	if err := s.addIncoming(group, moves.incoming); err != nil {
		return nil, err
	}
	if err := s.validateFinal(); err != nil {
		return nil, err
	}
	return s.steps, nil
}

func (s *voterPlanState) checkInitial() error {
	if len(s.voters) == 0 {
		return planUnsafef("group %q has no observed voters", s.groupID)
	}
	if len(s.voters)+len(s.learners) > s.maximum {
		return planUnsafef("group %q exceeds replica_capacity %d", s.groupID, s.maximum)
	}
	if s.healthKnown && s.liveCount(s.voters) < QuorumOf(len(s.voters)) {
		return planUnsafef("group %q has no observed live quorum", s.groupID)
	}
	keys := voterKeys(*s)
	return validateOldDomains(s.base, s.desired, s.groupID, keys)
}

func (s *voterPlanState) emit(kind StepKind, replica Replica) {
	s.steps = append(s.steps, Step{Kind: kind, GroupID: s.groupID, NodeName: replica.NodeName,
		ReplicaID: replica.ReplicaID})
}

func (s *voterPlanState) ensureLearner(replica Replica) error {
	if _, voter := s.voters[replica.ReplicaID]; voter {
		return planUnsafef("group %q voter %d cannot be prepared as a learner", s.groupID, replica.ReplicaID)
	}
	if _, learner := s.learners[replica.ReplicaID]; learner {
		return nil
	}
	if len(s.voters)+len(s.learners) >= s.maximum {
		return planUnsafef("group %q has no replica_capacity for learner %d", s.groupID, replica.ReplicaID)
	}
	s.emit(StepPrepareReplica, replica)
	s.emit(StepAddLearner, replica)
	s.learners[replica.ReplicaID] = struct{}{}
	return nil
}

func (s *voterPlanState) wait(replica Replica) {
	s.emit(StepWaitCaughtUp, replica)
	s.conditionalReady[replica.ReplicaID] = len(s.steps)
}

func (s *voterPlanState) canPromote(replica Replica) bool {
	if len(s.voters)+1 > s.voterMax {
		return false
	}
	after := cloneIDSet(s.voters)
	after[replica.ReplicaID] = struct{}{}
	view := voterPlanTopology{base: s.base, desired: s.desired, groupID: s.groupID}
	if validateVoterTransition(view, voterKeys(*s), voterKeysForSet(*s, after), *s) != nil {
		return false
	}
	live := s.liveCount(after)
	if !s.observedLive[replica.ReplicaID] && s.conditionalReady[replica.ReplicaID] == 0 {
		live++ // the immediately preceding wait is a conditional prerequisite
	}
	if s.healthKnown && live < QuorumOf(len(after)) {
		return false
	}
	return true
}

func (s *voterPlanState) promote(replica Replica) error {
	if !s.canPromote(replica) {
		return planUnsafef("group %q has no safe promotion for voter %d", s.groupID, replica.ReplicaID)
	}
	s.emit(StepPromoteVoter, replica)
	delete(s.learners, replica.ReplicaID)
	s.voters[replica.ReplicaID] = struct{}{}
	return nil
}

func (s *voterPlanState) canDestroyAfterPromotion(departing, arrival Replica) bool {
	clone := *s
	clone.voters = cloneIDSet(s.voters)
	clone.learners = cloneIDSet(s.learners)
	clone.conditionalReady = make(map[uint64]int, len(s.conditionalReady)+1)
	for id, seq := range s.conditionalReady {
		clone.conditionalReady[id] = seq
	}
	clone.conditionalReady[arrival.ReplicaID] = len(s.steps) + 1
	clone.voters[arrival.ReplicaID] = struct{}{}
	delete(clone.learners, arrival.ReplicaID)
	return clone.canDestroy(departing) == nil
}

func (s *voterPlanState) canDestroy(replica Replica) error {
	if !s.healthKnown {
		return planUnsafef("group %q replacement needs live membership observations", s.groupID)
	}
	if s.liveCount(s.voters) < QuorumOf(len(s.voters)) {
		return planUnsafef("group %q has no live quorum before destroying voter %d", s.groupID, replica.ReplicaID)
	}
	if err := validateDestructiveBoundary(s.groupID, len(s.voters)); err != nil {
		return err
	}
	after := cloneIDSet(s.voters)
	delete(after, replica.ReplicaID)
	if s.liveCount(after) < QuorumOf(len(after)) {
		return planUnsafef("group %q loses live quorum after destroying voter %d", s.groupID, replica.ReplicaID)
	}
	view := voterPlanTopology{base: s.base, desired: s.desired, groupID: s.groupID}
	return validateVoterTransition(view, voterKeys(*s), voterKeysForSet(*s, after), *s)
}

func (s *voterPlanState) destroy(replica Replica) error {
	if err := s.canDestroy(replica); err != nil {
		return err
	}
	if s.leader == 0 || s.leader == replica.ReplicaID {
		transfer, ok := s.transferTarget(replica.ReplicaID)
		if !ok {
			return planUnsafef("group %q has no live retained leader transfer target", s.groupID)
		}
		s.emit(StepTransferLeader, transfer)
		s.leader = transfer.ReplicaID
	}
	if wanted := s.desired.Group(s.groupID); targetReplicaByID(wanted, replica.ReplicaID) != nil {
		s.emit(StepDemoteVoter, replica)
		delete(s.voters, replica.ReplicaID)
		s.learners[replica.ReplicaID] = struct{}{}
	} else {
		s.emit(StepRemoveReplica, replica)
		delete(s.voters, replica.ReplicaID)
		s.emit(StepStopReplica, replica)
		if s.groupID != MetaGroupID {
			s.emit(StepCleanupReplicaState, replica)
		}
	}
	return nil
}

func (s *voterPlanState) remove(replica Replica) {
	s.emit(StepRemoveReplica, replica)
	delete(s.learners, replica.ReplicaID)
	s.emit(StepStopReplica, replica)
	if s.groupID != MetaGroupID {
		s.emit(StepCleanupReplicaState, replica)
	}
}

func (s *voterPlanState) transferTarget(exclude uint64) (Replica, bool) {
	group := s.desired.Group(s.groupID)
	live := make(map[uint64]struct{})
	for id := range s.voters {
		if s.observedLive[id] || s.conditionalReady[id] != 0 {
			live[id] = struct{}{}
		}
	}
	return transferTarget(group, live, exclude, s.preferred)
}

func (s *voterPlanState) liveCount(voters map[uint64]struct{}) int {
	count := 0
	for id := range voters {
		if s.observedLive[id] || s.conditionalReady[id] != 0 {
			count++
		}
	}
	return count
}

func compareReplicaIdentity(a, b Replica) int {
	if a.ReplicaID < b.ReplicaID {
		return -1
	}
	if a.ReplicaID > b.ReplicaID {
		return 1
	}
	return compareNames(a.NodeName, b.NodeName)
}

func pairIncomingIndex(view voterPlanTopology, departing Replica, incoming []Replica) int {
	base, desired, groupID := view.base, view.desired, view.groupID
	group := desired.Group(groupID)
	if group.VoterConfig == nil || group.VoterConfig.DistinctVoterLabel == "" {
		return 0
	}
	key := group.VoterConfig.DistinctVoterLabel
	old := desired.Node(departing.NodeName)
	if old == nil {
		old = base.provenNode(departing.NodeName)
	}
	if old != nil {
		for i, arrival := range incoming {
			if node := desired.Node(arrival.NodeName); node != nil && node.Labels[key] == old.Labels[key] {
				return i
			}
		}
	}
	return 0
}

func sameVoterIDs(actual map[uint64]struct{}, wanted []Replica) bool {
	if len(actual) != len(wanted) {
		return false
	}
	for _, replica := range wanted {
		if _, exists := actual[replica.ReplicaID]; !exists {
			return false
		}
	}
	return true
}

type voterMoves struct {
	want                              map[uint64]Replica
	incoming, outgoing, staleLearners []Replica
}

func (s *voterPlanState) classifyMoves(group *Group) voterMoves {
	want := make(map[uint64]Replica, len(group.Replicas))
	var incoming, outgoing, staleLearners []Replica
	for _, replica := range sortedReplicas(group.Replicas) {
		want[replica.ReplicaID] = replica
		if replica.Role == ReplicaVoter {
			if _, present := s.voters[replica.ReplicaID]; !present {
				incoming = append(incoming, replica)
			}
		}
	}
	for id := range s.voters {
		if replica, keep := want[id]; !keep || replica.Role != ReplicaVoter {
			old, _ := replicaIdentity(s.base.provenGroup(s.groupID), group, id)
			outgoing = append(outgoing, old)
		}
	}
	for id := range s.learners {
		if _, keep := want[id]; !keep {
			old, _ := replicaIdentity(s.base.provenGroup(s.groupID), group, id)
			staleLearners = append(staleLearners, old)
		}
	}
	slices.SortFunc(outgoing, compareReplicaIdentity)
	slices.SortFunc(staleLearners, compareReplicaIdentity)
	return voterMoves{want: want, incoming: incoming, outgoing: outgoing, staleLearners: staleLearners}
}

func (s *voterPlanState) retireStale(moves voterMoves, status GroupStatus) {
	initialNative := idSet(status.ConfState.Voters)
	for _, id := range status.ConfState.Learners {
		initialNative[id] = struct{}{}
	}
	// A learner that is no longer desired can be fully retired before a pair,
	// releasing genuine native capacity without dropping voter safety.
	for _, replica := range moves.staleLearners {
		s.remove(replica)
	}
	if previous := s.base.provenGroup(s.groupID); previous != nil {
		for _, replica := range sortedReplicas(previous.Replicas) {
			if _, kept := moves.want[replica.ReplicaID]; kept {
				continue
			}
			if _, native := initialNative[replica.ReplicaID]; !native {
				s.remove(replica) // prepared durable state still needs an exact receipt
			}
		}
	}
}

func (s *voterPlanState) replaceOutgoing(view voterPlanTopology, group *Group, moves *voterMoves) error {
	for _, departing := range moves.outgoing {
		if len(moves.incoming) == 0 {
			if err := s.destroy(departing); err != nil {
				return err
			}
			continue
		}
		index := pairIncomingIndex(view, departing, moves.incoming)
		arrival := moves.incoming[index]
		moves.incoming = slices.Delete(moves.incoming, index, index+1)
		if err := s.replacePair(group, departing, arrival); err != nil {
			return err
		}
	}
	return nil
}

func (s *voterPlanState) addIncoming(group *Group, incoming []Replica) error {
	for _, replica := range incoming {
		if err := s.ensureLearner(replica); err != nil {
			return err
		}
		s.wait(replica)
		if err := s.promote(replica); err != nil {
			return err
		}
	}
	for _, replica := range sortedReplicas(group.Replicas) {
		if replica.Role == ReplicaLearner {
			if err := s.ensureLearner(replica); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *voterPlanState) validateFinal() error {
	if !sameVoterIDs(s.voters, s.desired.VotersOf(s.groupID)) {
		return planUnsafef("group %q has no safe transition to its desired voter set", s.groupID)
	}
	violations, err := targetVoterViolations(s.base, s.desired, s.groupID, voterKeys(*s))
	if err != nil {
		return err
	}
	if len(violations) != 0 {
		return planUnsafef("group %q final placement is unsatisfied: %s", s.groupID, sortedMapKeys(violations)[0])
	}
	if err := ValidateVoterPlacement(s.desired, s.groupID, voterKeys(*s)); err != nil {
		return planUnsafef("group %q final placement is unsatisfied: %v", s.groupID, err)
	}
	return nil
}

func (s *voterPlanState) replacePair(group *Group, departing, arrival Replica) error {
	if _, alreadyLearner := s.learners[arrival.ReplicaID]; !alreadyLearner &&
		len(s.voters)+len(s.learners) >= s.maximum &&
		targetReplicaByID(group, departing.ReplicaID) == nil {
		// Exact-identity replacement at a hard host cap has no learner
		// seat. Release the old seat first only when the observed live
		// quorum and every intermediate placement rule permit it.
		if err := s.destroy(departing); err != nil {
			return err
		}
		return s.addVoter(arrival)
	}
	if err := s.ensureLearner(arrival); err != nil {
		return err
	}
	if s.canPromote(arrival) && s.canDestroyAfterPromotion(departing, arrival) {
		s.wait(arrival)
		if err := s.promote(arrival); err != nil {
			return err
		}
		return s.destroy(departing)
	}
	// The first wait is a readiness prerequisite for the pair. The second
	// immediately precedes promotion and must be rechecked by execution.
	s.wait(arrival)
	if err := s.destroy(departing); err != nil {
		return err
	}
	s.wait(arrival)
	return s.promote(arrival)
}

func (s *voterPlanState) addVoter(replica Replica) error {
	if err := s.ensureLearner(replica); err != nil {
		return err
	}
	s.wait(replica)
	return s.promote(replica)
}
