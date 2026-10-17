package topology

// StepCreateFoundingGroup and the following kinds identify replica, membership
// and cleanup steps.
const (
	StepCreateFoundingGroup StepKind = "create_founding_group"
	StepPrepareReplica      StepKind = "prepare_replica"
	StepAddLearner          StepKind = "add_learner"
	StepWaitCaughtUp        StepKind = "wait_caught_up"
	StepPromoteVoter        StepKind = "promote_voter"
	StepDemoteVoter         StepKind = "demote_voter"
	StepRemoveReplica       StepKind = "remove_replica"
	StepTransferLeader      StepKind = "transfer_leader"
	StepStopReplica         StepKind = "stop_replica"
	StepCleanupReplicaState StepKind = "cleanup_replica_state"
	StepDeleteGroupState    StepKind = "delete_group_state"
)

// Equal compares every operation and identity field of two steps.
func (s Step) Equal(other Step) bool { return s == other }

// Matches compares the complete authorization tuple with an expected operator
// step.
func (a AuthorizedStep) Matches(id OperationID, attempt uint32, version uint64, step Step) bool {
	return a.OperationID == id && a.Attempt == attempt && a.Version == version && a.StepSeq == step.Seq &&
		a.Kind == step.Kind && a.GroupID == step.GroupID && a.NodeName == step.NodeName &&
		a.ReplicaID == step.ReplicaID && a.TransferNodeName == step.TransferNodeName &&
		a.TransferReplicaID == step.TransferReplicaID && a.ExpectedVotersDigest == step.ExpectedVotersDigest &&
		a.ResultVotersDigest == step.ResultVotersDigest
}
