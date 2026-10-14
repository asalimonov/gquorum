package membership

import "go.etcd.io/raft/v3/raftpb"

// AddLearner constructs a single automatic Raft change that adds a learner.
func AddLearner(replicaID uint64) raftpb.ConfChangeV2 {
	return singleChange(raftpb.ConfChangeAddLearnerNode, replicaID)
}

// PromoteLearner constructs a single automatic Raft change that promotes a
// learner.
func PromoteLearner(replicaID uint64) raftpb.ConfChangeV2 {
	return singleChange(raftpb.ConfChangeAddNode, replicaID)
}

// RemoveReplica constructs a single automatic Raft change that removes a
// replica.
func RemoveReplica(replicaID uint64) raftpb.ConfChangeV2 {
	return singleChange(raftpb.ConfChangeRemoveNode, replicaID)
}

// DemoteVoter constructs a single automatic Raft change that demotes a voter to
// learner.
func DemoteVoter(replicaID uint64) raftpb.ConfChangeV2 {
	return singleChange(raftpb.ConfChangeAddLearnerNode, replicaID)
}
