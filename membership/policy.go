package membership

import "go.etcd.io/raft/v3/raftpb"

func AddLearner(replicaID uint64) raftpb.ConfChangeV2 {
	return singleChange(raftpb.ConfChangeAddLearnerNode, replicaID)
}

func PromoteLearner(replicaID uint64) raftpb.ConfChangeV2 {
	return singleChange(raftpb.ConfChangeAddNode, replicaID)
}

func RemoveReplica(replicaID uint64) raftpb.ConfChangeV2 {
	return singleChange(raftpb.ConfChangeRemoveNode, replicaID)
}

func DemoteVoter(replicaID uint64) raftpb.ConfChangeV2 {
	return singleChange(raftpb.ConfChangeAddLearnerNode, replicaID)
}
