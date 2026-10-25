package model

const (
	statePending   = "pending"
	stateRunning   = "running"
	stateComplete  = "complete"
	statePrepared  = "prepared"
	stateInstalled = "installed"
	stateReplayed  = "replayed"
	stateConsumed  = "consumed"
	archiveSystem  = "system"
	archiveWAL     = "wal"
)
const (
	seedPendingRank = iota
	seedPreparedRank
	seedInstalledRank
	seedReplayedRank
	seedConsumedRank
)

var seedRanks = map[string]int{statePending: seedPendingRank, statePrepared: seedPreparedRank,
	stateInstalled: seedInstalledRank, stateReplayed: seedReplayedRank, stateConsumed: seedConsumedRank}
