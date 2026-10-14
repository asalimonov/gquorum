package metastore

// ReplicaLookup preserves a retained boot ledger record and its presence.
type ReplicaLookup struct {
	Record ReplicaRecord
	Found  bool
}
