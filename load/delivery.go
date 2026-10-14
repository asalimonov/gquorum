package load

// DeliveryKind classifies an asynchronous transport completion or failure.
type DeliveryKind string

// Delivery outcomes distinguish unreachable destinations and completed snapshot
// transfers.
const (
	DeliveryUnreachable       DeliveryKind = "unreachable"
	DeliverySnapshotSucceeded DeliveryKind = "snapshot_succeeded"
	DeliverySnapshotFailed    DeliveryKind = "snapshot_failed"
)

// DeliveryReport binds an asynchronous transport outcome to the exact original
// delivery.
type DeliveryReport struct {
	From, To   Peer
	DeliveryID uint64
	Kind       DeliveryKind
}
