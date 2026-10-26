//revive:disable:max-public-structs Keep transfer states and bounded observation values together.
package snapshot

import "time"

// DeliveryState describes transport progress, independently of installation.
// Its empty zero value carries no delivery evidence.
type DeliveryState string

// Delivery states never promote native ingress acceptance to installation proof.
const (
	DeliveryReceiving  DeliveryState = "receiving"
	DeliveryVerifying  DeliveryState = "verifying"
	DeliveryHandingOff DeliveryState = "handing_off"
	DeliveryAccepted   DeliveryState = "accepted"
	DeliveryNotNeeded  DeliveryState = "not_needed"
	DeliveryFailed     DeliveryState = "failed"
)

// ReceiverOutcome describes receiver-owner evidence. The empty zero value and
// ReceiverUnknown both mean that the installation outcome is unknown.
type ReceiverOutcome string

// Receiver outcomes are meaningful only for their exact snapshot and owner.
const (
	ReceiverUnknown   ReceiverOutcome = "unknown"
	ReceiverInstalled ReceiverOutcome = "installed"
	ReceiverSkipped   ReceiverOutcome = "skipped"
	ReceiverFailed    ReceiverOutcome = "failed"
)

// Status is an owned, local transfer observation. Timestamps retain their
// original meaning when cached. Sender observations normally have an unknown
// ReceiverOutcome; successful transport delivery is not an install receipt.
type Status struct {
	Key             Key
	Direction       string
	SnapshotIndex   uint64
	SnapshotTerm    uint64
	TotalBytes      uint64
	VerifiedBytes   uint64
	StartedAt       time.Time
	LastProgressAt  time.Time
	Deadline        time.Time
	Delivery        DeliveryState
	ReceiverOutcome ReceiverOutcome
	ReservedBytes   uint64
	ErrorReason     string
	// AdmissionRefused distinguishes a local refusal from an issued delivery.
	// Its key has DeliveryID zero and only the destination's group/replica IDs;
	// resolving the remote node is deliberately deferred until admission.
	AdmissionRefused      bool
	ErrorSize, ErrorLimit uint64
	Refusals              uint64
	LastRefusedAt         time.Time
}

// Query selects bounded local observations. An empty GroupID selects all
// groups. The cursor is not a frozen snapshot; zero Limit selects the default.
type Query struct {
	GroupID string
	Cursor  uint64
	Limit   int
}

// Page contains owned observations and the node-wide admission counters. An
// empty page is not evidence of successful delivery or available quorum.
type Page struct {
	Items                   []Status
	NextCursor              uint64
	Truncated               bool
	Incoming, Outgoing      int
	ReceiveBytes, SendBytes uint64
}
