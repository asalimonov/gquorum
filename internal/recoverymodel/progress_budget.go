package recoverymodel

import (
	"crypto/sha256"
	"math"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

// ValidateJournalBudget reserves the encoded growth needed after acceptance,
// before any application backup or irreversible library archive begins.
func ValidateJournalBudget(input LocalManifest) error {
	future := CloneManifest(input)
	future.History = nil
	future.SuccessorStartedAt = &future.AcceptedAt
	future.CompletedAt = &future.AcceptedAt
	future.CancelledAt = &future.AcceptedAt
	future.Cancellation = &ActionAudit{Actor: future.Request.Actor, Reason: future.Request.Comment,
		At: future.AcceptedAt}
	future.Failure = &Issue{Code: "recovery_operation_failed", Message: strings.Repeat("\\", diagnosticBytes),
		Blocking: true, Remedy: RemedyInvestigate}
	if future.Request.Mode == ResetForJoin {
		joined := future.Identity
		joined.MemberID = math.MaxUint64
		future.JoinedIdentity = &joined
	}
	for i := range future.Seeds {
		seed := &future.Seeds[i]
		seed.State = stateConsumed
		seed.Attempt = math.MaxUint64
		seed.Destination = storage.Identity{ClusterID: future.Identity.ClusterID, NodeName: future.Identity.NodeName,
			GroupID: seed.GroupID, ReplicaID: math.MaxUint64}
		seed.MarkerDigest = strings.Repeat("f", sha256.Size*2)
		seed.Checkpoint = Position{Index: math.MaxUint64, Term: math.MaxUint64}
	}
	data, err := EncodeCanonical(future)
	if err != nil {
		return err
	}
	const reservedHistoryBytes = HistoryMaxEvents * historyEventBytes
	if uint64(len(data))+reservedHistoryBytes+HeaderMaxBytes > ManifestMaxBytes {
		return fault.New(fault.Invalid, "recovery manifest cannot retain bounded progress/history", nil)
	}
	return nil
}
