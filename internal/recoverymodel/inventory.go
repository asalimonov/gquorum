package recoverymodel

import (
	"github.com/asalimonov/gquorum/storage"
)

// ReplicaEvidence classifies an exact approved or proven historical identity.
// It is a source-inspection projection, never permission to execute an old grant.
type ReplicaEvidence struct {
	Identity       storage.Identity `json:"identity"`
	MemberID       uint64           `json:"member_id"`
	SourceVersion  uint64           `json:"source_version"`
	Classification string           `json:"classification"`
}
