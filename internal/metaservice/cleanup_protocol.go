package metaservice

import (
	"bytes"
	"encoding/json"
	"io"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const (
	collectionReportKind = "report_collection"
	cleanupStatusKind    = "report_cleanup_status"
)

type collectionRequest struct {
	ReporterMemberID uint64                           `json:"reporter_member_id,omitempty"`
	Kind             string                           `json:"kind"`
	Receipt          authoritystore.CollectionReceipt `json:"receipt"`
	ReadmissionKey   *topology.AuthorityKey           `json:"readmission_key,omitempty"`
}

type cleanupStatusRequest struct {
	ReporterMemberID uint64                  `json:"reporter_member_id,omitempty"`
	Kind             string                  `json:"kind"`
	Identity         storage.Identity        `json:"identity"`
	MemberID         uint64                  `json:"member_id"`
	RetirementKey    topology.AuthorityKey   `json:"retirement_key"`
	NotBefore        *time.Time              `json:"not_before,omitempty"`
	Blocked          *topology.BlockedReason `json:"blocked,omitempty"`
}

func decodeCleanupRequest(data []byte, into any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(into) != nil || decoder.Decode(new(any)) != io.EOF {
		return fault.ErrInvalid
	}
	return nil
}
