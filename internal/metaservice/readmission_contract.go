package metaservice

import (
	"github.com/asalimonov/gquorum/topology"
)

// Readmission carries exact prior and granted identities, group assignments and
// retirement grants.
type Readmission struct {
	Previous       topology.Identity         `json:"previous"`
	Granted        topology.Identity         `json:"granted"`
	AdmissionID    string                    `json:"admission_id"`
	MetaDBPath     string                    `json:"meta_db_path"`
	PreviousGroups []topology.Group          `json:"previous_groups"`
	GrantedGroups  []topology.Group          `json:"granted_groups"`
	Grants         []topology.AuthorityGrant `json:"grants"`
}

// ReadmissionRequiredError hands exact readmission orchestration back to the
// local Node owner.
type ReadmissionRequiredError struct{ Readmission Readmission }
