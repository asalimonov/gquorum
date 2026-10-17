package metaengine

import (
	"cmp"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type retiredGroup struct {
	ID      string `json:"id"`
	Version uint64 `json:"version"`
}
type allocationRecord struct {
	GroupID       string `json:"group_id"`
	NextReplicaID uint64 `json:"next_replica_id"`
}

func canonicalizeDocument(d *topology.Document) {
	slices.SortFunc(d.Nodes, func(a, b topology.Node) int { return cmp.Compare(a.Name, b.Name) })
	slices.SortFunc(d.Groups, func(a, b topology.Group) int { return cmp.Compare(a.ID, b.ID) })
	for i := range d.Groups {
		slices.SortFunc(d.Groups[i].Replicas, func(a, b topology.Replica) int {
			return cmp.Compare(a.ReplicaID, b.ReplicaID)
		})
	}
}

func validateRestoredRetirements(records []retiredGroup, latest, target topology.Document) error {
	retired := make(map[string]bool)
	for _, r := range records {
		if r.Version == 0 || r.Version > latest.Version || retired[r.ID] || topology.ValidateLoadID(r.ID) != nil {
			return fault.New(fault.Unreadable, "invalid retired group", nil)
		}
		retired[r.ID] = true
		if latest.Group(r.ID) != nil || target.Group(r.ID) != nil {
			return fault.New(fault.Unreadable, "retired group appears in desired topology", nil)
		}
	}
	return nil
}

func validateRestoredSessions(records []sessionState, held []lockEntry) error {
	sessions := make(map[string]bool)
	for _, s := range records {
		if s.SessionID == "" || s.NodeName == "" || s.InstanceID == "" || s.TTLSeconds <= 0 || sessions[s.SessionID] {
			return fault.New(fault.Unreadable, "invalid session record", nil)
		}
		sessions[s.SessionID] = true
	}
	locks := make(map[string]bool)
	for _, l := range held {
		if l.Key == "" || !sessions[l.SessionID] || locks[l.Key] {
			return fault.New(fault.Unreadable, "invalid lock record", nil)
		}
		locks[l.Key] = true
	}
	return nil
}

func validateRestoredAdmissions(records []AdmissionRecord, seen map[topology.OperationID]bool) error {
	admissions := make(map[string]AdmissionRecord)
	liveAdmissions := make(map[string]bool)
	for _, a := range records {
		key := admissionKey(a.NodeName, a.InstanceID)
		_, exists := admissions[key]
		identityErr := validateAdmissionIdentity(a)
		if exists || identityErr != nil {
			return fault.New(fault.Unreadable, "invalid admission record", nil)
		}
		admissions[key] = a
		if !a.State.valid() {
			return fault.New(fault.Unreadable, "unknown admission state", nil)
		}
		if a.ReadmissionOperatorID != "" && !seen[topology.OperationID(a.ReadmissionOperatorID)] {
			return fault.New(fault.Unreadable, "missing admission origin operation", nil)
		}
		if !a.State.terminal() {
			if !seen[a.OperationID] || liveAdmissions[a.NodeName] {
				return fault.New(fault.Unreadable, "invalid live admission execution", nil)
			}
			liveAdmissions[a.NodeName] = true
		}
	}
	return validateReadmissionOrigins(records, admissions)
}

func validateReadmissionOrigins(records []AdmissionRecord, admissions map[string]AdmissionRecord) error {
	for _, a := range records {
		if a.ReadmissionInstanceID == "" {
			continue
		}
		original, found := admissions[admissionKey(a.NodeName, a.ReadmissionInstanceID)]
		if !found || original.ReadmissionInstanceID != "" || original.MemberID != a.MemberID ||
			original.State != AdmissionSuperseded || original.originOperator() != a.originOperator() {
			return fault.New(fault.Unreadable, "readmission origin does not name the exact superseded admission", nil)
		}
	}
	return nil
}

func retainedIdentities(latest, target topology.Document) topology.Document {
	base := latest.Clone()
	for _, group := range target.Groups {
		current := base.Group(group.ID)
		if current == nil {
			base.Groups = append(base.Groups, group)
			continue
		}
		for _, r := range group.Replicas {
			conflict := false
			for _, retained := range current.Replicas {
				if retained.NodeName == r.NodeName || retained.ReplicaID == r.ReplicaID {
					conflict = true
					break
				}
			}
			if !conflict {
				current.Replicas = append(current.Replicas, r)
			}
		}
	}
	return base
}
