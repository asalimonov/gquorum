package model

import (
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func validateManifestInventory(r LocalManifest) error {
	checks := []func(LocalManifest) error{validateFloorsAndRetired, validateArchiveOrder,
		validateArchiveCoverage, validateSeeds}
	for _, check := range checks {
		if err := check(r); err != nil {
			return err
		}
	}
	previous := ""
	for _, source := range r.Sources {
		if source.GroupID <= previous {
			return fault.New(fault.Invalid, "sources are not sorted and unique", nil)
		}
		previous = source.GroupID
	}
	return nil
}
func validateArchiveOrder(r LocalManifest) error {
	previous := ""
	for _, entry := range r.Archive {
		key := entry.Root + "/" + entry.Name
		if key <= previous {
			return fault.New(fault.Invalid, "archive entries are not sorted and unique", nil)
		}
		if err := ValidateArchiveEntry(entry); err != nil {
			return err
		}
		if entry.Root == archiveWAL && r.Paths.LoadWALDir == "" {
			return fault.ErrInvalid
		}
		previous = key
	}
	return nil
}
func validateArchiveCoverage(r LocalManifest) error {
	if r.Request.Mode != Rebuild {
		return nil
	}
	for _, name := range []string{"_authority.bolt", "_meta.bolt"} {
		if !hasArchive(r.Archive, archiveSystem, name) {
			return fault.ErrInvalid
		}
	}
	for _, source := range r.Sources {
		if source.GroupID != topology.MetaGroupID && !hasArchive(r.Archive, archiveWAL, source.GroupID) {
			return fault.ErrInvalid
		}
	}
	return nil
}
func hasArchive(entries []ArchiveEntry, root, name string) bool {
	for _, entry := range entries {
		if entry.Root == root && entry.Name == name {
			return true
		}
	}
	return false
}
func validateFloorsAndRetired(r LocalManifest) error {
	previous := ""
	floors := make(map[string]uint64, len(r.Floors))
	for _, floor := range r.Floors {
		if floor.NextID == 0 || floor.GroupID <= previous || topology.ValidateID(floor.GroupID) != nil {
			return fault.ErrInvalid
		}
		previous = floor.GroupID
		floors[floor.GroupID] = floor.NextID
	}
	if err := validateRetired(r, floors); err != nil {
		return err
	}
	return validateApprovedFloors(r, floors)
}
func validateRetired(r LocalManifest, floors map[string]uint64) error {
	previous := ""
	for _, group := range r.Retired {
		if group <= previous || floors[group] == 0 || topology.ValidateLoadID(group) != nil ||
			r.Approved.Group(group) != nil {
			return fault.ErrInvalid
		}
		previous = group
	}
	return nil
}
func validateApprovedFloors(r LocalManifest, floors map[string]uint64) error {
	if r.Request.Mode == ResetForJoin {
		return nil
	}
	if floors[topology.MetaGroupID] != r.Identity.MemberID || r.Identity.MemberID <= r.Source.MemberID {
		return fault.ErrInvalid
	}
	for _, group := range r.Approved.Groups {
		if floors[group.ID] == 0 || floors[group.ID] < group.NextReplicaID {
			return fault.ErrInvalid
		}
		for _, replica := range group.Replicas {
			if floors[group.ID] <= replica.ReplicaID {
				return fault.ErrInvalid
			}
		}
	}
	return nil
}

// ValidateArchiveEntry validates one immutable top-level move intent.
func ValidateArchiveEntry(entry ArchiveEntry) error {
	if entry.Root != archiveSystem && entry.Root != archiveWAL || entry.Kind != "regular_file" &&
		entry.Kind != "directory" {
		return fault.ErrInvalid
	}
	if !safeComponent(entry.Name) || strings.HasPrefix(entry.Name, "@") ||
		!validText(entry.Classification) || !validDigest(entry.EvidenceDigest) {
		return fault.ErrInvalid
	}
	if entry.Root == archiveSystem {
		known := entry.Name == "_meta.bolt" || entry.Name == "_authority.bolt" ||
			strings.HasPrefix(entry.Name, "_meta.bolt.retired-")
		if !known || entry.Kind != "regular_file" {
			return fault.ErrInvalid
		}
	}
	if entry.Root == archiveWAL && entry.Kind == "regular_file" {
		if entry.Classification != "collection" || !strings.HasSuffix(entry.Name, ".collection") {
			return fault.ErrInvalid
		}
	}
	return nil
}
func validateSeeds(r LocalManifest) error {
	previous := ""
	for _, seed := range r.Seeds {
		if seed.GroupID <= previous || topology.ValidateLoadID(seed.GroupID) != nil {
			return fault.ErrInvalid
		}
		previous = seed.GroupID
		if err := validateSeedProgress(seed, r); err != nil {
			return err
		}
	}
	if r.Request.Mode == Rebuild && len(r.Seeds) != len(r.Approved.LoadGroups()) {
		return fault.ErrInvalid
	}
	return nil
}
func validateSeedProgress(s SeedProgress, r LocalManifest) error {
	if r.Approved.Group(s.GroupID) == nil {
		return fault.ErrInvalid
	}
	switch s.State {
	case statePending:
		if s.MarkerDigest != "" || s.Checkpoint != (Position{}) {
			return fault.ErrInvalid
		}
	case statePrepared, stateInstalled, stateReplayed, stateConsumed:
		if s.Attempt == 0 || !validDigest(s.MarkerDigest) {
			return fault.ErrInvalid
		}
	default:
		return fault.ErrInvalid
	}
	if s.State != statePending || s.Attempt > 0 || s.Destination != (storage.Identity{}) {
		if err := validateSeedDestination(s, r); err != nil {
			return err
		}
	}
	return nil
}
func validateSeedDestination(s SeedProgress, r LocalManifest) error {
	d := s.Destination
	if d.GroupID != s.GroupID || d.NodeName != r.Identity.NodeName || d.ClusterID != r.Identity.ClusterID ||
		d.ReplicaID == 0 {
		return fault.ErrIdentityMismatch
	}
	if (s.State == stateReplayed || s.State == stateConsumed) && (s.Checkpoint.Index == 0 ||
		s.Checkpoint.Term == 0) {
		return fault.ErrInvalid
	}
	return nil
}

// ValidateSeedMarker validates the complete image/grant binding, never its commit.
func ValidateSeedMarker(m SeedMarker) error {
	if m.Schema != SchemaVersion || ValidateRecoveryID(m.RecoveryID) != nil || !validDigest(m.PlanDigest) ||
		!validDigest(m.ImageDigest) {
		return fault.ErrUnreadable
	}
	sourceOwner := topology.Identity{NodeName: m.Source.Identity.NodeName,
		ClusterID: m.Source.Identity.ClusterID, MemberID: m.Source.MemberID}
	if err := ValidateSeedSource(m.Source, sourceOwner); err != nil {
		return err
	}
	if err := m.Grant.Validate(); err != nil {
		return err
	}
	if err := validateMarkerDestination(m); err != nil {
		return err
	}
	for _, target := range m.Grant.Targets {
		if target.NodeName == m.Destination.NodeName && target.MemberID == m.MemberID &&
			target.ReplicaID == m.Destination.ReplicaID {
			return nil
		}
	}
	return fault.New(fault.IdentityMismatch, "seed lacks its exact grant target", nil)
}
func validateMarkerDestination(m SeedMarker) error {
	d := m.Destination
	if d.NodeName == "" || d.ClusterID == "" || d.ReplicaID == 0 || m.MemberID == 0 ||
		d.GroupID != m.Source.GroupID ||
		d.ClusterID == m.Source.Identity.ClusterID || m.Grant.Key.ClusterID != d.ClusterID ||
		m.Grant.Key.GroupID != d.GroupID || topology.ValidateLoadID(d.GroupID) != nil {
		return fault.ErrIdentityMismatch
	}
	return nil
}
