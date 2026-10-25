package recoverymodel

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ValidateManifest checks accepted authority and progress without opening stores.
func ValidateManifest(r LocalManifest) error {
	checks := []func(LocalManifest) error{validateManifestHeader, validateManifestSource,
		validateManifestInventory, validateManifestProgress}
	for _, check := range checks {
		if err := check(r); err != nil {
			return fault.New(fault.Unreadable, "invalid recovery manifest", err)
		}
	}
	return nil
}

func validateManifestHeader(r LocalManifest) error {
	if r.Schema != SchemaVersion || r.Revision == 0 {
		return fault.ErrInvalid
	}
	if err := ValidateRequest(r.Request); err != nil {
		return err
	}
	name, err := BackupName(r.AcceptedAt, r.Request.RecoveryID)
	if err != nil || name != r.BackupName {
		return fault.New(fault.Invalid, "backup name does not bind acceptance", err)
	}
	if err := ValidatePaths(r.Paths); err != nil {
		return err
	}
	resources, err := ResolveResources(r.Resources)
	if err != nil || resources != r.Resources {
		return fault.New(fault.Invalid, "manifest requires latched resource limits", err)
	}
	if r.Acceptance.PlanDigest != r.PlanDigest || !r.Acceptance.AcknowledgeDataLoss ||
		r.Request.Mode == Rebuild && !r.Acceptance.OldDeploymentFenced || !validDigest(r.PlanDigest) ||
		!validDigest(r.Fingerprint) {
		return fault.ErrInvalid
	}
	digest, err := DigestAccepted(AcceptedFromManifest(r))
	if err != nil || digest != r.PlanDigest {
		return fault.New(fault.Invalid, "accepted plan digest mismatch", err)
	}
	if r.Failure != nil {
		return validateIssue(*r.Failure)
	}
	return nil
}

// ValidatePaths rejects relative, aliased lexical and private recovery roots.
// Physical bindings are checked separately while holding directory leases.
func ValidatePaths(p Paths) error {
	if !normalizedRoot(p.StorageDir) || p.LoadWALDir != "" && !normalizedRoot(p.LoadWALDir) {
		return fault.ErrInvalid
	}
	if p.LoadWALDir != "" {
		relative, err := filepath.Rel(p.LoadWALDir, p.StorageDir)
		if err != nil || relative == "." || relative != ".." && !strings.HasPrefix(relative,
			".."+string(filepath.Separator)) {
			return fault.ErrInvalid
		}
	}
	return nil
}
func normalizedRoot(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) {
		return false
	}
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == "@gquorum-recovery" {
			return false
		}
	}
	return true
}
func validDigest(digest string) bool {
	if len(digest) != sha256.Size*2 || strings.ToLower(digest) != digest {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}
func validateDurableIdentity(id topology.Identity) error {
	if id.InstanceID != "" || topology.ValidateID(id.NodeName) != nil || !validText(id.ClusterID) {
		return fault.ErrIdentityMismatch
	}
	return nil
}
func validateManifestSource(r LocalManifest) error {
	if r.Source.MemberID == 0 || r.Request.Mode == Rebuild && r.Identity.MemberID == 0 {
		return fault.ErrIdentityMismatch
	}
	if err := validateDurableIdentity(r.Source); err != nil {
		return err
	}
	if err := validateDurableIdentity(r.Identity); err != nil {
		return err
	}
	if r.Identity.NodeName != r.Source.NodeName || r.Identity.ClusterID == r.Source.ClusterID ||
		r.Request.ClusterID != "" && r.Request.ClusterID != r.Identity.ClusterID {
		return fault.ErrIdentityMismatch
	}
	if r.Request.Mode == ResetForJoin {
		if r.Identity.MemberID != 0 || len(r.Sources) != 0 || len(r.Seeds) != 0 {
			return fault.ErrInvalid
		}
		return nil
	}
	return validateRebuildSource(r)
}
func validateRebuildSource(r LocalManifest) error {
	if r.Approved.Version == 0 || r.Approved.ClusterID != r.Source.ClusterID {
		return fault.ErrInvalid
	}
	if err := r.Approved.Validate(r.Rules); err != nil {
		return err
	}
	if len(r.Sources) != len(r.Approved.Groups) {
		return fault.New(fault.Invalid, "rebuild requires every approved group", nil)
	}
	for _, source := range r.Sources {
		if err := validateManifestSeed(r, source); err != nil {
			return err
		}
	}
	for _, required := range r.Request.RequiredGroups {
		if r.Approved.Group(required) == nil {
			return fault.ErrInvalid
		}
	}
	return nil
}
func validateManifestSeed(r LocalManifest, source SeedSource) error {
	if source.SourceVersion > r.Approved.Version {
		return fault.ErrInvalid
	}
	group := r.Approved.Group(source.GroupID)
	if group == nil {
		return fault.ErrInvalid
	}
	if group.Storage != topology.StorageDefault {
		return fault.ErrUnsupported
	}
	limit := r.Resources.MaxGroupBytes
	if source.GroupID == topology.MetaGroupID {
		limit = r.Resources.MaxMetadataBytes
	}
	if source.Bytes > limit {
		return fault.ErrInvalid
	}
	return validateSeedSource(source, r.Source)
}

func validateSeedSource(s SeedSource, owner topology.Identity) error {
	if s.Bytes == 0 || s.SourceVersion == 0 || s.Identity.ReplicaID == 0 || s.Identity.GroupID != s.GroupID ||
		s.Identity.NodeName != owner.NodeName || s.Identity.ClusterID != owner.ClusterID ||
		s.MemberID != owner.MemberID ||
		topology.ValidateID(s.GroupID) != nil || !validDigest(s.Digest) {
		return fault.ErrInvalid
	}
	if s.GroupID == topology.MetaGroupID && s.Identity.ReplicaID != owner.MemberID {
		return fault.ErrIdentityMismatch
	}
	if s.Snapshot.Index == 0 || s.Snapshot.Index > s.Commit.Index || s.Commit.Index > s.LastLog.Index ||
		s.Snapshot.Term == 0 || s.Snapshot.Term > s.Commit.Term || s.Commit.Term > s.LastLog.Term ||
		s.LastLog.Term > s.HardStateTerm {
		return fault.ErrInvalid
	}
	return nil
}
func validateIssue(issue Issue) error {
	if !validText(issue.Code) || !validText(issue.Message) || issue.GroupID != "" &&
		topology.ValidateID(issue.GroupID) != nil {
		return fault.ErrInvalid
	}
	switch issue.Remedy {
	case "", RemedyRetry, RemedyCancel, RemedyInvestigate:
		return nil
	default:
		return fault.ErrInvalid
	}
}

// ValidateCurrent checks an index only; the named manifest must still be checked.
func ValidateCurrent(c Current) error {
	if c.Schema != SchemaVersion {
		return fault.ErrUnreadable
	}
	if c.RecoveryID == "" && c.PlanDigest == "" && c.BackupName == "" {
		return nil
	}
	if ValidateRecoveryID(c.RecoveryID) != nil || !validDigest(c.PlanDigest) {
		return fault.ErrUnreadable
	}
	const stampLength = len("20060102T150405Z")
	if len(c.BackupName) != stampLength+1+len(c.RecoveryID) {
		return fault.ErrUnreadable
	}
	at, err := time.Parse("20060102T150405Z", c.BackupName[:stampLength])
	if err != nil {
		return fault.ErrUnreadable
	}
	expected, err := BackupName(at, c.RecoveryID)
	if err != nil || expected != c.BackupName {
		return fault.ErrUnreadable
	}
	return nil
}

func safeComponent(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name &&
		!strings.ContainsAny(name, "/\\\x00")
}
