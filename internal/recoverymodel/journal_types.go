//revive:disable:max-public-structs These records are the complete private recovery journal schema.
package recoverymodel

import (
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Paths retains private caller-configured storage roots.
type Paths struct {
	StorageDir string `json:"storage_dir"`
	LoadWALDir string `json:"load_wal_dir"`
}

// Current is a private recovery persistence value.
type Current struct {
	BackupName string `json:"backup_name"`
	Schema     uint32 `json:"schema"`
	RecoveryID string `json:"recovery_id"`
	PlanDigest string `json:"plan_digest"`
}

// LocalManifest is a private recovery persistence value.
type LocalManifest struct {
	History            []StatusEvent      `json:"history,omitempty"`
	BackupName         string             `json:"backup_name"`
	Schema             uint32             `json:"schema"`
	Revision           uint64             `json:"revision"`
	Request            Request            `json:"request"`
	Acceptance         Acceptance         `json:"acceptance"`
	Paths              Paths              `json:"paths"`
	Resources          Resources          `json:"resources"`
	Source             topology.Identity  `json:"source"`
	Identity           topology.Identity  `json:"identity"`
	Fingerprint        string             `json:"fingerprint"`
	PlanDigest         string             `json:"plan_digest"`
	Rules              topology.Rules     `json:"rules"`
	Approved           topology.Document  `json:"approved"`
	Floors             []AllocationFloor  `json:"floors"`
	Retired            []string           `json:"retired"`
	Sources            []SeedSource       `json:"sources"`
	Archive            []ArchiveEntry     `json:"archive"`
	Files              FilesProgress      `json:"files"`
	Seeds              []SeedProgress     `json:"seeds"`
	Phase              Phase              `json:"phase"`
	Failure            *Issue             `json:"failure,omitempty"`
	AcceptedAt         time.Time          `json:"accepted_at"`
	CompletedAt        *time.Time         `json:"completed_at,omitempty"`
	JoinedIdentity     *topology.Identity `json:"joined_identity,omitempty"`
	Cancellation       *ActionAudit       `json:"cancellation,omitempty"`
	CancelledAt        *time.Time         `json:"cancelled_at,omitempty"`
	SuccessorStartedAt *time.Time         `json:"successor_started_at,omitempty"`
	Rollback           *ActionAudit       `json:"rollback,omitempty"`       // reserved; never initiated in v1
	RolledBackAt       *time.Time         `json:"rolled_back_at,omitempty"` // reserved
}

// AllocationFloor is a private recovery persistence value.
type AllocationFloor struct {
	GroupID string `json:"group_id"`
	NextID  uint64 `json:"next_id"`
}

// SeedSource is a private recovery persistence value.
// TODO(not planned): streaming snapshots/hard RSS control; encoded byte bounds
// retain full-buffer application contracts. Never reconstruct commit from an
// application projection or repair/truncate the source log.
type SeedSource struct {
	GroupID       string           `json:"group_id"`
	Identity      storage.Identity `json:"identity"`
	MemberID      uint64           `json:"member_id"`
	SourceVersion uint64           `json:"source_version"`
	Snapshot      Position         `json:"snapshot"`
	Commit        Position         `json:"commit"`
	LastLog       Position         `json:"last_log"`
	HardStateTerm uint64           `json:"hard_state_term"`
	Digest        string           `json:"digest"`
	Bytes         uint64           `json:"bytes"`
}

// ArchiveEntry is a private recovery persistence value.
type ArchiveEntry struct {
	Root           string `json:"root"` // system or wal
	Name           string `json:"name"` // one top-level relative entry
	Kind           string `json:"kind"` // regular_file or directory
	Classification string `json:"classification"`
	EvidenceDigest string `json:"evidence_digest"`
	Moved          bool   `json:"moved"`
	Restored       bool   `json:"restored"` // reserved reverse-move proof
}

// ActionAudit is a private recovery persistence value.
type ActionAudit struct {
	Actor  string    `json:"actor"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// FilesProgress is a private recovery persistence value.
type FilesProgress struct {
	Required bool         `json:"required"`
	Backup   HookProgress `json:"backup"`
	Undo     HookProgress `json:"undo"`
}

// HookProgress is a private recovery persistence value.
type HookProgress struct {
	State   string `json:"state"` // pending, running, complete
	Attempt uint64 `json:"attempt"`
}

// SeedProgress is a private recovery persistence value.
type SeedProgress struct {
	GroupID      string           `json:"group_id"`
	Destination  storage.Identity `json:"destination"`
	State        string           `json:"state"` // pending/prepared/installed/replayed/consumed
	Attempt      uint64           `json:"attempt"`
	MarkerDigest string           `json:"marker_digest,omitempty"`
	Checkpoint   Position         `json:"checkpoint"`
}

// SeedMarker is a private recovery persistence value.
type SeedMarker struct {
	Schema      uint32                  `json:"schema"`
	RecoveryID  string                  `json:"recovery_id"`
	PlanDigest  string                  `json:"plan_digest"`
	Source      SeedSource              `json:"source"`
	Destination storage.Identity        `json:"destination"`
	MemberID    uint64                  `json:"member_id"`
	Grant       topology.AuthorityGrant `json:"grant"`
	ImageDigest string                  `json:"image_digest"`
}

// SourceOptions retains only caller-configured source roots and expected durable identity.
type SourceOptions struct {
	Paths     Paths
	NodeName  string
	ClusterID string
}

// AcceptedPlan contains immutable inspection evidence, excluding approval and progress.
type AcceptedPlan struct {
	Request       Request           `json:"request"`
	Paths         Paths             `json:"paths"`
	Resources     Resources         `json:"resources"`
	Source        topology.Identity `json:"source"`
	Identity      topology.Identity `json:"identity"`
	Fingerprint   string            `json:"fingerprint"`
	Rules         topology.Rules    `json:"rules"`
	Approved      topology.Document `json:"approved"`
	Floors        []AllocationFloor `json:"floors"`
	Retired       []string          `json:"retired"`
	Sources       []SeedSource      `json:"sources"`
	Archive       []ArchiveEntry    `json:"archive"`
	FilesRequired bool              `json:"files_required"`
	Archived      Summary           `json:"archived"`
	Groups        []GroupRecovery   `json:"groups"`
	Issues        []Issue           `json:"issues,omitempty"`
}

// SeedKey binds a load seed to the exact accepted operation and destination owner.
type SeedKey struct {
	RecoveryID, PlanDigest string
	Destination            storage.Identity
	MemberID               uint64
}

// SeedSpec supplies the accepted source and ordinary destination creation grant.
type SeedSpec struct {
	Key        SeedKey
	BackupName string
	Source     SeedSource
	Grant      topology.AuthorityGrant
	Resources  Resources
}

// CheckpointProof identifies the actual durable normalized destination snapshot.
type CheckpointProof struct {
	Identity storage.Identity
	Position Position
	Digest   string
}
