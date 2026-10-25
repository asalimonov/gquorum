// Package recoverymodel contains shared recovery values and private persistence records.
//
//revive:disable:max-public-structs Recovery reports share one immutable value vocabulary.
package recoverymodel

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Mode selects rebuilding a survivor or resetting an old participant for admission.
type Mode string

// Rebuild and ResetForJoin select supported recovery modes.
const (
	Rebuild      Mode = "rebuild"
	ResetForJoin Mode = "reset_for_join"
)

// Request identifies an audited recovery inspection and its desired lineage.
type Request struct {
	RecoveryID     string   `json:"recovery_id,omitempty"` // empty: generate during Inspect
	Mode           Mode     `json:"mode"`
	ClusterID      string   `json:"cluster_id,omitempty"`
	Actor          string   `json:"actor"`
	Comment        string   `json:"comment"`
	RequiredGroups []string `json:"required_groups,omitempty"`
}

// Acceptance records exact-plan loss and deployment-fencing attestations.
type Acceptance struct {
	PlanDigest          string `json:"plan_digest"`
	AcknowledgeDataLoss bool   `json:"acknowledge_data_loss"`
	OldDeploymentFenced bool   `json:"old_deployment_fenced"`
}

// Position identifies a retained Raft log position.
type Position struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
}

// Remedy suggests an operator response without authorizing an effect.
type Remedy string

// Remedies suggest retrying, cancelling, or investigating an operation.
const (
	RemedyRetry       Remedy = "retry"
	RemedyCancel      Remedy = "cancel"
	RemedyInvestigate Remedy = "investigate"
)

// Issue describes a recovery blocker or observation.
type Issue struct {
	Code     string `json:"code"`
	GroupID  string `json:"group_id,omitempty"`
	Blocking bool   `json:"blocking"`
	Message  string `json:"message"`
	Remedy   Remedy `json:"remedy,omitempty"`
}

// GroupRecovery reports retained evidence for one required group.
type GroupRecovery struct {
	GroupID  string           `json:"group_id"`
	Source   storage.Identity `json:"source"`
	Snapshot Position         `json:"snapshot"`
	Commit   Position         `json:"commit"`
	LastLog  Position         `json:"last_log"`
	Bytes    uint64           `json:"bytes"`
	Usable   bool             `json:"usable"`
}

// Summary reports old work preserved in the backup rather than resumed.
type Summary struct {
	LatestVersion        uint64                 `json:"latest_version"`
	ApprovedVersion      uint64                 `json:"approved_version"`
	OperationsNotResumed []topology.OperationID `json:"operations_not_resumed"`
	UnresolvedCleanup    []storage.Identity     `json:"unresolved_cleanup"`
	SessionsNotRestored  uint64                 `json:"sessions_not_restored"`
	LocksNotRestored     uint64                 `json:"locks_not_restored"`
	RetiredGroupIDs      []string               `json:"retired_group_ids"`
}

// Plan is an owned operator report, never executable authority.
type Plan struct {
	Digest                    string            `json:"digest"`
	Request                   Request           `json:"request"`
	Source                    topology.Identity `json:"source"`
	Identity                  topology.Identity `json:"identity"`
	Resources                 Resources         `json:"resources"`
	Groups                    []GroupRecovery   `json:"groups"`
	Archived                  Summary           `json:"archived"`
	Issues                    []Issue           `json:"issues"`
	MayLoseAcknowledgedWrites bool              `json:"may_lose_acknowledged_writes"`
}

// Operation identifies one durable accepted recovery.
type Operation struct {
	RecoveryID string    `json:"recovery_id"`
	PlanDigest string    `json:"plan_digest"`
	BackupName string    `json:"backup_name"`
	AcceptedAt time.Time `json:"accepted_at"`
	Mode       Mode      `json:"mode"`
}

// FilesAction selects an idempotent application-owned file operation.
type FilesAction string

// FilesBackup and FilesUndo select application backup and restoration.
const (
	FilesBackup FilesAction = "backup"
	FilesUndo   FilesAction = "undo"
)

// FilesTask supplies an owned node-wide application-file action.
type FilesTask struct {
	Action     FilesAction       `json:"action"`
	RecoveryID string            `json:"recovery_id"`
	PlanDigest string            `json:"plan_digest"`
	Mode       Mode              `json:"mode"`
	Source     topology.Identity `json:"source"`
	ClusterID  string            `json:"cluster_id"`
	Groups     []string          `json:"groups"`
	BackupDir  string            `json:"backup_dir"`
}

// FilesHook backs up or restores application-owned files and joins all work before returning.
type FilesHook func(context.Context, FilesTask) error

// Resources bounds encoded input bytes; zero fields select defaults.
type Resources struct {
	MaxRecordBytes   uint64 `json:"max_record_bytes,omitempty"`
	MaxGroupBytes    uint64 `json:"max_group_bytes,omitempty"`
	MaxMetadataBytes uint64 `json:"max_metadata_bytes,omitempty"`
}

// Options selects the application-files hook and optional resource limits.
type Options struct {
	Files     FilesHook
	Resources Resources
}

// Phase describes durable local recovery progress.
type Phase string

// Phases retain durable operation progress; rollback phases are reserved.
const (
	Approved   Phase = "approved"
	Files      Phase = "files"
	Cancelling Phase = "cancelling"
	Cancelled  Phase = "cancelled"
	// Reserved values: this version never initiates rollback.
	RollingBack   Phase = "rolling_back"
	RolledBack    Phase = "rolled_back"
	Archiving     Phase = "archiving"
	AwaitingStart Phase = "awaiting_start"
	Importing     Phase = "importing"
	Validating    Phase = "validating"
	Complete      Phase = "complete"
	JoinReady     Phase = "join_ready"
	Joined        Phase = "joined"
)

// GroupProgress reports restoration progress for one exact load replica.
type GroupProgress struct {
	GroupID string   `json:"group_id"`
	State   string   `json:"state"` // pending, prepared, installed, replayed, consumed
	Applied Position `json:"applied"`
	Issue   *Issue   `json:"issue,omitempty"`
}

// StatusEvent records one durable local progress observation.
type StatusEvent struct {
	At      time.Time `json:"at"`
	Phase   Phase     `json:"phase"`
	Message string    `json:"message"`
	GroupID string    `json:"group_id,omitempty"`
	Issue   *Issue    `json:"issue,omitempty"`
}

// Status reports durable historical recovery progress, not current quorum.
type Status struct {
	History    []StatusEvent     `json:"history"`
	BackupName string            `json:"backup_name"`
	AcceptedAt time.Time         `json:"accepted_at"`
	RecoveryID string            `json:"recovery_id"`
	PlanDigest string            `json:"plan_digest"`
	Mode       Mode              `json:"mode"`
	Phase      Phase             `json:"phase"`
	Identity   topology.Identity `json:"identity"`
	Resources  Resources         `json:"resources"`
	Groups     []GroupProgress   `json:"groups"`
	Issue      *Issue            `json:"issue,omitempty"`
	Complete   bool              `json:"complete"`
}
