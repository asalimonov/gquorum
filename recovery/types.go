//revive:disable:max-public-structs Keep the recovery value vocabulary together for API navigation.

package recovery

import "github.com/asalimonov/gquorum/internal/recovery/model"

// Mode selects rebuilding a survivor or resetting an old participant for admission.
type Mode = model.Mode

// Request identifies an audited recovery inspection and its desired lineage.
type Request = model.Request

// Acceptance records exact-plan loss and deployment-fencing attestations.
type Acceptance = model.Acceptance

// Position identifies a retained Raft log position.
type Position = model.Position

// Remedy suggests an operator response without authorizing an effect.
type Remedy = model.Remedy

// Issue describes a recovery blocker or observation.
type Issue = model.Issue

// GroupRecovery reports retained evidence for one required group.
type GroupRecovery = model.GroupRecovery

// Summary reports old work preserved in the backup rather than resumed.
type Summary = model.Summary

// Plan is an owned operator report, never executable authority.
type Plan = model.Plan

// Operation identifies one durable accepted recovery.
type Operation = model.Operation

// FilesAction selects an idempotent application-owned file operation.
type FilesAction = model.FilesAction

// FilesTask supplies an owned node-wide backup or undo request.
type FilesTask = model.FilesTask

// FilesHook backs up or restores application-owned files and joins all work before returning.
type FilesHook = model.FilesHook

// Resources bounds encoded recovery input sizes.
type Resources = model.Resources

// Options selects the application-files hook and optional resource limits.
type Options = model.Options

// Phase describes durable local recovery progress.
type Phase = model.Phase

// GroupProgress reports restoration progress for one exact load replica.
type GroupProgress = model.GroupProgress

// StatusEvent records a bounded durable local progress observation.
type StatusEvent = model.StatusEvent

// Status reports durable historical recovery progress, not current quorum.
type Status = model.Status

// Modes choose complete-survivor rebuilding or explicit reset for join.
const (
	Rebuild      = model.Rebuild
	ResetForJoin = model.ResetForJoin
)

// Remedies describe operator responses, never authorization.
const (
	RemedyRetry       = model.RemedyRetry
	RemedyCancel      = model.RemedyCancel
	RemedyInvestigate = model.RemedyInvestigate
)

// FilesBackup and FilesUndo select the application-owned action.
const (
	FilesBackup = model.FilesBackup
	FilesUndo   = model.FilesUndo
)

// Phases describe durable operation progress; rollback values are reserved.
const (
	Approved      = model.Approved
	Files         = model.Files
	Cancelling    = model.Cancelling
	Cancelled     = model.Cancelled
	RollingBack   = model.RollingBack
	RolledBack    = model.RolledBack
	Archiving     = model.Archiving
	AwaitingStart = model.AwaitingStart
	Importing     = model.Importing
	Validating    = model.Validating
	Complete      = model.Complete
	JoinReady     = model.JoinReady
	Joined        = model.Joined
)
