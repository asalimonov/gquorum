//revive:disable:max-public-structs Keep the recovery value vocabulary together for API navigation.

package recovery

import "github.com/asalimonov/gquorum/internal/recoverymodel"

// Mode selects rebuilding a survivor or resetting an old participant for admission.
type Mode = recoverymodel.Mode

// Request identifies an audited recovery inspection and its desired lineage.
type Request = recoverymodel.Request

// Acceptance records exact-plan loss and deployment-fencing attestations.
type Acceptance = recoverymodel.Acceptance

// Position identifies a retained Raft log position.
type Position = recoverymodel.Position

// Remedy suggests an operator response without authorizing an effect.
type Remedy = recoverymodel.Remedy

// Issue describes a recovery blocker or observation.
type Issue = recoverymodel.Issue

// GroupRecovery reports retained evidence for one required group.
type GroupRecovery = recoverymodel.GroupRecovery

// Summary reports old work preserved in the backup rather than resumed.
type Summary = recoverymodel.Summary

// Plan is an owned operator report, never executable authority.
type Plan = recoverymodel.Plan

// Operation identifies one durable accepted recovery.
type Operation = recoverymodel.Operation

// FilesAction selects an idempotent application-owned file operation.
type FilesAction = recoverymodel.FilesAction

// FilesTask supplies an owned node-wide backup or undo request.
type FilesTask = recoverymodel.FilesTask

// FilesHook backs up or restores application-owned files and joins all work before returning.
type FilesHook = recoverymodel.FilesHook

// Resources bounds encoded recovery input sizes.
type Resources = recoverymodel.Resources

// Options selects the application-files hook and optional resource limits.
type Options = recoverymodel.Options

// Phase describes durable local recovery progress.
type Phase = recoverymodel.Phase

// GroupProgress reports restoration progress for one exact load replica.
type GroupProgress = recoverymodel.GroupProgress

// Status reports durable historical recovery progress, not current quorum.
type Status = recoverymodel.Status

// Modes choose complete-survivor rebuilding or explicit reset for join.
const (
	Rebuild      = recoverymodel.Rebuild
	ResetForJoin = recoverymodel.ResetForJoin
)

// Remedies describe operator responses, never authorization.
const (
	RemedyRetry       = recoverymodel.RemedyRetry
	RemedyCancel      = recoverymodel.RemedyCancel
	RemedyInvestigate = recoverymodel.RemedyInvestigate
)

// FilesBackup and FilesUndo select the application-owned action.
const (
	FilesBackup = recoverymodel.FilesBackup
	FilesUndo   = recoverymodel.FilesUndo
)

// Phases describe durable operation progress; rollback values are reserved.
const (
	Approved      = recoverymodel.Approved
	Files         = recoverymodel.Files
	Cancelling    = recoverymodel.Cancelling
	Cancelled     = recoverymodel.Cancelled
	RollingBack   = recoverymodel.RollingBack
	RolledBack    = recoverymodel.RolledBack
	Archiving     = recoverymodel.Archiving
	AwaitingStart = recoverymodel.AwaitingStart
	Importing     = recoverymodel.Importing
	Validating    = recoverymodel.Validating
	Complete      = recoverymodel.Complete
	JoinReady     = recoverymodel.JoinReady
	Joined        = recoverymodel.Joined
)
