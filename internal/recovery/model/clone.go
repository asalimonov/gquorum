package model

import "slices"

// CloneRequest copies caller-owned group assertions.
func CloneRequest(value Request) Request {
	value.RequiredGroups = slices.Clone(value.RequiredGroups)
	return value
}

// ClonePlan copies all mutable report values.
func ClonePlan(value Plan) Plan {
	value.Request = CloneRequest(value.Request)
	value.Groups = slices.Clone(value.Groups)
	value.Issues = slices.Clone(value.Issues)
	value.Archived = cloneSummary(value.Archived)
	return value
}

// CloneStatus copies operation observations without sharing diagnostic pointers.
func CloneStatus(value Status) Status {
	value.History = CloneHistory(value.History)
	value.Issue = clonePointer(value.Issue)
	value.Groups = slices.Clone(value.Groups)
	for i := range value.Groups {
		value.Groups[i].Issue = clonePointer(value.Groups[i].Issue)
	}
	return value
}

// CloneManifest copies the journal's immutable evidence and mutable progress.
func CloneManifest(value LocalManifest) LocalManifest {
	value.History = CloneHistory(value.History)
	value.Request = CloneRequest(value.Request)
	value.Approved = value.Approved.Clone()
	value.Floors = slices.Clone(value.Floors)
	value.Retired = slices.Clone(value.Retired)
	value.Sources = slices.Clone(value.Sources)
	value.Archive = slices.Clone(value.Archive)
	value.Seeds = slices.Clone(value.Seeds)
	value.Failure = clonePointer(value.Failure)
	value.CompletedAt = clonePointer(value.CompletedAt)
	value.JoinedIdentity = clonePointer(value.JoinedIdentity)
	value.Cancellation = clonePointer(value.Cancellation)
	value.CancelledAt = clonePointer(value.CancelledAt)
	value.SuccessorStartedAt = clonePointer(value.SuccessorStartedAt)
	value.Rollback = clonePointer(value.Rollback)
	value.RolledBackAt = clonePointer(value.RolledBackAt)
	return value
}

// CloneAcceptedPlan owns the evidence used for inspection and approval.
func CloneAcceptedPlan(value AcceptedPlan) AcceptedPlan {
	value.Request = CloneRequest(value.Request)
	value.Approved = value.Approved.Clone()
	value.Floors = slices.Clone(value.Floors)
	value.Retired = slices.Clone(value.Retired)
	value.Sources = slices.Clone(value.Sources)
	value.Archive = slices.Clone(value.Archive)
	value.Archived = cloneSummary(value.Archived)
	value.Groups = slices.Clone(value.Groups)
	value.Issues = slices.Clone(value.Issues)
	return value
}

// CloneImport preserves ownership of retained receipts and recovery policy scopes.
func CloneImport(value ImportState) ImportState {
	value.Groups = slices.Clone(value.Groups)
	for i := range value.Groups {
		if prior := value.Groups[i].Consumed; prior != nil {
			consumed := *prior
			consumed.HostReceipt = prior.HostReceipt.Clone()
			consumed.GroupReceipt = prior.GroupReceipt.Clone()
			value.Groups[i].Consumed = &consumed
		}
	}
	value.Policies = slices.Clone(value.Policies)
	value.CompletedAt = clonePointer(value.CompletedAt)
	return value
}

func cloneSummary(value Summary) Summary {
	value.OperationsNotResumed = slices.Clone(value.OperationsNotResumed)
	value.UnresolvedCleanup = slices.Clone(value.UnresolvedCleanup)
	value.RetiredGroupIDs = slices.Clone(value.RetiredGroupIDs)
	return value
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	owned := *value
	return &owned
}

// CloneHistory owns each event and optional diagnostic.
func CloneHistory(history []StatusEvent) []StatusEvent {
	result := slices.Clone(history)
	for i := range result {
		result[i].Issue = clonePointer(result[i].Issue)
	}
	return result
}
