package model

// StatusFromManifest projects history without claiming a running process or quorum.
func StatusFromManifest(r LocalManifest) Status {
	identity := r.Identity
	if r.JoinedIdentity != nil {
		identity = *r.JoinedIdentity
	}
	identity.InstanceID = ""
	status := Status{History: CloneHistory(r.History), BackupName: r.BackupName,
		AcceptedAt: r.AcceptedAt, RecoveryID: r.Request.RecoveryID,
		PlanDigest: r.PlanDigest, Mode: r.Request.Mode, Phase: r.Phase, Identity: identity,
		Resources: r.Resources, Issue: clonePointer(r.Failure),
		Complete: r.Phase == Complete || r.Phase == JoinReady || r.Phase == Joined}
	for _, seed := range r.Seeds {
		status.Groups = append(status.Groups, GroupProgress{GroupID: seed.GroupID, State: seed.State,
			Applied: seed.Checkpoint})
	}
	if r.Phase == RollingBack || r.Phase == RolledBack {
		status.Issue = &Issue{Code: "unsupported_rollback", Blocking: true,
			Message: "rollback state is reserved and cannot be resumed by this version", Remedy: RemedyInvestigate}
	}
	return status
}
