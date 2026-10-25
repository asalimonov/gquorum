package recoverymodel

import "slices"

// PlanFromAccepted returns owned operator evidence without executable internals.
func PlanFromAccepted(input AcceptedPlan) (Plan, error) {
	accepted := CloneAcceptedPlan(input)
	plan := Plan{Request: accepted.Request, Source: accepted.Source, Identity: accepted.Identity,
		Resources: accepted.Resources,
		Groups:    accepted.Groups, Archived: accepted.Archived, Issues: accepted.Issues,
		MayLoseAcknowledgedWrites: true}
	if accepted.Fingerprint == "" {
		return plan, nil
	}
	digest, err := DigestAccepted(accepted)
	plan.Digest = digest
	return plan, err
}

// PlanFromManifest projects immutable accepted evidence without reading evolving B.
func PlanFromManifest(manifest LocalManifest) (Plan, error) {
	accepted := AcceptedFromManifest(manifest)
	accepted.Archived.ApprovedVersion = manifest.Approved.Version
	accepted.Archived.RetiredGroupIDs = slices.Clone(manifest.Retired)
	for _, source := range manifest.Sources {
		accepted.Groups = append(accepted.Groups, GroupRecovery{GroupID: source.GroupID,
			Source: source.Identity, Snapshot: source.Snapshot,
			Commit: source.Commit, LastLog: source.LastLog, Bytes: source.Bytes, Usable: true})
	}
	accepted.Issues = append(accepted.Issues, Issue{Code: "historical_plan",
		Message: "historical source evidence; full old operation and cleanup summaries remain in the backup",
		Remedy:  RemedyInvestigate})
	return PlanFromAccepted(accepted)
}

// OperationFromManifest returns the exact durable approval identity and timestamp.
func OperationFromManifest(manifest LocalManifest) Operation {
	return Operation{RecoveryID: manifest.Request.RecoveryID, PlanDigest: manifest.PlanDigest,
		BackupName: manifest.BackupName,
		AcceptedAt: manifest.AcceptedAt, Mode: manifest.Request.Mode}
}
