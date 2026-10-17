package metaengine

import (
	"math"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// API_RETRY: replicated replay runs before document validation or allocation;
// an accepted historical request need not still be a valid new mutation.
func (s *scopedState) applyProposeDocument(index uint64, p ProposeDocument) (proposalApplication, error) {
	if p.ExpectedVersion != s.Latest.Version {
		result, err := retainedMutation(p, s.Latest.Version, s.indexes)
		return proposalApplication{result: result}, err
	}
	if index <= s.AppliedIndex || s.Latest.Version == math.MaxUint64 {
		return proposalApplication{}, fault.ErrInvalid
	}
	doc := p.Doc.Clone()
	doc.Version, doc.RebasedFrom = s.Latest.Version+1, 0
	canonicalizeDocument(&doc)
	if err := s.validateStagedDocument(doc); err != nil {
		return proposalApplication{}, err
	}
	if p.Intent != nil {
		if err := validateProposedIntent(*p.Intent, s.Latest, doc, s.Latest.Version); err != nil {
			return proposalApplication{}, err
		}
	}
	report, err := placementResult(s.Target, doc, topology.AffectedPlacementGroups(s.Latest, doc))
	if err != nil {
		return proposalApplication{}, err
	}
	next := *s
	next.installStagedDocument(doc, p.Intent)
	next.AppliedIndex = index
	next.indexes, err = prepareScopedGraph(&next, next.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s = next
	return proposalApplication{changed: true, result: ApplyResult{Accepted: true,
		ProposalID: p.ProposalID, Doc: doc.Clone(), Placement: report}}, nil
}

func (s *scopedState) validateStagedDocument(doc topology.Document) error {
	if doc.ParentVersion >= doc.Version {
		return fault.New(fault.Invalid, "document parent must precede its assigned version", nil)
	}
	retired := make(map[string]bool, len(s.RetiredGroups))
	for _, group := range s.RetiredGroups {
		retired[group.ID] = true
	}
	// This inventory supplies retained identities to transition validation only;
	// it is never used as a converged topology or planning source.
	if err := topology.ValidateTransition(retainedIdentities(s.Latest, s.Target), doc, s.Rules, retired); err != nil {
		return err
	}
	if err := validateScopedPlacement(s.Target, doc, s.Rules); err != nil {
		return err
	}
	if err := s.validateRepairAllocations(doc); err != nil {
		return err
	}
	return s.validateRebasedFloors(doc)
}

func (s *scopedState) installStagedDocument(doc topology.Document, intent *Intent) {
	history := slices.Clone(s.History)
	for _, retained := range []topology.Document{s.Latest, doc} {
		if retained.Version != 0 && !slices.ContainsFunc(history, func(d topology.Document) bool {
			return d.Version == retained.Version
		}) {
			history = append(history, retained.Clone())
		}
	}
	s.History, s.Latest = history, doc.Clone()
	s.rememberScopedAllocations(doc)
	if intent != nil {
		s.Intents = append(slices.Clone(s.Intents), *intent)
	}
}

func validateProposedIntent(intent Intent, before, after topology.Document, parent uint64) error {
	if intent.ExpectedVersion != before.Version || intent.Version != after.Version ||
		!validIntentFingerprint(intent) || !validIntentTuple(intent) {
		return fault.ErrInvalid
	}
	// Rollback's ParentVersion identifies its requested historical source. CAS
	// ancestry remains explicit in ExpectedVersion, independently of that lineage.
	if intent.Kind != "rollback_to" && after.ParentVersion != parent {
		return fault.ErrInvalid
	}
	if err := validateIntentSuccessor(intent, after); err != nil {
		return err
	}
	if intent.Kind == "readmit_node" || intent.Kind == intentReseatReplica {
		return validateReplacementIntent(intent, before, after)
	}
	if intent.PreviousID != 0 {
		return fault.ErrInvalid
	}
	return nil
}

func validateReplacementIntent(intent Intent, before, after topology.Document) error {
	group := intent.GroupID
	if intent.Kind == "readmit_node" {
		if group != "" && group != topology.MetaGroupID {
			return fault.ErrInvalid
		}
		group = topology.MetaGroupID
	} else if topology.ValidateLoadID(group) != nil {
		return fault.ErrInvalid
	}
	prior := documentReplica(before, group, intent.PreviousID)
	successor := documentReplica(after, group, intent.SuccessorID)
	if intent.PreviousID == 0 || intent.SuccessorID <= intent.PreviousID || prior == nil || successor == nil ||
		prior.NodeName != intent.NodeName || successor.NodeName != intent.NodeName ||
		documentReplica(after, group, intent.PreviousID) != nil {
		return fault.New(fault.IdentityMismatch, "mutation provenance does not match the exact replacement diff", nil)
	}
	return nil
}
