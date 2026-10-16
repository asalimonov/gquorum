package metaengine

import (
	"strings"

	"github.com/asalimonov/gquorum/topology"
)

func validateScopedIntents(state *scopedState, index *scopedIndexes) error {
	for _, intent := range state.Intents {
		doc, found := index.documents[intent.Version]
		if !found || intent.Version == 0 || intent.ExpectedVersion != intent.Version-1 ||
			doc.RebasedFrom != 0 || !validIntentParent(intent, doc) || !validIntentFingerprint(intent) {
			return invalidScopedState("invalid accepted intent provenance")
		}
		if err := validateIntentSuccessor(intent, doc); err != nil {
			return err
		}
		if !validIntentTuple(intent) {
			return invalidScopedState("invalid intent replacement tuple")
		}
		if before, retained := index.documents[intent.ExpectedVersion]; retained && intent.PreviousID != 0 {
			if err := validateReplacementIntent(intent, before, doc); err != nil {
				return invalidScopedState("retained intent contradicts its replacement diff")
			}
		}
	}
	return nil
}

// Shared with typed provenance construction/acceptance in Tasks 47–48.
func knownIntentKind(kind string) bool {
	switch kind {
	case "add_node", "readmit_node", "drain_node", "activate_node", "exclude_node", "set_placement",
		"set_group_placement", "create_group", "set_group_voter_config", "set_group_hosting",
		"set_replica_pinned_learner", "delete_group", "change_replica", intentReseatReplica, "rollback_to",
		"force_retire_node":
		return true
	default:
		return false
	}
}

func validateIntentSuccessor(intent Intent, doc topology.Document) error {
	if intent.GroupID != "" && topology.ValidateID(intent.GroupID) != nil {
		return invalidScopedState("invalid intent group identity")
	}
	if intent.SuccessorID == 0 {
		return nil
	}
	group := intent.GroupID
	if group == "" {
		group = topology.MetaGroupID
	}
	replica := documentReplica(doc, group, intent.SuccessorID)
	if replica == nil || replica.NodeName != intent.NodeName {
		return invalidScopedState("accepted intent lacks its exact successor assignment")
	}
	return nil
}

func validIntentTuple(intent Intent) bool {
	if strings.ContainsRune(intent.NodeName, 0) {
		return false
	}
	if intent.Kind == "readmit_node" && intent.GroupID != "" && intent.GroupID != topology.MetaGroupID {
		return false
	}
	if intent.Kind == intentReseatReplica && topology.ValidateLoadID(intent.GroupID) != nil {
		return false
	}
	if intent.Kind == "readmit_node" || intent.Kind == intentReseatReplica {
		return intent.PreviousID != 0 && intent.SuccessorID > intent.PreviousID && intent.NodeName != ""
	}
	return intent.PreviousID == 0
}

// Reseat has repair-source ancestry when independent staged intent is rebased;
// rollback has requested-history ancestry. Their CAS parent stays in the record.
func validIntentParent(intent Intent, doc topology.Document) bool {
	if intent.Kind == "rollback_to" || intent.Kind == intentReseatReplica {
		return doc.ParentVersion <= intent.ExpectedVersion
	}
	return doc.ParentVersion == intent.ExpectedVersion
}
