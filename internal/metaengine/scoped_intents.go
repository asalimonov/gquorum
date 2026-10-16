package metaengine

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/asalimonov/gquorum/topology"
)

func validateScopedIntents(state *scopedState, index *scopedIndexes) error {
	for _, intent := range state.Intents {
		doc, found := index.documents[intent.Version]
		digest, err := hex.DecodeString(intent.Digest)
		if !found || err != nil || len(digest) != sha256.Size || intent.ExpectedVersion >= intent.Version ||
			doc.ParentVersion != intent.ExpectedVersion ||
			doc.RebasedFrom != 0 || !knownIntentKind(intent.Kind) ||
			hex.EncodeToString(digest) != intent.Digest {
			return invalidScopedState("invalid accepted intent provenance")
		}
		if err := validateIntentSuccessor(intent, doc); err != nil {
			return err
		}
		if intent.PreviousID != 0 && (intent.SuccessorID <= intent.PreviousID || intent.NodeName == "") ||
			strings.ContainsRune(intent.NodeName, 0) {
			return invalidScopedState("invalid intent replacement tuple")
		}
	}
	return nil
}

// Shared with typed provenance construction/acceptance in Tasks 46–47.
func knownIntentKind(kind string) bool {
	switch kind {
	case "add_node", "readmit_node", "drain_node", "activate_node", "exclude_node", "set_placement",
		"set_group_placement", "create_group", "set_group_voter_config", "set_group_hosting",
		"set_replica_pinned_learner", "delete_group", "change_replica", "reseat_replica", "rollback_to",
		"force_retire_node":
		return true
	default:
		return false
	}
}

func validateIntentSuccessor(intent intentRecord, doc topology.Document) error {
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
