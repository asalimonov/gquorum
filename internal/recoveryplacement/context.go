// Package recoveryplacement adapts pure topology validation for exact retained recovery deficits.
package recoveryplacement

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Context contains only immutable scopes obtained from replicated import state.
// Zero context keeps ordinary topology behavior. Applications cannot provide it
// through public Node or Cluster methods.
type Context struct {
	cluster  string
	version  uint64
	policies map[string]model.PolicyState
	phase    string
	survivor storage.Identity
	groups   map[string]storage.Identity
}

// FromImport validates and owns private policy provenance. Callers must obtain
// state from the authoritative metadata FSM, never an observation or request.
func FromImport(state *model.ImportState, target topology.Document) (Context, error) {
	if state == nil {
		return Context{}, nil
	}
	if err := validateImportHeader(state, target); err != nil {
		return Context{}, err
	}
	groups := map[string]bool{topology.MetaGroupID: true}
	if err := collectImportGroups(state, target, groups); err != nil {
		return Context{}, err
	}
	context := Context{cluster: target.ClusterID, version: state.Version,
		policies: make(map[string]model.PolicyState),
		phase:    state.Phase, survivor: state.Survivor, groups: make(map[string]storage.Identity)}
	context.groups[topology.MetaGroupID] = state.Survivor
	for _, group := range state.Groups {
		context.groups[group.GroupID] = group.Destination
	}
	for _, policy := range state.Policies {
		if !groups[policy.GroupID] || context.policies[policy.GroupID].GroupID != "" {
			return Context{}, fault.ErrOperationAuthorization
		}
		sum, err := hex.DecodeString(policy.PolicyDigest)
		if err != nil || len(sum) != sha256.Size || strings.ToLower(policy.PolicyDigest) != policy.PolicyDigest {
			return Context{}, fault.ErrOperationAuthorization
		}
		context.policies[policy.GroupID] = policy
	}
	if len(context.policies) != len(groups) {
		return Context{}, fault.ErrOperationAuthorization
	}
	return context, nil
}

// PolicyDigest binds original count targets and every non-cardinality policy.
// Replica membership and descriptive labels are separate identity/transition facts.
func PolicyDigest(doc topology.Document, groupID string) (string, error) {
	group := doc.Group(groupID)
	if group == nil {
		return "", fault.ErrInvalid
	}
	value := struct {
		GroupID   string
		Storage   topology.StorageClass
		Placement topology.PlacementRule
		Voters    *topology.VoterConfig
		Hosting   *topology.HostingRule
	}{groupID, group.Storage, doc.GroupPlacement(groupID), group.VoterConfig, group.Hosting}
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (c Context) applies(doc topology.Document, group string) bool {
	policy, found := c.policies[group]
	if !found || policy.PolicyReleased || c.cluster != doc.ClusterID || doc.Version < c.version {
		return false
	}
	digest, err := PolicyDigest(doc, group)
	return err == nil && digest == policy.PolicyDigest
}

func validateImportHeader(state *model.ImportState, target topology.Document) error {
	if state.Schema != model.SchemaVersion || state.Version != 2 || state.Version > target.Version ||
		state.PlanDigest == "" || state.Survivor.ReplicaID == 0 || state.Survivor.GroupID != topology.MetaGroupID ||
		state.Source.ClusterID == state.Survivor.ClusterID || state.Survivor.ClusterID != target.ClusterID {
		return fault.ErrOperationAuthorization
	}
	if state.Source.InstanceID != "" || state.Source.MemberID == 0 || state.Source.ClusterID == "" ||
		state.Survivor.ReplicaID <= state.Source.MemberID || state.Survivor.NodeName != state.Source.NodeName {
		return fault.ErrOperationAuthorization
	}
	if model.ValidateRecoveryID(state.RecoveryID) != nil || topology.ValidateID(state.Source.NodeName) != nil {
		return fault.ErrOperationAuthorization
	}
	digest, err := hex.DecodeString(state.PlanDigest)
	if err != nil || len(digest) != sha256.Size || strings.ToLower(state.PlanDigest) != state.PlanDigest {
		return fault.ErrOperationAuthorization
	}
	if state.Phase != "importing" && state.Phase != "complete" {
		return fault.ErrOperationAuthorization
	}
	return nil
}

func collectImportGroups(state *model.ImportState, target topology.Document, groups map[string]bool) error {
	for _, group := range state.Groups {
		if group.Destination.ReplicaID == 0 || group.Destination.GroupID != group.GroupID ||
			group.Destination.ClusterID != target.ClusterID || groups[group.GroupID] {
			return fault.ErrOperationAuthorization
		}
		if topology.ValidateLoadID(group.GroupID) != nil {
			return fault.ErrOperationAuthorization
		}
		if group.Destination.NodeName != state.Survivor.NodeName {
			return fault.ErrOperationAuthorization
		}
		groups[group.GroupID] = true
	}
	return nil
}
