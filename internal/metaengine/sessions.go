package metaengine

import (
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

type sessionState struct {
	SessionID  string `json:"session_id"`
	NodeName   string `json:"node_name"`
	InstanceID string `json:"instance_id"`
	TTLSeconds int    `json:"ttl_seconds"`
}

func (s *scopedState) applyScopedGrantSession(_ uint64, p GrantSession) (proposalApplication, error) {
	if p.SessionID == "" || p.NodeName == "" || p.InstanceID == "" || p.TTLSeconds <= 0 {
		return proposalApplication{result: rejectProposal(p.ProposalID, proposalGuardrail,
			"session requires exact identity and positive TTL")}, nil
	}
	identity := topology.Identity{ClusterID: s.Target.ClusterID, NodeName: p.NodeName,
		MemberID: s.Target.MemberID(p.NodeName)}
	if _, admitted := s.admissionBasis(identity); !admitted {
		return proposalApplication{result: rejectProposal(p.ProposalID, proposalAuthorization,
			"session requires proven approved metadata membership")}, nil
	}
	for _, existing := range s.Sessions {
		if existing.SessionID != p.SessionID {
			continue
		}
		if existing.NodeName != p.NodeName || existing.InstanceID != p.InstanceID ||
			existing.TTLSeconds != p.TTLSeconds {
			result := rejectProposal(p.ProposalID, proposalAuthorization, "session identity differs")
			return proposalApplication{result: result}, nil
		}
		return proposalApplication{result: ApplyResult{ProposalID: p.ProposalID, Accepted: true}}, nil
	}
	next := *s
	next.Sessions = append(slices.Clone(s.Sessions), sessionState{SessionID: p.SessionID,
		NodeName: p.NodeName, InstanceID: p.InstanceID, TTLSeconds: p.TTLSeconds})
	if !slices.Contains(next.Activated, p.NodeName) {
		next.Activated = append(slices.Clone(s.Activated), p.NodeName)
	}
	*s = next
	return proposalApplication{changed: true, result: ApplyResult{ProposalID: p.ProposalID, Accepted: true}}, nil
}

func (s *scopedState) applyScopedRevokeSession(_ uint64, p RevokeSession) (proposalApplication, error) {
	next := *s
	next.Sessions = slices.DeleteFunc(slices.Clone(s.Sessions), func(row sessionState) bool {
		return row.SessionID == p.SessionID
	})
	if len(next.Sessions) == len(s.Sessions) {
		return proposalApplication{result: ApplyResult{ProposalID: p.ProposalID, Accepted: true}}, nil
	}
	next.Locks = slices.DeleteFunc(slices.Clone(s.Locks), func(row lockEntry) bool {
		return row.SessionID == p.SessionID
	})
	*s = next
	return proposalApplication{changed: true, result: ApplyResult{ProposalID: p.ProposalID, Accepted: true}}, nil
}

func (s *scopedState) applyScopedAcquireLock(_ uint64, p AcquireLock) (proposalApplication, error) {
	if p.Key == "" {
		return proposalApplication{result: rejectProposal(p.ProposalID, proposalGuardrail, "lock key required")}, nil
	}
	var acquiring *sessionState
	for i := range s.Sessions {
		if s.Sessions[i].SessionID == p.SessionID {
			acquiring = &s.Sessions[i]
			break
		}
	}
	if acquiring == nil {
		return proposalApplication{result: ApplyResult{ProposalID: p.ProposalID,
			Reason: fmt.Sprintf("lock %q: acquiring session %q does not exist", p.Key, p.SessionID)}}, nil
	}
	for _, held := range s.Locks {
		if held.Key != p.Key {
			continue
		}
		if held.SessionID == p.SessionID {
			return proposalApplication{result: ApplyResult{ProposalID: p.ProposalID, Accepted: true}}, nil
		}
		for _, owner := range s.Sessions {
			if owner.SessionID == held.SessionID {
				message := fmt.Sprintf("lock %q already held by a live session of node %q",
					p.Key, owner.NodeName)
				return proposalApplication{result: ApplyResult{ProposalID: p.ProposalID,
					Reason: message}}, nil
			}
		}
	}
	next := *s
	next.Locks = append(slices.Clone(s.Locks), lockEntry{Key: p.Key, SessionID: p.SessionID})
	*s = next
	return proposalApplication{changed: true, result: ApplyResult{ProposalID: p.ProposalID, Accepted: true}}, nil
}

type lockEntry struct {
	Key       string `json:"key"`
	SessionID string `json:"session_id"`
}
