package metaengine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ReplayMutation inspects historical acceptance without allocating or writing.
// Task78 replaces this schema3 adapter with the scoped intent/document indexes.
func (m *Machine) ReplayMutation(p ProposeDocument) (ApplyResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	index := &scopedIndexes{documents: make(map[uint64]topology.Document)}
	for _, doc := range m.history {
		index.documents[doc.Version] = doc
	}
	index.documents[m.latest.Version] = m.latest
	return retainedMutation(p, m.latest.Version, index)
}

func retainedMutation(p ProposeDocument, latest uint64, index *scopedIndexes) (ApplyResult, error) {
	conflict := &fault.Error{Code: fault.VersionConflict, ExpectedVersion: p.ExpectedVersion, CurrentVersion: latest}
	if p.ExpectedVersion >= latest || p.ExpectedVersion == math.MaxUint64 {
		return ApplyResult{}, conflict
	}
	accepted := index.documents[p.ExpectedVersion+1]
	if p.Intent != nil {
		if !validIntentFingerprint(*p.Intent) {
			return ApplyResult{}, fault.ErrInvalid
		}
		prior := index.intents[intentKey{p.ExpectedVersion, p.Intent.Kind, p.Intent.Digest}]
		if prior != nil {
			if !sameIntentIdentity(*p.Intent, *prior) {
				return ApplyResult{}, conflict
			}
			accepted = index.documents[prior.Version]
		} else if accepted.Version != 0 {
			return ApplyResult{}, conflict
		}
	} else if accepted.Version != 0 && !sameRawStage(p.Doc, accepted) {
		return ApplyResult{}, conflict
	}
	if accepted.Version == 0 {
		return ApplyResult{}, fault.New(fault.HistoryUnavailable, "retained history cannot prove mutation outcome", nil)
	}
	result := ApplyResult{Accepted: true, ProposalID: p.ProposalID, Doc: accepted.Clone(),
		OperatorID: topology.OperatorID(index.first[accepted.Version])}
	result.RebasedTo = rebasedSuccessor(accepted.Version, index)
	return result, nil
}

func validIntentFingerprint(intent Intent) bool {
	digest, err := hex.DecodeString(intent.Digest)
	return err == nil && len(digest) == sha256.Size && hex.EncodeToString(digest) == intent.Digest &&
		knownIntentKind(intent.Kind)
}

func sameIntentIdentity(request, accepted Intent) bool {
	return request.GroupID == accepted.GroupID && request.NodeName == accepted.NodeName &&
		request.PreviousID == accepted.PreviousID &&
		(request.SuccessorID == 0 || request.SuccessorID == accepted.SuccessorID)
}

// Raw Stage keeps exact IDs, allocators, supplied timestamps and audit. Only
// fields overwritten unconditionally by acceptance are not caller intent.
func sameRawStage(request, accepted topology.Document) bool {
	left, right := request.Clone(), accepted.Clone()
	left.Version, left.RebasedFrom, right.Version, right.RebasedFrom = 0, 0, 0, 0
	canonicalizeDocument(&left)
	canonicalizeDocument(&right)
	a, err := json.Marshal(left)
	if err != nil {
		return false
	}
	b, err := json.Marshal(right)
	return err == nil && string(a) == string(b)
}
