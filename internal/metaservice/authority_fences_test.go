//go:build !functional

package metaservice

import (
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestPendingFencesSelectNewestBoundaryPerResource(t *testing.T) {
	t.Parallel()
	host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "n1"}
	group := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "work"}
	key := func(revision uint64) topology.AuthorityKey {
		return topology.AuthorityKey{ClusterID: "cluster", GroupID: "work", Revision: revision}
	}
	input := []topology.FenceObligation{
		{Resource: host, Key: key(3)}, {Resource: group, Key: key(2)},
		{Resource: host, Key: key(4)}, {Resource: host, Key: key(3)},
		{Resource: group, Key: key(5), Evidence: &topology.FenceEvidence{}},
	}
	got := latestPendingFences(input)
	if len(got) != 2 || got[0].Resource != group || got[0].Key != key(2) || got[1].Resource != host || got[1].Key != key(4) {
		t.Fatal(got)
	}
	got[0].Key.Revision = 99
	if input[1].Key != key(2) {
		t.Fatal("pending selection aliases catalog input")
	}
}
