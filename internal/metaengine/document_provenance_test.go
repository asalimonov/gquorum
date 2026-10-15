//go:build !functional

package metaengine

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestOrdinaryDocumentDerivationClearsInheritedRebase(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"stage", "admit"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			m := convergedAuthorityFixture(t)
			// The atomic rebase installer is not active yet. Seed its generated
			// field to exercise existing Clone-based paths independently of it.
			m.latest.RebasedFrom = 1
			var proposal any = ProposeDocument{Doc: m.latest.Clone(), ExpectedVersion: m.latest.Version}
			if kind == "admit" {
				proposal = AdmitNode{Node: topology.Node{Name: "n4", Type: "worker"}, MemberID: 4, At: time.Now().UTC()}
			}
			result := apply(t, m.topologySM, 6, proposal)
			if !result.Accepted || result.Doc.RebasedFrom != 0 || m.latest.RebasedFrom != 0 {
				t.Fatal("inherited provenance prevented an ordinary edit or leaked into it", result)
			}
			for _, doc := range m.history {
				if doc.Version == result.Doc.Version && doc.RebasedFrom != 0 {
					t.Fatal("history persisted generated provenance from an ordinary edit")
				}
			}
		})
	}
}
