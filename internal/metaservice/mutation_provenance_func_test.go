//go:build functional

package metaservice

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestMutationNormalizesGeneratedProvenance(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	documents, err := f.g.computeMutation(t.Context(), preview(), mutationPolicy{},
		func(doc *topology.Document, _ time.Time) error {
			doc.RebasedFrom = 9
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if documents.Next.RebasedFrom != 0 || documents.Next.Version != documents.Base.Version+1 ||
		documents.Next.ParentVersion != documents.Base.Version || f.g.machine.Latest().Version != documents.Base.Version {
		t.Fatal("typed preview retained generated provenance or mutated accepted state", documents)
	}
}
