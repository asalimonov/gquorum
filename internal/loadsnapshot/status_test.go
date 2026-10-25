//go:build !functional

package loadsnapshot

import (
	"fmt"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
)

func TestSnapshotStatusBoundedPagingAndOwnedCopies(t *testing.T) {
	t.Parallel()
	c := NewCoordinator(snapshot.Options{}, Ports{})
	for i := range 300 {
		group := "a"
		if i%2 == 1 {
			group = "b"
		}
		c.observe(snapshot.Status{Key: snapshot.Key{To: load.Peer{Replica: load.ReplicaRef{GroupID: group}}, DeliveryID: uint64(i + 1)},
			Direction: "incoming", Delivery: snapshot.DeliveryFailed, ErrorReason: strings.Repeat("x", 600)})
	}
	page := c.List(snapshot.Query{Limit: 1000})
	if len(page.Items) != 128 || !page.Truncated || len(page.Items[0].ErrorReason) != 512 {
		t.Fatal(len(page.Items), page.Truncated)
	}
	page.Items[0].ErrorReason = "changed"
	if c.List(snapshot.Query{Limit: 1}).Items[0].ErrorReason == "changed" {
		t.Fatal("caller changed retained history")
	}
	var seen []uint64
	cursor := uint64(0)
	for {
		part := c.List(snapshot.Query{GroupID: "a", Cursor: cursor, Limit: 3})
		for _, row := range part.Items {
			if row.Key.To.Replica.GroupID != "a" {
				t.Fatal(row)
			}
			seen = append(seen, row.Key.DeliveryID)
		}
		if part.NextCursor < cursor {
			t.Fatal("cursor regressed")
		}
		cursor = part.NextCursor
		if !part.Truncated {
			break
		}
	}
	if len(seen) != 128 {
		t.Fatal("filtered history lost rows", len(seen))
	}
	for i, id := range seen {
		if i > 0 && id <= seen[i-1] {
			t.Fatal(fmt.Sprint(seen))
		}
	}
}
