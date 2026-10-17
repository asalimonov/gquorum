//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestScopedCleanupPageBoundsOwnedProofAndStableCursor(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	slices.Reverse(s.Cleanup)
	prepareScopedFixture(t, &s)
	before, _ := json.Marshal(s)
	first, err := queryScopedCleanupPage(&s, "", 1)
	if err != nil || len(first.Records) != 1 || first.Records[0].Identity.GroupID != "a" || first.Next == "" {
		t.Fatal("first page is not bounded/exact", first, err)
	}
	first.Records[0].Collection.Token = "caller"
	first.Records[0].Removed.Action.NodeName = "caller"
	second, err := queryScopedCleanupPage(&s, first.Next, 1)
	if err != nil || len(second.Records) != 1 || second.Records[0].Identity.GroupID != "b" || second.Next != "" {
		t.Fatal("cursor skipped/repeated the next exact identity", second, err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("page or its cursor changed retained proof")
	}
	// Successive pages observe current state; a new identity behind the cursor
	// appears on a fresh scan, without a retained server-side snapshot/lease.
	inserted := s.Cleanup[0].clone()
	inserted.Identity.GroupID = "0"
	s.Cleanup = append(s.Cleanup, inserted)
	s.indexes, err = s.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	continued, err := queryScopedCleanupPage(&s, first.Next, 100)
	if err != nil || len(continued.Records) != 1 || continued.Records[0].Identity.GroupID != "b" {
		t.Fatal("insert behind cursor changed continuation", continued, err)
	}
	fresh, err := queryScopedCleanupPage(&s, "", 0)
	if err != nil || len(fresh.Records) != 3 || fresh.Records[0].Identity.GroupID != "0" {
		t.Fatal("fresh scan did not discover newly retained debt", fresh, err)
	}
}

func TestScopedCleanupPageRejectsMalformedOrForeignCursor(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	prepareScopedFixture(t, &s)
	for _, cursor := range []string{"%", `{}`, `{"version":2,"last":{}}`,
		`{"version":1,"last":{"cluster_id":"foreign","group_id":"a","node_name":"n","replica_id":7}}`,
		`{"version":1,"last":{"cluster_id":"c","group_id":"a","node_name":"n","replica_id":7},"extra":1}`} {
		after := cursor
		if cursor != "%" {
			after = base64.RawURLEncoding.EncodeToString([]byte(cursor))
		}
		if _, err := queryScopedCleanupPage(&s, after, 1); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("malformed cursor accepted", cursor, err)
		}
	}
	for _, limit := range []int{-1, 1001} {
		if _, err := queryScopedCleanupPage(&s, "", limit); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("unbounded page accepted", limit, err)
		}
	}
}
