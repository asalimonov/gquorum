//go:build !functional

package metaengine

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestProgressCursorsRejectMalformedOrChangedFilters(t *testing.T) {
	t.Parallel()
	s := progressFilterFixture(t)
	query := topology.ProgressQuery{GroupID: "a", Limit: 1}
	first, err := queryScopedProgress(&s, query)
	if err != nil || first.Next == "" {
		t.Fatal(first, err)
	}
	for _, changed := range []topology.ProgressQuery{
		{}, {GroupID: "b"}, {GroupID: "a", NodeName: "old"}, {GroupID: "a", OperationID: "exec-v2-a1"},
	} {
		changed.After = first.Next
		if _, err := queryScopedProgress(&s, changed); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("filter changed", changed, err)
		}
	}
	query.After, query.Limit = first.Next, 1000
	if _, err := queryScopedProgress(&s, query); err != nil {
		t.Fatal("limit unnecessarily bound to cursor", err)
	}
	s.Latest.ClusterID = "foreign"
	if _, err := queryScopedProgress(&s, query); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("foreign cursor", err)
	}
	s.Latest.ClusterID = "c"
	for _, limit := range []int{-1, 1001} {
		if _, err := queryScopedProgress(&s, topology.ProgressQuery{Limit: limit}); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(limit, err)
		}
	}
	filters := progressFilterHash("c", topology.ProgressQuery{})
	valid := progressCursor{Version: 1, Last: progressKey{Kind: progressGroup, ID: "a"}, Filters: filters}
	for _, mutate := range []func(*progressCursor){
		func(c *progressCursor) { c.Version = 2 },
		func(c *progressCursor) { c.Filters = "wrong" },
		func(c *progressCursor) { c.Last.Kind = 0 },
		func(c *progressCursor) { c.Last.Kind = 5 },
		func(c *progressCursor) { c.Last.ID = "" },
		func(c *progressCursor) { c.Last.ID = "bad/group" },
		func(c *progressCursor) { c.Last.Scope = "extra" },
		func(c *progressCursor) { c.Last.ReplicaID = 1 },
		func(c *progressCursor) { c.Last.Kind = progressOperation },
		func(c *progressCursor) { c.Last = progressKey{Kind: progressCleanup, ID: "a", Scope: "node"} },
	} {
		bad := valid
		mutate(&bad)
		data, _ := json.Marshal(bad)
		if _, err := queryScopedProgress(&s, topology.ProgressQuery{After: base64.RawURLEncoding.EncodeToString(data)}); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("invalid cursor accepted", string(data), err)
		}
	}
	data, _ := json.Marshal(valid)
	for _, bad := range []string{"%", "", "null", "{}", string(data) + " {}", string(data[:len(data)-1]) + `,"extra":1}`} {
		after := base64.RawURLEncoding.EncodeToString([]byte(bad))
		if bad == "" {
			after = "%"
		}
		if _, err := queryScopedProgress(&s, topology.ProgressQuery{After: after}); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("malformed cursor accepted", bad, err)
		}
	}
}
