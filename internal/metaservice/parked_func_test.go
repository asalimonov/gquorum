//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
)

func TestHandleJoin_RefusalRecordsParkAndFollowersReadCurrentLeader(t *testing.T) {
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	var follower *Group
	for _, g := range groups {
		if g != leader {
			follower = g
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	req := parkedRequest("stranger", "instance")
	data, _ := json.Marshal(req)
	header := &metapb.Header{ClusterId: "cluster", NodeName: req.Identity.NodeName, MemberId: req.Identity.MemberID}
	reply := follower.Call(ctx, "Join", header, data)
	if reply.ErrorCode == "" || len(follower.parked.snapshot()) != 0 {
		t.Fatal("follower redirect parked a process", reply)
	}
	reply = leader.Call(ctx, "Join", header, data)
	if reply.ErrorCode == "" {
		t.Fatal("unapproved node admitted")
	}
	records, err := follower.ParkedJoins(ctx)
	if err != nil || len(records) != 1 || records[0].RefusalKind != parkedUnapproved || records[0].ReportedMemberID != 7 || records[0].ReportedDocVersion != 3 {
		t.Fatal(records, err)
	}
	before := len(leader.parked.snapshot())
	header.ClusterId = "other"
	leader.Call(ctx, "Join", header, data)
	header.ClusterId = "cluster"
	header.NodeName = ""
	req = parkedRequest("", "bad")
	data, _ = json.Marshal(req)
	leader.Call(ctx, "Join", header, data)
	if len(leader.parked.snapshot()) != before {
		t.Fatal("malformed/foreign-cluster join parked")
	}
	// A follower refuses its local registry over the wire, even when it has old entries.
	follower.parked.record(parkedRequest("old-leader-only", "old"), parkedExcluded, "stale")
	if err = metatransport.ReplyError(follower.Call(ctx, "ParkedRead", follower.transport.Header(), nil)); err == nil {
		t.Fatal("follower published stale local registry")
	}
}
