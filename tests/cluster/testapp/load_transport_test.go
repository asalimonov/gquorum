//go:build !functional

package main

import (
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/tests/cluster/testapp/loadpb"
)

func TestLoadTransportPreservesStructuredSafetyRefusal(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"post-change live voter quorum would be unavailable", "replica liveness is unknown", "leader must transfer before removal or demotion"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			want := &membership.SafetyRefusal{GroupID: "work", Operation: "remove_replica", ReplicaID: 2, Reason: reason, LiveVoters: 1, Quorum: 2, CatchUpLag: 17, HealthKnown: true}
			err := replyError(domainReply(want))
			var got *membership.SafetyRefusal
			if !errors.Is(err, fault.ErrUnsafeMembership) || !errors.As(err, &got) || !reflect.DeepEqual(got, want) {
				t.Fatal("transport erased membership classification", err, got)
			}
		})
	}
}

func TestLoadTransportDoesNotInventSafetyDetailsFromUnknownPayload(t *testing.T) {
	t.Parallel()
	for _, payload := range []string{"", "not-json", `{"schema":2,"safety":{"GroupID":"work","ReplicaID":2,"Reason":"replica liveness is unknown"}}`, `{"schema":1,"safety":{}}`} {
		err := replyError(&loadpb.Reply{ErrorCode: string(fault.UnsafeMembership), Message: "refused", Data: []byte(payload)})
		var safety *membership.SafetyRefusal
		if !errors.Is(err, fault.ErrUnsafeMembership) || errors.As(err, &safety) {
			t.Fatal("unknown payload invented a retryable refusal", err)
		}
	}
}
