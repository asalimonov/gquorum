//go:build clustertest

package cluster

import (
	"net"
	"testing"
)

func TestHostPortReservationHandoffRetainsStoppedContainerLease(t *testing.T) {
	t.Parallel()
	number, listener, err := reserveHostPort()
	if err != nil {
		t.Fatal(err)
	}
	ports := &hostPorts{numbers: []int{number}, listeners: []net.Listener{listener}}
	t.Cleanup(ports.release)
	address := listener.Addr().String()
	if duplicate, err := net.Listen("tcp4", address); err == nil {
		_ = duplicate.Close()
		t.Fatal("reservation did not exclude another host listener")
	}
	ports.handoff()
	replacement, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatal("handoff did not release the socket for Docker", err)
	}
	defer replacement.Close()
	hostPortLeases.Lock()
	retained := hostPortLeases.used[number]
	hostPortLeases.Unlock()
	if !retained {
		t.Fatal("handoff released the lease needed for retained container restarts")
	}
	other, next, err := reserveHostPort()
	if err != nil {
		t.Fatal(err)
	}
	defer (&hostPorts{numbers: []int{other}, listeners: []net.Listener{next}}).release()
	if other == number {
		t.Fatal("concurrent lease reused a retained container's port")
	}
}
