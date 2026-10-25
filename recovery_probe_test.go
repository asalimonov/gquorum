//go:build !functional

package gquorum

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

type unopenedRecoveryResolver struct{}

func (unopenedRecoveryResolver) Resolve(context.Context, string) (string, error) {
	panic("constructor resolved a peer")
}
func TestRecoveryCopiesProbeClientConfigurationWithoutAcquisition(t *testing.T) {
	t.Parallel()
	credential := []byte("credential")
	clientTLS := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "peer"}
	options := NodeOptions{StorageDir: "/not-opened", Identity: topology.Identity{NodeName: "n1", ClusterID: "A"},
		Meta: MetaOptions{Network: MetaNetworkOptions{Resolver: unopenedRecoveryResolver{}, ClusterCredential: credential,
			ClientTLS: clientTLS, AdvertiseAddress: "127.0.0.1:1"}}}
	handle, err := NewRecovery(options)
	if err != nil {
		t.Fatal(err)
	}
	credential[0] = 'x'
	clientTLS.ServerName = "changed"
	if string(handle.probe.credential) != "credential" || handle.probe.clientTLS.ServerName != "peer" {
		t.Fatal("probe client settings were borrowed")
	}
	if handle.probe.resolver == nil || handle.probe.advertise != "127.0.0.1:1" {
		t.Fatal("missing probe context")
	}
}
