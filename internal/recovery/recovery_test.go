//go:build !functional

package recovery

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/asalimonov/gquorum/recovery"

	"github.com/asalimonov/gquorum/internal/recovery/model"
)

type unopenedRecoveryResolver struct{}

func (unopenedRecoveryResolver) Resolve(context.Context, string) (string, error) {
	panic("constructor resolved a peer")
}
func TestRecoveryCopiesProbeClientConfigurationWithoutAcquisition(t *testing.T) {
	t.Parallel()
	credential := []byte("credential")
	clientTLS := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "peer"}
	handle, err := New(Config{Source: model.SourceOptions{Paths: model.Paths{StorageDir: "/not-opened"}, NodeName: "n1", ClusterID: "A"}, Probe: ProbeOptions{Resolver: unopenedRecoveryResolver{}, Credential: credential, ClientTLS: clientTLS, AdvertiseAddress: "127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	credential[0] = 'x'
	clientTLS.ServerName = "changed"
	if string(handle.probe.Credential) != "credential" || handle.probe.ClientTLS.ServerName != "peer" {
		t.Fatal("probe client settings were borrowed")
	}
	if handle.probe.Resolver == nil || handle.probe.AdvertiseAddress != "127.0.0.1:1" {
		t.Fatal("missing probe context")
	}
}

func TestRecoveryCopiesSourceAndKeepsZeroOverrides(t *testing.T) {
	t.Parallel()
	config := Config{Source: model.SourceOptions{Paths: model.Paths{StorageDir: "/not-opened", LoadWALDir: "/not-opened/load"}, NodeName: "n1"}, WALConfigured: true}
	options := recovery.Options{}
	handle, err := New(config, options)
	if err != nil {
		t.Fatal(err)
	}
	config.Source.Paths.LoadWALDir = "changed"
	config.Source.NodeName = "changed"
	options.Resources.MaxGroupBytes = 42
	if handle.source.Paths.LoadWALDir != "/not-opened/load" || handle.source.NodeName != "n1" || handle.options.Resources != (recovery.Resources{}) {
		t.Fatal("copied source or default selection changed", handle.source, handle.options)
	}
}
