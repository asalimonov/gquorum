//go:build !functional

package main

import (
	"io"
	"testing"
	"time"

	"github.com/alecthomas/kong"
)

func parseCLI(t *testing.T, argv ...string) (cli, error) {
	t.Helper()
	var args cli
	parser, err := kong.New(&args, kong.Writers(io.Discard, io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	_, err = parser.Parse(argv)
	return args, err
}

func TestCLIParsesManifestAndDefaultServeCommand(t *testing.T) {
	t.Parallel()
	manifest := `[{"name":"node1","type":"worker","member_id":1,"meta_address":"node1:9000"},{"name":"node2","type":"observer","member_id":2,"meta_address":"node2:9000"}]`
	flags := []string{"--node", "node1", "--credential", "example", "--peers", manifest, "--cluster", "example", "--meta-listen", ":9000", "--http-listen", ":8080"}
	for _, prefix := range [][]string{nil, {"serve"}} {
		args, err := parseCLI(t, append(prefix, flags...)...)
		if err != nil {
			t.Fatal(err)
		}
		cfg := args.Serve.Config
		if cfg.Node != "node1" || cfg.Cluster != "example" || cfg.HTTPListen != ":8080" || cfg.MetaListen != ":9000" || len(cfg.Peers.Members) != 2 {
			t.Fatalf("unexpected configuration: node=%q cluster=%q http=%q meta=%q peers=%d", cfg.Node, cfg.Cluster, cfg.HTTPListen, cfg.MetaListen, len(cfg.Peers.Members))
		}
		if cfg.Peers.Members[1].Name != "node2" || cfg.Peers.Members[1].Type != "observer" || cfg.Peers.Members[1].MemberID != 2 {
			t.Fatal(cfg.Peers.Members[1])
		}
	}
}

func TestCLIRejectsInvalidPeerJSON(t *testing.T) {
	t.Parallel()
	for _, manifest := range []string{`{}`, `[{"name":`, `[{"member_id":"one"}]`} {
		if _, err := parseCLI(t, "serve", "--node", "node1", "--credential", "example", "--peers", manifest); err == nil {
			t.Fatalf("accepted invalid manifest %q", manifest)
		}
	}
}

func TestCLIHealthcheckNeedsNoClusterConfiguration(t *testing.T) {
	t.Parallel()
	args, err := parseCLI(t, "healthcheck", "http://localhost:8080/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	if args.Healthcheck.URL != "http://localhost:8080/health/ready" || args.Healthcheck.Timeout != 2*time.Second {
		t.Fatal(args.Healthcheck)
	}
}
