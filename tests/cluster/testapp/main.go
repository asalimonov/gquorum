// The process-test fixture owns caller protocols, state machines and test
// controls around the public gquorum library.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/topology"
)

type peer struct {
	Name        string            `json:"name"`
	Type        topology.NodeType `json:"type"`
	MemberID    uint64            `json:"member_id"`
	MetaAddress string            `json:"meta_address"`
	LoadAddress string            `json:"load_address"`
	HTTPAddress string            `json:"http_address"`
}
type config struct {
	NodeName, ClusterID, InstanceID, DataDir, Credential string
	NodeType                                             topology.NodeType
	MetaListen, LoadListen, HTTPListen                   string
	MetaAddress, LoadAddress, HTTPAddress                string
	Peers                                                []peer
	Join                                                 bool
	Limits                                               topology.Limits
	SessionTTL, TickInterval                             time.Duration
	WALWriteWait                                         time.Duration
	SnapshotThreshold                                    uint64
	Bootstrap                                            bool
	Reconciliation                                       meta.ReconciliationOptions
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
func main() {
	c := config{Limits: (topology.Limits{}).WithDefaults()}
	var manifest, nodeType, healthcheck string
	flag.StringVar(&healthcheck, "healthcheck", "", "probe an HTTP readiness endpoint and exit")
	flag.StringVar(&c.NodeName, "node", envOr("GQUORUM_NODE", ""), "local node name")
	flag.StringVar(&nodeType, "type", envOr("GQUORUM_TYPE", "worker"), "caller-defined node type")
	flag.StringVar(&c.ClusterID, "cluster", envOr("GQUORUM_CLUSTER", "example"), "cluster identity")
	flag.StringVar(&c.DataDir, "data", envOr("GQUORUM_DATA", ""), "explicit local data directory")
	flag.StringVar(&c.Credential, "credential", envOr("GQUORUM_CREDENTIAL", ""), "shared example protocol credential")
	flag.StringVar(&manifest, "peers", envOr("GQUORUM_PEERS", ""), "JSON founding peer manifest")
	flag.StringVar(&c.MetaListen, "meta-listen", ":9000", "meta listener")
	flag.StringVar(&c.LoadListen, "load-listen", ":9001", "application load listener")
	flag.StringVar(&c.HTTPListen, "http-listen", ":8080", "HTTP listener")
	flag.StringVar(&c.MetaAddress, "meta-address", os.Getenv("GQUORUM_META_ADDRESS"), "advertised meta address; defaults to local manifest entry")
	flag.StringVar(&c.LoadAddress, "load-address", os.Getenv("GQUORUM_LOAD_ADDRESS"), "advertised load address; defaults to local manifest entry")
	flag.StringVar(&c.HTTPAddress, "http-address", os.Getenv("GQUORUM_HTTP_ADDRESS"), "advertised HTTP address; defaults to local manifest entry")
	flag.BoolVar(&c.Join, "join", false, "join through supplied peers instead of founding")
	flag.BoolVar(&c.Bootstrap, "bootstrap", true, "declare the demo groups after founding")
	flag.IntVar(&c.Limits.MaxNodes, "max-nodes", c.Limits.MaxNodes, "Limits.MaxNodes")
	flag.IntVar(&c.Limits.MaxReplicas, "max-replicas", c.Limits.MaxReplicas, "Limits.MaxReplicas")
	flag.DurationVar(&c.SessionTTL, "session-ttl", 3*time.Second, "meta activation session TTL")
	flag.DurationVar(&c.TickInterval, "tick", 50*time.Millisecond, "Raft tick interval")
	flag.DurationVar(&c.WALWriteWait, "wal-write-wait", 0, "load WAL coalescing wait")
	flag.Uint64Var(&c.SnapshotThreshold, "snapshot-threshold", 64, "load snapshot interval in applied entries")
	flag.BoolVar(&c.Reconciliation.Disabled, "disable-reconciliation", false, "disable automatic load replica repair")
	flag.DurationVar(&c.Reconciliation.Interval, "reconciliation-interval", 0, "automatic repair observation interval; zero selects the library default")
	flag.DurationVar(&c.Reconciliation.TickTimeout, "reconciliation-timeout", 0, "automatic repair observation timeout; zero selects the library default")
	flag.Parse()
	if healthcheck != "" {
		client := &http.Client{Timeout: 2 * time.Second}
		response, err := client.Get(healthcheck)
		if err != nil {
			os.Exit(1)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}
	c.NodeType = topology.NodeType(nodeType)
	if err := decodeJSON([]byte(manifest), &c.Peers); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	c.InstanceID = hex.EncodeToString(nonce[:])
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	app, err := newApplication(c, slog.Default(), nil)
	if err != nil {
		slog.Error("construct example", "error", err)
		os.Exit(1)
	}
	if err = app.Start(ctx); err != nil {
		_ = app.Close(context.Background())
		slog.Error("start example", "error", err)
		os.Exit(1)
	}
	slog.Info("example listening", "node", c.NodeName, "http", app.httpListener.Addr().String())
	select {
	case <-ctx.Done():
	case <-app.closed:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = app.Close(shutdown); err != nil {
		slog.Error("close example", "error", err)
		os.Exit(1)
	}
}
