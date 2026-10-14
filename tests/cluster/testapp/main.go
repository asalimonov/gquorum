// The process-test fixture owns caller protocols, state machines and test
// controls around the public gquorum library.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/util"
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
	AddressFree                                          bool
	Limits                                               topology.Limits
	SessionTTL, TickInterval                             time.Duration
	WALWriteWait                                         time.Duration
	ProposalTimeout                                      time.Duration
	SnapshotThreshold                                    uint64
	Bootstrap                                            bool
	GateFounders                                         bool
	FoundingTimeout                                      time.Duration
	Scheduling                                           scheduling.Options
	AdmissionPinPhase                                    string
	CallerScheduler, RetryRemoval                        bool
	MetaFaultProxy                                       bool
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
	flag.BoolVar(&c.AddressFree, "address-free", false, "resolve all locations from the caller manifest without topology labels")
	flag.BoolVar(&c.Join, "join", false, "join through supplied peers instead of founding")
	flag.BoolVar(&c.Bootstrap, "bootstrap", true, "declare the demo groups after founding")
	flag.BoolVar(&c.GateFounders, "gate-founders", false, "caller startup gate: require every configured founder before serving")
	flag.DurationVar(&c.FoundingTimeout, "founding-timeout", 0, "caller founding wait deadline; zero waits until cancellation")
	flag.IntVar(&c.Limits.MaxNodes, "max-nodes", c.Limits.MaxNodes, "Limits.MaxNodes")
	flag.IntVar(&c.Limits.MaxReplicas, "max-replicas", c.Limits.MaxReplicas, "Limits.MaxReplicas")
	flag.DurationVar(&c.SessionTTL, "session-ttl", 3*time.Second, "meta activation session TTL")
	flag.DurationVar(&c.TickInterval, "tick", 50*time.Millisecond, "Raft tick interval")
	flag.DurationVar(&c.WALWriteWait, "wal-write-wait", 0, "load WAL coalescing wait")
	flag.DurationVar(&c.ProposalTimeout, "proposal-timeout", 0, "load proposal result timeout; zero selects the library default")
	flag.Uint64Var(&c.SnapshotThreshold, "snapshot-threshold", 64, "load snapshot interval in applied entries")
	flag.BoolVar(&c.Scheduling.Disabled, "disable-reconciliation", false, "disable automatic load replica repair")
	flag.DurationVar(&c.Scheduling.Interval, "reconciliation-interval", 0, "automatic repair observation interval; zero selects the library default")
	flag.DurationVar(&c.Scheduling.Timeout, "reconciliation-timeout", 0, "automatic repair observation timeout; zero selects the library default")
	flag.BoolVar(&c.CallerScheduler, "caller-scheduler", false, "fixture: node-specific caller scheduler")
	flag.BoolVar(&c.RetryRemoval, "retry-removal", false, "fixture: retry removal once and retain diagnostics")
	flag.BoolVar(&c.MetaFaultProxy, "meta-fault-proxy", false, "fixture: proxy metadata traffic through authenticated fault barriers")
	flag.StringVar(&c.AdmissionPinPhase, "admission-pin-phase", "", "fixture: stop joining after pending or ready identity pin")
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
	c.InstanceID = util.NewInstanceID()
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
	slog.Info("fixture process identity", "node", c.NodeName, "instance_id", c.InstanceID, "meta_address", app.cfg.MetaAddress)
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
