// The scheduler example runs three managed replicas with quorum-backed task
// state.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/topology"
)

const (
	maxNetworkPort      = 65535
	healthcheckArgCount = 3
)

// config is read from flags or GQUORUM_* environment variables. Every node
// lists the same peers; member IDs are derived from the names.
type config struct {
	Node           string   `env:"GQUORUM_NODE" required:"true" help:"Local node name; must appear in --peers."`
	Cluster        string   `env:"GQUORUM_CLUSTER" default:"scheduler-example"`
	Peers          []string `env:"GQUORUM_PEERS" required:"true" sep:"," help:"${peers}"`
	MetaPort       int      `env:"GQUORUM_META_PORT" default:"9000"`
	LoadPort       int      `env:"GQUORUM_LOAD_PORT" default:"9001"`
	HTTPListen     string   `env:"GQUORUM_HTTP_LISTEN" default:":8080"`
	Data           string   `env:"GQUORUM_DATA" default:"/data"`
	CredentialFile string   `env:"GQUORUM_CREDENTIAL_FILE" required:"true" type:"path"`
	AllowPlaintext bool     `env:"GQUORUM_ALLOW_PLAINTEXT" help:"${plaintext}"`
}

func readConfig(args []string) (config, error) {
	var c config
	parser, err := kong.New(&c, cliHelp(), kong.Name("scheduler"),
		kong.Description("A durable three-node task scheduler."))
	if err != nil {
		return config{}, err
	}
	if _, err := parser.Parse(args); err != nil {
		return config{}, err
	}
	slices.Sort(c.Peers)
	return c, c.Validate()
}

func (c config) Validate() error {
	if !c.AllowPlaintext {
		return errors.New("this demo requires explicit --allow-plaintext on its private network")
	}
	if c.Cluster == "" || c.Data == "" || c.CredentialFile == "" || len(c.Peers) == 0 {
		return errors.New("cluster, data, credential and peers are required")
	}
	for i, name := range c.Peers {
		if err := topology.ValidateID(name); err != nil {
			return err
		}
		if i > 0 && c.Peers[i-1] == name {
			return fmt.Errorf("duplicate peer %q", name)
		}
	}
	if !slices.Contains(c.Peers, c.Node) {
		return errors.New("local node missing from peers")
	}
	if c.MetaPort < 1 || c.MetaPort > maxNetworkPort || c.LoadPort < 1 ||
		c.LoadPort > maxNetworkPort || c.MetaPort == c.LoadPort {
		return errors.New("meta and load ports must be distinct and in 1..65535")
	}
	if _, port, err := net.SplitHostPort(c.HTTPListen); err != nil || port == "" {
		return errors.New("invalid HTTP listener address")
	}
	return nil
}

// peers routes each node by name: Compose service names are host names.
func (c config) peers() gquorum.StaticPeers {
	peers := gquorum.StaticPeers{}
	for _, name := range c.Peers {
		peers[name] = gquorum.StaticPeer{
			MetaAddress: net.JoinHostPort(name, strconv.Itoa(c.MetaPort)),
			LoadAddress: net.JoinHostPort(name, strconv.Itoa(c.LoadPort)),
		}
	}
	return peers
}

func run(ctx context.Context, cfg config) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	runtime, err := newRuntime(cfg, logger)
	if err != nil {
		return err
	}
	err = runtime.Start(ctx)
	if err == nil {
		select {
		case <-ctx.Done():
		case err = <-runtime.errors:
		}
	}
	// A short Stop timeout is a wait timeout, not permission to close borrowed
	// resources underneath a retained owner. Keep joining until ownership ends.
	for {
		stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		shutdownErr := runtime.Stop(stop)
		cancel()
		if shutdownErr == nil {
			break
		}
		logger.Error("shutdown_waiting", "error", shutdownErr)
	}
	return err
}
func main() {
	// Compose health checks reuse the example binary; no shell or curl
	// required.
	if len(os.Args) == healthcheckArgCount && os.Args[1] == "healthcheck" {
		client := http.Client{Timeout: 2 * time.Second}
		//nolint:gosec // The demo operator supplies the coordinator URL.
		response, err := client.Get(os.Args[2])
		if err != nil {
			os.Exit(1)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}
	cfg, err := readConfig(os.Args[1:])
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func cliHelp() kong.Vars {
	return kong.Vars{
		"plaintext": "Explicitly allow the isolated Compose plaintext network.",
		"peers":     "Comma-separated node names; each is also its host name.",
	}
}
