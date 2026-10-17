// The cache server replicates an in-memory, size-bounded key/value cache across
// a gquorum load group and serves it over a subset of the Redis protocol.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
)

const (
	workerErrorCapacity = 4
	healthcheckArgCount = 3
)

// config is read from flags or GQUORUM_* environment variables. Every node
// lists the same peers; member IDs are derived from the names.
type config struct {
	Node         string   `env:"GQUORUM_NODE" required:"true" help:"Local node name; must appear in --peers."`
	Cluster      string   `env:"GQUORUM_CLUSTER" default:"cache-example"`
	Peers        []string `env:"GQUORUM_PEERS" required:"true" sep:"," help:"${peers}"`
	MetaPort     int      `env:"GQUORUM_META_PORT" default:"9000"`
	LoadPort     int      `env:"GQUORUM_LOAD_PORT" default:"9001"`
	HealthListen string   `env:"CACHE_HEALTH_LISTEN" default:":8080"`
	RedisListen  string   `env:"CACHE_REDIS_LISTEN" default:":6379"`
	Data         string   `env:"GQUORUM_DATA" default:"/data" help:"${data}"`

	DisableFSync bool `name:"storage-disable-fsync" env:"STORAGE_DISABLE_FSYNC" default:"false" help:"${fsync}"`

	CredentialFile string `env:"GQUORUM_CREDENTIAL_FILE" required:"true" type:"path"`
	AllowPlaintext bool   `env:"GQUORUM_ALLOW_PLAINTEXT" help:"${plaintext}"`
}

func (c config) Validate() error {
	if !c.AllowPlaintext {
		return errors.New("this demo requires explicit --allow-plaintext on its private network")
	}
	if !slices.Contains(c.Peers, c.Node) {
		return errors.New("local node missing from peers")
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
	r := &runtime{cfg: cfg, logger: logger, errors: make(chan error, workerErrorCapacity)}
	err := r.Start(ctx)
	if err == nil {
		select {
		case <-ctx.Done():
		case err = <-r.errors:
		}
	}
	// A Stop timeout only ends one wait; keep joining until ownership ends.
	for {
		stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		shutdownErr := r.Stop(stop)
		cancel()
		if shutdownErr == nil {
			return err
		}
		logger.Error("shutdown_waiting", "error", shutdownErr)
	}
}

// healthcheck lets Compose probe readiness with the server binary itself:
// PING answers PONG only once the local cache replica is ready.
func healthcheck(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := resp.Dial(ctx, addr)
	if err != nil {
		return err
	}
	defer client.Close()
	reply, err := client.Do(2*time.Second, "PING")
	if err != nil {
		return err
	}
	if reply.Str != "PONG" {
		return fmt.Errorf("not ready: %s", reply.Str)
	}
	return nil
}

func main() {
	if len(os.Args) == healthcheckArgCount && os.Args[1] == "healthcheck" {
		if err := healthcheck(os.Args[2]); err != nil {
			os.Exit(1)
		}
		return
	}
	var cfg config
	parser, err := kong.New(&cfg, cliHelp(), kong.Name("cache-server"),
		kong.Description("A replicated in-memory cache speaking a Redis subset."))
	if err == nil {
		_, err = parser.Parse(os.Args[1:])
	}
	if err == nil {
		err = cfg.Validate()
	}
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
		"data":      "Library Raft and topology state; the cache itself is in memory.",
		"fsync": "Disable load WAL/applied-state fsync; " +
			"host crashes may lose acknowledged writes or corrupt state.",
	}
}
