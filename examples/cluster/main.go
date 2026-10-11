// The example runs three _meta voters with a small topology REST API.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type peer struct {
	Name        string            `json:"name"`
	Type        topology.NodeType `json:"type"`
	MemberID    uint64            `json:"member_id"`
	MetaAddress string            `json:"meta_address"`
}

type addresses map[string]string

func (a addresses) Resolve(ctx context.Context, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if address := a[name]; address != "" {
		return address, nil
	}
	return "", fault.ErrIdentityMismatch
}
func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func run(ctx context.Context) (result error) {
	var peers []peer
	if err := json.Unmarshal([]byte(os.Getenv("GQUORUM_PEERS")), &peers); err != nil {
		return fmt.Errorf("peer manifest: %w", err)
	}
	name := os.Getenv("GQUORUM_NODE")
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	identity := topology.Identity{NodeName: name, ClusterID: envOr("GQUORUM_CLUSTER", "example"), InstanceID: hex.EncodeToString(nonce[:])}
	var founders []gquorum.FoundingMember
	var local topology.Node
	resolve := addresses{}
	for _, p := range peers {
		node := topology.Node{Name: p.Name, Type: p.Type, State: topology.NodeActive}
		founders = append(founders, gquorum.FoundingMember{Node: node, MemberID: p.MemberID, Address: p.MetaAddress})
		resolve[p.Name] = p.MetaAddress
		if p.Name == name {
			local = node
			identity.MemberID = p.MemberID
		}
	}
	metaListener, err := net.Listen("tcp", envOr("GQUORUM_META_LISTEN", ":9000"))
	if err != nil {
		return err
	}
	defer metaListener.Close()
	node, err := gquorum.NewNode(gquorum.NodeOptions{
		Identity:  identity,
		Node:      local,
		Bootstrap: gquorum.BootstrapOptions{Founders: founders},
		Meta: gquorum.MetaOptions{
			DBPath: filepath.Join(envOr("GQUORUM_DATA", "/data"), "meta.bolt"),
			Network: gquorum.MetaNetworkOptions{
				Listener: metaListener, AdvertiseAddress: resolve[name], Resolver: resolve,
				AllowPlaintext: true, ClusterCredential: []byte(os.Getenv("GQUORUM_CREDENTIAL")),
			},
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := node.Stop(shutdown); err != nil {
			result = errors.Join(result, err)
		}
	}()
	if err := node.Start(ctx); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", envOr("GQUORUM_HTTP_LISTEN", ":8080"))
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: routes(node), ReadHeaderTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	slog.Info("example listening", "node", name, "http", listener.Addr())
	select {
	case <-ctx.Done():
	case err := <-serveDone:
		if err != http.ErrServerClosed {
			return err
		}
		return nil
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		<-serveDone
		return err
	}
	<-serveDone
	return nil
}
func main() {
	healthcheck := flag.String("healthcheck", "", "probe an HTTP readiness endpoint and exit")
	flag.Parse()
	if *healthcheck != "" {
		client := http.Client{Timeout: 2 * time.Second}
		response, err := client.Get(*healthcheck)
		if err != nil {
			os.Exit(1)
		}
		_ = response.Body.Close()
		if response.StatusCode != 200 {
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("example", "error", err)
		os.Exit(1)
	}
}
