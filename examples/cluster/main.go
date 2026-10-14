// The example runs three _meta voters with a small topology REST API.
package main

import (
	"context"
	"errors"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
)

type cli struct {
	Serve       serveCommand       `cmd:"" default:"withargs" help:"Run a cluster participant."`
	Healthcheck healthcheckCommand `cmd:"" help:"Probe an HTTP readiness endpoint and exit."`
}

type serveCommand struct {
	Config config `embed:""`
}

func (cmd *serveCommand) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return run(ctx, cmd.Config)
}

func run(ctx context.Context, cfg config) (result error) {
	// Construction validates settings; Start below acquires durable stores and
	// starts consensus. Canceling ctx begins shutdown for the whole
	// participant.
	resources, err := newNode(cfg)
	if err != nil {
		return err
	}
	node, listener := resources.node, resources.listener
	// Close the caller-created listener after Node has joined, including when
	// startup fails before the library acquires it.
	defer listener.Close()
	// Stop must join Raft and callbacks even after the process context is
	// canceled. Defers run in reverse order, so Node joins before the listener
	// is released.
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		result = errors.Join(result, node.Stop(shutdown))
	}()
	if err := node.Start(ctx); err != nil {
		return err
	}
	// Publish HTTP immediately; /health/ready stays unavailable until Node is
	// ready.
	return serveHTTP(ctx, cfg.HTTPListen, routes(node))
}

func main() {
	var args cli
	ctx := kong.Parse(&args, cliHelp(), kong.Name("cluster"), kong.Description("A small gquorum topology example."))
	ctx.FatalIfErrorf(ctx.Run())
}
