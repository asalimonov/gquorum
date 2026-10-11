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
	node, listener, err := newNode(cfg)
	if err != nil {
		return err
	}
	// Close the caller-created listener after Node has joined, including when
	// startup fails before the library acquires it.
	defer listener.Close()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		result = errors.Join(result, node.Stop(shutdown))
	}()
	if err := node.Start(ctx); err != nil {
		return err
	}
	return serveHTTP(ctx, cfg.HTTPListen, routes(node))
}

func main() {
	var args cli
	ctx := kong.Parse(&args, kong.Name("cluster"), kong.Description("A small gquorum topology example."))
	ctx.FatalIfErrorf(ctx.Run())
}
