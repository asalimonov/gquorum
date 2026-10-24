package main

import (
	"context"
	"net/http"
	"time"

	"github.com/asalimonov/gquorum/load"
)

// healthHandler checks replay readiness and confirms a fresh cache-group quorum
// through the existing read barrier, without proposing application commands.
// Its budget leaves time to reply within the loader's one-second probe timeout.
func healthHandler(group func() (*load.Group, error)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 800*time.Millisecond)
		defer cancel()
		g, err := group()
		if err == nil {
			err = g.WaitReady(ctx)
		}
		if err == nil {
			err = g.ReadBarrier(ctx)
		}
		if err != nil || !g.Running() {
			http.Error(w, "cache replica unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
