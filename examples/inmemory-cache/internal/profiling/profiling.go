// Package profiling provides an opt-in diagnostic listener for the example.
package profiling

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// Listen serves standard Go profiles on address. An empty address disables
// profiling. The caller owns shutdown; stop joins requests and the listener.
func Listen(address string) (stop func(), err error) {
	if address == "" {
		return func() {}, nil
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	ctx, cancel := context.WithCancel(context.Background())
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(l)
	}()
	return func() {
		cancel() // End active CPU/trace profiles before joining their handlers.
		_ = server.Shutdown(context.Background())
		<-done
	}, nil
}
