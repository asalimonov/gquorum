//go:build functional

package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
)

func TestRequestRetriesFreshConnectionsWithIdenticalCommand(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	commands := make(chan []string, 3)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for attempt := range 3 {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			args, _ := resp.ReadCommand(bufio.NewReader(conn))
			commands <- args
			if attempt == 1 {
				_, _ = io.WriteString(conn, "-LOADING replica starting\r\n")
			} else if attempt == 2 {
				_, _ = io.WriteString(conn, "+OK\r\n")
			}
			_ = conn.Close()
		}
	}()
	defer func() { cancel(); <-done }()
	want := []string{"SET", "key:0001", "key:0001|1|payload"}
	pool := newRequestPool([]string{listener.Addr().String()}, 1)
	defer pool.close()
	reply, err := pool.request(ctx, listener.Addr().String(), want...)
	if err != nil || reply.Str != "OK" {
		t.Fatalf("reply=%+v error=%v", reply, err)
	}
	for range 3 {
		if got := <-commands; !reflect.DeepEqual(got, want) {
			t.Fatalf("retry changed command: %v", got)
		}
	}
}

func TestRequestPoolReusesConnectionsBeyondEphemeralPortRange(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int64
	var handlers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			handlers.Go(func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					if _, err := resp.ReadCommand(reader); err != nil {
						return
					}
					if _, err := io.WriteString(conn, "+PONG\r\n"); err != nil {
						return
					}
				}
			})
		}
	}()
	pool := newRequestPool([]string{listener.Addr().String()}, 10)
	defer func() { pool.close(); _ = listener.Close(); <-done; handlers.Wait() }()
	var callers sync.WaitGroup
	for range 10 {
		callers.Go(func() {
			for range 4000 {
				reply, err := pool.request(ctx, listener.Addr().String(), "PING")
				if err != nil || reply.Str != "PONG" {
					t.Errorf("reply=%+v error=%v", reply, err)
					return
				}
			}
		})
	}
	callers.Wait()
	if got := accepted.Load(); got > 10 {
		t.Fatalf("40,000 commands opened %d connections; pool limit is 10", got)
	}
}

func TestRequestPoolCancellationDiscardsConnection(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pool := newRequestPool([]string{listener.Addr().String()}, 1)
	accepted := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = resp.ReadCommand(bufio.NewReader(conn))
		close(accepted)
		_, _ = io.Copy(io.Discard, conn)
	}()
	defer func() { cancel(); pool.close(); _ = listener.Close(); <-done }()
	result := make(chan error, 1)
	go func() { _, err := pool.request(ctx, listener.Addr().String(), "PING"); result <- err }()
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach server")
	}
	cancel()
	select {
	case err := <-result:
		if err != context.Canceled {
			t.Fatalf("canceled request returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt the borrowed connection")
	}
}

// request reuses a pooled connection. Failed attempts discard their connection
// before retrying the same endpoint. Balanced requests select a node separately.
func (p *requestPool) request(ctx context.Context, endpoint string, args ...string) (resp.Reply, error) {
	return retryRequest(ctx, func(call context.Context) (resp.Reply, error) {
		return p.exchange(call, endpoint, args)
	})
}
