package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/examples/inmemory-cache/cache"
	"github.com/asalimonov/gquorum/examples/inmemory-cache/resp"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

const (
	maxClients     = 256
	commandTimeout = 3 * time.Second
)

// retry bounds read attempts; proposal retries use the full command deadline.
var retry = fault.RetryOptions{AttemptTimeout: time.Second, InitialBackoff: 20 * time.Millisecond,
	MaxBackoff: 200 * time.Millisecond}

var proposalRetry = load.ProposalRetryOptions{InitialBackoff: 20 * time.Millisecond, MaxBackoff: 200 * time.Millisecond}

const (
	setCommandArgs = 3
	requestIDBytes = 8
)

// redisServer serves a Redis subset. Writes are proposed to the cache group
// (followers forward to the leader); reads use Group.Read, a quorum-confirmed
// read on the local replica, so any node returns the latest acknowledged write.
type redisServer struct {
	group   func() (*load.Group, error)
	logger  *slog.Logger
	clients chan struct{}
	conns   sync.WaitGroup
	done    chan struct{}
}

func newRedisServer(group func() (*load.Group, error), logger *slog.Logger) *redisServer {
	return &redisServer{group: group, logger: logger,
		clients: make(chan struct{}, maxClients), done: make(chan struct{})}
}

// Serve accepts connections until ctx ends, then closes them and joins their
// handlers. Wait observes that join.
func (s *redisServer) Serve(ctx context.Context, listener net.Listener) error {
	defer close(s.done)
	defer s.conns.Wait()
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case s.clients <- struct{}{}:
		default:
			s.logger.Warn("client_rejected", "remote", conn.RemoteAddr().String(), "limit", maxClients)
			_, _ = io.WriteString(conn, "-ERR max number of clients reached\r\n")
			_ = conn.Close()
			continue
		}
		s.conns.Go(func() {
			defer func() { <-s.clients }()
			s.serveConn(ctx, conn)
		})
	}
}

// Wait joins Serve and every connection handler.
func (s *redisServer) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *redisServer) serveConn(ctx context.Context, conn net.Conn) {
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	defer conn.Close()
	r, w := bufio.NewReader(conn), resp.NewWriter(conn)
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				w.Error("ERR protocol error")
				_ = w.Flush()
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		call, cancel := context.WithTimeout(ctx, commandTimeout)
		quit := s.execute(call, args, w)
		cancel()
		flushErr := w.Flush()
		if flushErr != nil || quit {
			return
		}
	}
}

// execute runs one command and writes exactly one reply.
func (s *redisServer) execute(ctx context.Context, args []string, w resp.Writer) (quit bool) {
	name := strings.ToUpper(args[0])
	switch name {
	case "QUIT":
		w.Simple("OK")
		return true
	case "COMMAND": // redis-cli asks for command docs on connect
		w.Array(0)
		return false
	case "ECHO":
		if len(args) != 2 {
			wrongArgs(w, name)
		} else {
			w.Bulk(args[1])
		}
		return false
	}
	group, err := s.group()
	if err != nil {
		w.Error("LOADING cache replica is not ready")
		return false
	}
	switch {
	case name == "PING" && len(args) <= 2:
		replyPing(args, w)
	case name == "SET" && len(args) == setCommandArgs:
		request := writeRequest{command: cache.Command{Op: "set", Key: args[1], Value: args[2]}}
		if !write(ctx, group, w, request) {
			return false
		}
		w.Simple("OK")
	case name == "DEL" && len(args) >= 2:
		var count cache.Count
		request := writeRequest{command: cache.Command{Op: "del", Keys: args[1:]}, result: &count}
		if write(ctx, group, w, request) {
			w.Int(int64(count.N))
		}
	case name == "GET" && len(args) == 2:
		replyGet(ctx, group, args[1], w)
	case name == "EXISTS" && len(args) >= 2:
		count(ctx, group, w, cache.Query{Op: "exists", Keys: args[1:]})
	case name == "DBSIZE" && len(args) == 1:
		count(ctx, group, w, cache.Query{Op: "dbsize"})
	case name == "INFO":
		replyInfo(group, w)
	case slices.Contains([]string{"PING", "SET", "DEL", "GET", "EXISTS", "DBSIZE"}, name):
		wrongArgs(w, name)
	default:
		w.Error(fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
	return false
}

type writeRequest struct {
	command cache.Command
	result  any
}

// write submits through the library's safe proposal retry helper.
func write(ctx context.Context, group *load.Group, w resp.Writer, request writeRequest) bool {
	cmd, out := request.command, request.result
	if cmd.Op == "set" && (cmd.Key == "" || len(cmd.Key) > cache.MaxKeyBytes || len(cmd.Value) > cache.MaxValueBytes) {
		w.Error("ERR key or value size out of range")
		return false
	}
	data, _ := json.Marshal(cmd)
	id := make([]byte, requestIDBytes)
	_, _ = rand.Read(id)
	result, err := load.ProposeRetry(ctx, group.Propose, load.Proposal{RequestID: cmd.Op + "/" + hex.EncodeToString(
		id), Data: data}, proposalRetry)
	if err != nil {
		w.Error("ERR " + err.Error())
		return false
	}
	if result.Result.Code != "ok" {
		w.Error("ERR rejected: " + result.Result.Code)
		return false
	}
	if out != nil {
		_ = json.Unmarshal(result.Result.Data, out)
	}
	return true
}

func read(ctx context.Context, group *load.Group, w resp.Writer, q cache.Query) (load.Result, bool) {
	data, _ := json.Marshal(q)
	out, err := fault.Retry(ctx, retry, func(call context.Context) (load.Result, error) {
		return group.Read(call, data)
	})
	if err != nil {
		replyError(w, err)
		return out, false
	}
	return out, true
}

func count(ctx context.Context, group *load.Group, w resp.Writer, q cache.Query) {
	out, ok := read(ctx, group, w, q)
	if !ok {
		return
	}
	var count cache.Count
	_ = json.Unmarshal(out.Data, &count)
	w.Int(int64(count.N))
}

// replyError maps library failures to Redis-style errors. TRYAGAIN means no
// quorum or leader was reachable within the command deadline.
func replyError(w resp.Writer, err error) {
	if fault.Retryable(err) || errors.Is(err, context.DeadlineExceeded) {
		w.Error("TRYAGAIN " + err.Error())
		return
	}
	w.Error("ERR " + err.Error())
}

func wrongArgs(w resp.Writer, name string) {
	w.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)))
}

func replyPing(args []string, w resp.Writer) {
	if len(args) == 2 {
		w.Bulk(args[1])
	} else {
		w.Simple("PONG")
	}
}

func replyGet(ctx context.Context, group *load.Group, key string, w resp.Writer) {
	out, ok := read(ctx, group, w, cache.Query{Op: "get", Key: key})
	if ok && out.Code == "nil" {
		w.Nil()
	} else if ok {
		w.Bulk(string(out.Data))
	}
}

func replyInfo(group *load.Group, w resp.Writer) {
	status := group.Status()
	role := "slave"
	if group.IsLeader() {
		role = "master"
	}
	w.Bulk(fmt.Sprintf(
		"# Replication\r\nrole:%s\r\nleader_replica_id:%d\r\n"+
			"replica_id:%d\r\nterm:%d\r\napplied_index:%d\r\n",
		role, status.LeaderID, group.ReplicaID(), status.Term, status.Applied))
}
