// Package loadsnapshot implements the Node-owned load snapshot transfer
// mechanism behind the public snapshot integration contracts.
package loadsnapshot

import (
	"context"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load/snapshot"
)

// Resolver returns the current managed coordinator without acquiring resources.
// Callers retain the exact coordinator returned for an operation; they must not
// redirect its session into a successor after a wait.
type Resolver func() (*Coordinator, error)

type service struct {
	options snapshot.Options
	resolve Resolver
	mu      sync.Mutex
	last    *Coordinator // observation-only fallback, never work admission
}

// NewService copies already-resolved policy and stores the internal resolver.
// It does not call the resolver, perform I/O, acquire resources or start workers.
func NewService(options snapshot.Options, resolve Resolver) snapshot.Service {
	return &service{options: options, resolve: resolve}
}

func (s *service) Options() snapshot.Options { return s.options }

func (s *service) Begin(ctx context.Context, req snapshot.BeginRequest) (snapshot.Ack, error) {
	c, err := s.forWork(ctx)
	if err != nil {
		return snapshot.Ack{}, err
	}
	return c.BeginSnapshot(ctx, req)
}

func (s *service) Write(ctx context.Context, chunk snapshot.Chunk) (snapshot.Ack, error) {
	c, err := s.forWork(ctx)
	if err != nil {
		return snapshot.Ack{}, err
	}
	return c.WriteSnapshot(ctx, chunk)
}

func (s *service) Finish(ctx context.Context, session snapshot.Session) (snapshot.Ack, error) {
	c, err := s.forWork(ctx)
	if err != nil {
		return snapshot.Ack{}, err
	}
	return c.FinishSnapshot(ctx, session)
}

func (s *service) Detach(ctx context.Context, session snapshot.Session) error {
	c, err := s.forWork(ctx)
	if err != nil {
		return err
	}
	return c.DetachSnapshot(ctx, session)
}

func (s *service) Abort(ctx context.Context, session snapshot.Session) error {
	c, err := s.forWork(ctx)
	if err != nil {
		return err
	}
	return c.AbortSnapshot(ctx, session)
}

func (s *service) SessionDone(session snapshot.Session) <-chan struct{} {
	c, err := s.current()
	if err != nil {
		return closedSessionSignal
	}
	return c.SessionDone(session)
}

func (s *service) List(query snapshot.Query) snapshot.Page {
	c := s.forObservation()
	if c == nil {
		return snapshot.Page{}
	}
	return c.List(query)
}

func (s *service) forWork(ctx context.Context) (*Coordinator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.current()
}

func (s *service) current() (*Coordinator, error) {
	if s.resolve == nil {
		return nil, fault.ErrReplicaNotReady
	}
	// The resolver can take the Node's lock; never call it under our lock.
	c, err := s.resolve()
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, fault.ErrReplicaNotReady
	}
	s.mu.Lock()
	s.last = c
	s.mu.Unlock()
	return c, nil
}

func (s *service) forObservation() *Coordinator {
	c, err := s.current()
	if err == nil {
		return c
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

var closedSessionSignal = func() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}()

// RememberCoordinator retains observation-only access even if no incoming RPC
// looked up this source-only coordinator before Node shutdown. It grants no work.
func RememberCoordinator(view snapshot.Service, c *Coordinator) {
	if s, ok := view.(*service); ok {
		s.mu.Lock()
		s.last = c
		s.mu.Unlock()
	}
}
