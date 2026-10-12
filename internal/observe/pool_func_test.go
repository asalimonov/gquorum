//go:build functional

package observe

import (
	"context"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"testing"
	"time"
)

func TestReadPoolCompletedCallsReleaseExactSourceBeforeReturning(t *testing.T) {
	t.Parallel()
	p, err := NewReadPool(2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := p.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	for range 1000 {
		if err := p.DoKey(t.Context(), "same-source", func(context.Context) error { return nil }); err != nil {
			t.Fatal("completed callback still excluded its next read", err)
		}
	}
}

func TestReadPoolOwnsCanceledCallbacksAndPreventsOverlap(t *testing.T) {
	t.Parallel()
	p, err := NewReadPool(2)
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer close(release)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := p.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	go func() {
		done <- p.DoKey(ctx, "exact", func(context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := p.DoKey(t.Context(), "exact", func(context.Context) error { t.Error("overlapped canceled source"); return nil }); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
	if err := p.DoKey(t.Context(), "independent", func(context.Context) error { return nil }); err != nil {
		t.Fatal("other slot blocked", err)
	}
	stop, cancelStop := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancelStop()
	if err := p.Stop(stop); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("forgot outstanding owner", err)
	}
	if err := p.Do(t.Context(), func(context.Context) error { t.Error("called after stop"); return nil }); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}

func TestReadPoolCancellationBeforeDispatchAndStop(t *testing.T) {
	t.Parallel()
	p, err := NewReadPool(1)
	if err != nil {
		t.Fatal(err)
	}
	entered, done := make(chan struct{}), make(chan error, 1)
	go func() {
		done <- p.Do(t.Context(), func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() })
	}()
	<-entered
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := p.Do(ctx, func(context.Context) error { t.Error("dispatched canceled read"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stop, cancelStop := context.WithTimeout(t.Context(), time.Second)
	defer cancelStop()
	if err := p.Stop(stop); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
