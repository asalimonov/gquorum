//go:build !functional

package watchstate

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestLatestSeedsCoalescesClonesAndFinishes(t *testing.T) {
	t.Parallel()
	clone := func(v []int) []int { return append([]int(nil), v...) }
	initial := []int{1}
	p := New(2, initial, clone)
	initial[0] = 99
	a, err := p.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Subscribe(t.Context()); !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal(err)
	}
	av := <-a
	av[0] = 8
	if bv := <-b; bv[0] != 1 {
		t.Fatal("subscriber shares a value", bv)
	}
	p.Publish([]int{2})
	p.Publish([]int{3})
	if v := <-a; v[0] != 3 {
		t.Fatal("did not coalesce", v)
	}
	p.Finish([]int{4})
	p.Publish([]int{5})
	for _, ch := range []<-chan []int{a, b} {
		v, open := <-ch
		if !open || v[0] != 4 {
			t.Fatal("missing final value", v, open)
		}
		if _, open := <-ch; open {
			t.Fatal("terminal channel open")
		}
	}
	if err := p.Join(t.Context()); err != nil {
		t.Fatal(err)
	}
	late, err := p.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if v := <-late; v[0] != 4 {
		t.Fatal("late watch missed final", v)
	}
	if _, open := <-late; open {
		t.Fatal("late watch open")
	}
	p.Finish([]int{6}) // idempotent
}

func TestLatestCancellationAndConcurrentFinish(t *testing.T) {
	t.Parallel()
	p := New(128, 0, func(v int) int { return v })
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		ctx, cancel := context.WithCancel(t.Context())
		ch, err := p.Subscribe(ctx)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			cancel()
			for range ch {
			}
		})
	}
	wg.Go(func() {
		for i := 1; i < 100; i++ {
			p.Publish(i)
		}
		p.Finish(100)
	})
	wg.Wait()
	if err := p.Join(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := p.Subscribe(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
