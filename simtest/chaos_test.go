package simtest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/kudesn1k1/artel"
	"github.com/kudesn1k1/artel/transport"
)

// Contract tests for the chaos transport: a decorator over any Transport that
// applies the anomaly vocabulary at Send, deciding by its own seeded PRNG. The
// wire underneath is the in-process transport, so "delivered" means the peer's
// handler ran. The seed fixes the decisions, not the goroutine schedule, so
// the tests about decision sequences send from one goroutine only.
//
// Live engines run with their default send timeout of two seconds, and there
// is no bridge to shorten it from here: scenarios must keep DelayMax well
// below it, or every delayed push becomes a retained failure.

// probe is a bare participant on the wire: it serves a handler on the given
// transport and records every message that reaches it.
type probe struct {
	mu       sync.Mutex
	received []artel.Message
}

func serveProbe(t *testing.T, tr artel.Transport) *probe {
	t.Helper()
	p := &probe{}
	if err := tr.Serve(func(_ context.Context, m artel.Message) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.received = append(p.received, m)
		return nil
	}); err != nil {
		t.Fatalf("serve %s: %v", tr.ID(), err)
	}
	return p
}

func (p *probe) messages() []artel.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.received)
}

func (p *probe) count() int { return len(p.messages()) }

func pushFrom(from, payload string) artel.Message {
	return artel.Message{From: from, Kind: artel.KindPush, Payload: []byte(payload)}
}

// closeSpy records whether Close reached the inner transport.
type closeSpy struct {
	artel.Transport
	closed bool
}

func (c *closeSpy) Close() error {
	c.closed = true
	return c.Transport.Close()
}

func TestChaosIsATransparentWrapper(t *testing.T) {
	reg := transport.NewInProcessRegistry()
	inner := &closeSpy{Transport: transport.NewInProcess("a", []string{"b", "c"}, reg)}
	a := Chaos(inner, 1, ChaosConfig{})

	if got := a.ID(); got != "a" {
		t.Fatalf("ID() = %q, want the inner's %q", got, "a")
	}
	if got, want := a.Peers(), []string{"b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("Peers() = %v, want the inner's %v", got, want)
	}

	// Serve through the wrapper: the engine registers its handler on whatever
	// transport it was given, so a message to "a" must reach a handler served
	// via the decorator.
	served := serveProbe(t, a)
	b := transport.NewInProcess("b", []string{"a"}, reg)
	if err := b.Send(context.Background(), "a", pushFrom("b", "hello")); err != nil {
		t.Fatalf("send to the wrapped node: %v", err)
	}
	if got := served.messages(); len(got) != 1 || string(got[0].Payload) != "hello" {
		t.Fatalf("the handler served through the wrapper saw %+v, want the one push", got)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !inner.closed {
		t.Fatal("Close did not reach the inner transport")
	}
}

// One send under one certain anomaly. Delivered is how many times the peer's
// handler ran; err is what the sender was told. Dup with a lost ack pins the
// rule the simulator already keeps: two deliveries, one outcome.
func TestChaosFates(t *testing.T) {
	cases := []struct {
		name      string
		cfg       ChaosConfig
		delivered int
		err       bool
	}{
		{"no faults", ChaosConfig{}, 1, false},
		{"drop", ChaosConfig{DropP: 1}, 0, true},
		{"dup", ChaosConfig{DupP: 1}, 2, false},
		{"ack lost", ChaosConfig{AckLostP: 1}, 1, true},
		{"ack lie", ChaosConfig{AckLieP: 1}, 0, false},
		{"dup with a lost ack", ChaosConfig{DupP: 1, AckLostP: 1}, 2, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reg := transport.NewInProcessRegistry()
			b := serveProbe(t, transport.NewInProcess("b", nil, reg))
			a := Chaos(transport.NewInProcess("a", []string{"b"}, reg), 1, c.cfg)

			msg := pushFrom("a", "delta")
			err := a.Send(context.Background(), "b", msg)
			if (err != nil) != c.err {
				t.Fatalf("Send returned %v, want error: %v", err, c.err)
			}
			got := b.messages()
			if len(got) != c.delivered {
				t.Fatalf("delivered %d times, want %d: %+v", len(got), c.delivered, got)
			}
			for _, m := range got {
				if !reflect.DeepEqual(m, msg) {
					t.Fatalf("delivered %+v, want the message as sent %+v", m, msg)
				}
			}
		})
	}
}

func TestChaosDelayHoldsTheSend(t *testing.T) {
	const delay = 40 * time.Millisecond
	reg := transport.NewInProcessRegistry()
	b := serveProbe(t, transport.NewInProcess("b", nil, reg))
	a := Chaos(transport.NewInProcess("a", []string{"b"}, reg), 1, ChaosConfig{DelayMin: delay, DelayMax: delay})

	start := time.Now()
	if err := a.Send(context.Background(), "b", pushFrom("a", "delta")); err != nil {
		t.Fatalf("send: %v", err)
	}
	if elapsed := time.Since(start); elapsed < delay {
		t.Fatalf("Send returned after %v, want at least the delay of %v", elapsed, delay)
	}
	if b.count() != 1 {
		t.Fatalf("delivered %d times, want once", b.count())
	}
}

// A send whose context ends during the delay fails with the context's error
// and is never delivered — not late, not from a goroutine that outlived the
// call. The check waits past the whole delay before declaring the wire empty.
func TestChaosCancelledDelayNeverDelivers(t *testing.T) {
	const delay = 300 * time.Millisecond
	reg := transport.NewInProcessRegistry()
	b := serveProbe(t, transport.NewInProcess("b", nil, reg))
	a := Chaos(transport.NewInProcess("a", []string{"b"}, reg), 1, ChaosConfig{DelayMin: delay, DelayMax: delay})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := a.Send(ctx, "b", pushFrom("a", "delta"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send returned %v, want the context's deadline error", err)
	}
	if elapsed := time.Since(start); elapsed >= delay {
		t.Fatalf("Send returned after %v: it sat out the whole delay instead of the context", elapsed)
	}

	time.Sleep(delay + 100*time.Millisecond)
	if b.count() != 0 {
		t.Fatalf("delivered %d times after the context ended, want never", b.count())
	}
}

// Delays are per send, not a lock the whole node waits behind: senders that
// start together finish together.
func TestChaosDelaysRunConcurrently(t *testing.T) {
	const delay, senders = 200 * time.Millisecond, 4
	reg := transport.NewInProcessRegistry()
	b := serveProbe(t, transport.NewInProcess("b", nil, reg))
	a := Chaos(transport.NewInProcess("a", []string{"b"}, reg), 1, ChaosConfig{DelayMin: delay, DelayMax: delay})

	start := time.Now()
	var wg sync.WaitGroup
	for i := range senders {
		wg.Go(func() {
			if err := a.Send(context.Background(), "b", pushFrom("a", fmt.Sprint(i))); err != nil {
				t.Errorf("send %d: %v", i, err)
			}
		})
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed >= 2*delay {
		t.Fatalf("%d concurrent sends took %v, want about one delay of %v", senders, elapsed, delay)
	}
	if b.count() != senders {
		t.Fatalf("delivered %d times, want %d", b.count(), senders)
	}
}

// outcome is what one send came to: the copies that reached the peer and
// whether the sender was told of a failure.
type outcome struct {
	copies int
	failed bool
}

// decisions sends n pushes from one goroutine through a decorator seeded with
// seed and reports every outcome in order. Payloads are distinct so that the
// copies of each send can be counted.
func decisions(t *testing.T, seed uint64, cfg ChaosConfig, n int) []outcome {
	t.Helper()
	reg := transport.NewInProcessRegistry()
	b := serveProbe(t, transport.NewInProcess("b", nil, reg))
	a := Chaos(transport.NewInProcess("a", []string{"b"}, reg), seed, cfg)

	out := make([]outcome, n)
	for i := range n {
		err := a.Send(context.Background(), "b", pushFrom("a", fmt.Sprint(i)))
		out[i].failed = err != nil
	}
	for _, m := range b.messages() {
		var i int
		if _, err := fmt.Sscan(string(m.Payload), &i); err != nil || i < 0 || i >= n {
			t.Fatalf("delivered a message that was never sent: %+v", m)
		}
		out[i].copies++
	}
	return out
}

// The seed is the whole source of randomness: the same seed over the same
// sends yields the same outcomes, another seed yields other outcomes.
func TestChaosSeedFixesTheDecisions(t *testing.T) {
	cfg := ChaosConfig{DropP: 0.5, DupP: 0.5, AckLostP: 0.5}
	const n = 64
	first := decisions(t, 7, cfg, n)
	again := decisions(t, 7, cfg, n)
	if !slices.Equal(first, again) {
		t.Fatalf("seed 7 decided\n%v\nand then\n%v", first, again)
	}
	other := decisions(t, 8, cfg, n)
	if slices.Equal(first, other) {
		t.Fatalf("seeds 7 and 8 decided the same %d sends alike: the seed is not used", n)
	}
	for _, o := range first {
		if o.copies > 2 {
			t.Fatalf("a send reached the peer %d times, at most two copies are possible", o.copies)
		}
	}
}

// Send is called from several goroutines at once, as the engine does from its
// worker pool and from a handler answering a pull. The race detector is the
// assertion; the counts only bound what one send may produce.
func TestChaosSendFromManyGoroutines(t *testing.T) {
	const senders, each = 8, 50
	reg := transport.NewInProcessRegistry()
	b := serveProbe(t, transport.NewInProcess("b", nil, reg))
	a := Chaos(transport.NewInProcess("a", []string{"b"}, reg), 1, ChaosConfig{DropP: 0.5, DupP: 0.5, AckLostP: 0.5})

	var wg sync.WaitGroup
	for range senders {
		wg.Go(func() {
			for i := range each {
				_ = a.Send(context.Background(), "b", pushFrom("a", fmt.Sprint(i)))
			}
		})
	}
	wg.Wait()
	if got, most := b.count(), 2*senders*each; got > most {
		t.Fatalf("delivered %d times from %d sends, more than two copies each", got, senders*each)
	}
}
