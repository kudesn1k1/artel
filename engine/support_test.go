package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crdtlab/transport"
	"crdtlab/types/delta/counter"
)

// Test support for the anti-entropy engine.
//
// The engine is asynchronous by design (ticker + bounded worker pool), so every
// assertion about what a peer ended up with is EVENTUAL: poll until it holds or
// fail on a deadline. We do not reshape the engine to make tests synchronous.

const (
	// tick is the gossip interval: short enough that several rounds fit in a
	// poll deadline, long enough not to spin the CPU.
	tick = 5 * time.Millisecond

	// waitDeadline bounds every eventual assertion.
	waitDeadline = 3 * time.Second
)

// decodeGCounter is the decoder closure the engine needs: UnmarshalBinary lives
// on *GCounterState, the engine ships values of GCounterState.
func decodeGCounter(b []byte) (counter.GCounterState, error) {
	var s counter.GCounterState
	if err := s.UnmarshalBinary(b); err != nil {
		return counter.GCounterState{}, err
	}
	return s, nil
}

type gEngine = Engine[counter.GCounterState, *counter.GCounter]

// node is one replica + its transport + its engine.
type node struct {
	id      string
	replica *counter.GCounter
	engine  *gEngine
}

// newNode wires a node onto the shared registry but does NOT start gossiping.
// Stopping it is registered as cleanup, so a test may also stop it early.
func newNode(t *testing.T, reg *transport.Registry, id string, peers ...string) *node {
	t.Helper()
	rep := counter.NewGCounter(id)
	e := NewEngine(rep, transport.NewInProcess(id, peers, reg), decodeGCounter)
	t.Cleanup(func() { _ = e.Stop() })
	return &node{id: id, replica: rep, engine: e}
}

func (n *node) start(t *testing.T) {
	t.Helper()
	if err := n.engine.Start(tick); err != nil {
		t.Fatalf("start %s: %v", n.id, err)
	}
}

// mesh builds a running full-mesh cluster: every node gossips to all the others.
func mesh(t *testing.T, ids ...string) map[string]*node {
	t.Helper()
	reg := transport.NewRegistry()
	nodes := make(map[string]*node, len(ids))
	for _, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		nodes[id] = newNode(t, reg, id, peers...)
	}
	for _, n := range nodes {
		n.start(t)
	}
	return nodes
}

// waitFor polls cond until it holds or waitDeadline expires.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitDeadline)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(tick / 2)
	}
	t.Fatalf("timed out after %v waiting for %s", waitDeadline, what)
}

// flakyLink wraps a Transport so a test can cut and restore the wire, and can
// wait for a real send attempt instead of sleeping a guessed duration.
type flakyLink struct {
	transport.Transport
	broken   atomic.Bool
	attempts atomic.Int64
}

func (f *flakyLink) Send(peerID string, m transport.Message) error {
	f.attempts.Add(1)
	if f.broken.Load() {
		return fmt.Errorf("flaky: peer %q unreachable", peerID)
	}
	return f.Transport.Send(peerID, m)
}

// waitForAttemptsAfter waits until at least two further send attempts have been
// made, i.e. a whole round has certainly run since the caller's last mutation.
func (f *flakyLink) waitForAttemptsAfter(t *testing.T, baseline int64) {
	t.Helper()
	waitFor(t, "a gossip round to attempt a send", func() bool {
		return f.attempts.Load() >= baseline+2
	})
}

// gatedLink parks every Send until the gate is opened, so a test can hold a
// send "in flight" for as long as it likes.
type gatedLink struct {
	transport.Transport
	gate chan struct{}
	once sync.Once
}

func newGatedLink(inner transport.Transport) *gatedLink {
	return &gatedLink{Transport: inner, gate: make(chan struct{})}
}

func (g *gatedLink) Send(peerID string, m transport.Message) error {
	<-g.gate
	return g.Transport.Send(peerID, m)
}

func (g *gatedLink) open() { g.once.Do(func() { close(g.gate) }) }

// manyPeers reports more peers than the engine's send queue can hold, so a
// single round cannot possibly enqueue a job for every peer.
type manyPeers struct {
	ids    []string
	mu     sync.Mutex
	pulled map[string]struct{}
}

func newManyPeers(n int) *manyPeers {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("peer-%d", i)
	}
	return &manyPeers{ids: ids, pulled: make(map[string]struct{})}
}

// Send always succeeds; the Pulls that made it out are recorded.
func (m *manyPeers) Send(peerID string, msg transport.Message) error {
	if msg.Kind == transport.Pull {
		m.mu.Lock()
		m.pulled[peerID] = struct{}{}
		m.mu.Unlock()
	}
	return nil
}

func (m *manyPeers) pulledCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pulled)
}

func (m *manyPeers) ID() string                    { return "many_peers" }
func (m *manyPeers) Peers() []string               { return m.ids }
func (m *manyPeers) Serve(transport.Handler) error { return nil }
func (m *manyPeers) Close() error                  { return nil }

// probe is a bare registry participant — no engine — used to speak the wire
// protocol directly: send a raw Message and record what comes back.
type probe struct {
	tr       *transport.InProcess
	mu       sync.Mutex
	received []transport.Message
}

func newProbe(t *testing.T, reg *transport.Registry, id string, peers ...string) *probe {
	t.Helper()
	p := &probe{tr: transport.NewInProcess(id, peers, reg)}
	if err := p.tr.Serve(func(m transport.Message) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.received = append(p.received, m)
		return nil
	}); err != nil {
		t.Fatalf("probe %s serve: %v", id, err)
	}
	return p
}

func (p *probe) pushes() []transport.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []transport.Message
	for _, m := range p.received {
		if m.Kind == transport.Push {
			out = append(out, m)
		}
	}
	return out
}
