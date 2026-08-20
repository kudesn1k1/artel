package artel_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kudesn1k1/artel"
	"github.com/kudesn1k1/artel/transport"
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

	// neverTimeOut is handed to sendTimeout by the tests that need a send to stay
	// genuinely parked inside the transport, so a per-send deadline cannot be what
	// makes them pass. Tests about deadlines set a short one instead.
	neverTimeOut = time.Hour
)

// decodeGCounter turns a wire payload back into a state. The engine derives this
// for itself from its type parameters; the tests need it to inspect payloads they
// intercept on the wire.
func decodeGCounter(b []byte) (artel.GCounterState, error) {
	var s artel.GCounterState
	if err := s.UnmarshalBinary(b); err != nil {
		return artel.GCounterState{}, err
	}
	return s, nil
}

type gEngine = artel.Engine[artel.GCounterState, *artel.GCounterState, *artel.GCounter]

// node is one replica + its transport + its engine.
//
// Note the two ids: nodeID is the transport address book key and is stable
// across restarts, replicaID is the incarnation and is fresh on every boot.
// Most tests never restart anything and pass the same string for both.
type node struct {
	nodeID    string
	replicaID string
	replica   *artel.GCounter
	engine    *gEngine
}

// newNode wires a node onto the shared registry but does NOT start gossiping.
// Stopping it is registered as cleanup, so a test may also stop it early.
func newNode(t *testing.T, reg *transport.Registry, id string, peers ...string) *node {
	t.Helper()
	return newNodeAs(t, reg, id, id, peers...)
}

// newNodeAs is newNode with the node id and the replica id given separately —
// what a restart actually looks like: same address, new incarnation.
func newNodeAs(t *testing.T, reg *transport.Registry, nodeID, replicaID string, peers ...string) *node {
	t.Helper()
	rep := artel.NewGCounter(replicaID)
	e := artel.NewEngine(rep, transport.NewInProcess(nodeID, peers, reg))
	t.Cleanup(func() { _ = e.Stop(context.Background()) })
	return &node{nodeID: nodeID, replicaID: replicaID, replica: rep, engine: e}
}

func (n *node) start(t *testing.T) {
	t.Helper()
	if err := n.engine.Start(context.Background(), tick); err != nil {
		t.Fatalf("start %s: %v", n.nodeID, err)
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

// waitFor polls cond until it holds or waitDeadline expires. The optional detail
// closures are evaluated only on failure, so a timeout reports what the world
// actually looked like instead of just what it should have looked like.
func waitFor(t *testing.T, what string, cond func() bool, detail ...func() string) {
	t.Helper()
	deadline := time.Now().Add(waitDeadline)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(tick / 2)
	}
	msg := fmt.Sprintf("timed out after %v waiting for %s", waitDeadline, what)
	for _, d := range detail {
		msg += "; actual: " + d()
	}
	t.Fatal(msg)
}

// valuesOf renders the cluster as "A=3 B=3 C=0", for failure messages.
func valuesOf(nodes map[string]*node) string {
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%s=%d", id, nodes[id].replica.Value()))
	}
	return strings.Join(parts, " ")
}

// flakyLink wraps a Transport so a test can cut and restore the wire, and can
// wait for a real send attempt instead of sleeping a guessed duration.
type flakyLink struct {
	artel.Transport
	broken   atomic.Bool
	attempts atomic.Int64
}

func (f *flakyLink) Send(ctx context.Context, peerID string, m artel.Message) error {
	f.attempts.Add(1)
	if f.broken.Load() {
		return fmt.Errorf("flaky: peer %q unreachable", peerID)
	}
	return f.Transport.Send(ctx, peerID, m)
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
//
// It parks on the caller's ctx as well as on the gate: a real transport aborts
// when its context is cancelled or expires, and a mock that ignored ctx would
// make cancellation untestable.
type gatedLink struct {
	artel.Transport
	gate     chan struct{}
	once     sync.Once
	attempts atomic.Int64
	parked   atomic.Int64
}

func newGatedLink(inner artel.Transport) *gatedLink {
	return &gatedLink{Transport: inner, gate: make(chan struct{})}
}

func (g *gatedLink) Send(ctx context.Context, peerID string, m artel.Message) error {
	g.attempts.Add(1)
	g.parked.Add(1)
	defer g.parked.Add(-1)

	select {
	case <-g.gate:
		return g.Transport.Send(ctx, peerID, m)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gatedLink) open() { g.once.Do(func() { close(g.gate) }) }

// partialGate stalls sends to the named peers only and lets every other peer
// through, so a test can hold one peer hostage without freezing the whole node.
type partialGate struct {
	artel.Transport
	stalled map[string]bool
	gate    chan struct{}
	once    sync.Once
}

func newPartialGate(inner artel.Transport, stalled ...string) *partialGate {
	set := make(map[string]bool, len(stalled))
	for _, id := range stalled {
		set[id] = true
	}
	return &partialGate{Transport: inner, stalled: set, gate: make(chan struct{})}
}

func (g *partialGate) Send(ctx context.Context, peerID string, m artel.Message) error {
	if g.stalled[peerID] {
		select {
		case <-g.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.Transport.Send(ctx, peerID, m)
}

func (g *partialGate) open() { g.once.Do(func() { close(g.gate) }) }

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
func (m *manyPeers) Send(_ context.Context, peerID string, msg artel.Message) error {
	if msg.Kind == artel.KindPull {
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

func (m *manyPeers) ID() string                { return "many_peers" }
func (m *manyPeers) Peers() []string           { return m.ids }
func (m *manyPeers) Serve(artel.Handler) error { return nil }
func (m *manyPeers) Close() error              { return nil }

// probe is a bare registry participant — no engine — used to speak the wire
// protocol directly: send a raw Message and record what comes back.
type probe struct {
	tr       *transport.InProcess
	mu       sync.Mutex
	received []artel.Message
}

func newProbe(t *testing.T, reg *transport.Registry, id string, peers ...string) *probe {
	t.Helper()
	p := &probe{tr: transport.NewInProcess(id, peers, reg)}
	if err := p.tr.Serve(func(_ context.Context, m artel.Message) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.received = append(p.received, m)
		return nil
	}); err != nil {
		t.Fatalf("probe %s serve: %v", id, err)
	}
	return p
}

func (p *probe) send(to string, m artel.Message) error {
	return p.tr.Send(context.Background(), to, m)
}

func (p *probe) pushes() []artel.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []artel.Message
	for _, m := range p.received {
		if m.Kind == artel.KindPush {
			out = append(out, m)
		}
	}
	return out
}
