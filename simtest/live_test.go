package simtest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kudesn1k1/artel"
	"github.com/kudesn1k1/artel/transport"
)

// Live engines under chaos. The simulator checks a protocol core under a
// virtual clock and a virtual network; everything the engine wraps around
// the core — the ticker, the worker pool, the locks, the send timeouts, the
// pull on start, Stop and its drain — it never runs. These tests run all of
// it: real engines gossiping over the in-process wire through the chaos
// transport, which fakes only the network. They are written against the
// public surface, NewEngine, Start, Stop and Transport, so they hold the
// engine's runtime to the same claims after any rewrite of its internals:
// it converges under a persistent mix of anomalies, a restarted node catches
// up, a lost ack costs a resend and never a value, and the one thing direct
// dissemination cannot do is pinned as its limit. The engine is asynchronous,
// so every assertion is eventual: poll until it holds or fail on a deadline.
// The oracles still judge the outcome, from a History built by hand and the
// observations read off the replicas.
//
// Two facts about the wire shape what these tests may claim. Delivery is
// synchronous: a pull is answered from inside the peer's handler, on the
// puller's goroutine, through the answering node's chaos transport, so a
// dropped answer fails the pull, which retries, and a delayed answer holds the
// puller's worker for both delays. And a stopped engine keeps its handler
// registered: it still merges pushes and still answers pulls with its final
// state. A dead node here is silent, not absent.

const (
	liveTick     = 2 * time.Millisecond
	liveDeadline = 5 * time.Second
)

// waitFor polls cond until it holds or liveDeadline expires. The detail
// closures run only on failure, so a timeout reports what the cluster
// actually looked like.
func waitFor(t *testing.T, what string, cond func() bool, detail ...func() string) {
	t.Helper()
	deadline := time.Now().Add(liveDeadline)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(liveTick / 2)
	}
	msg := fmt.Sprintf("timed out after %v waiting for %s", liveDeadline, what)
	for _, d := range detail {
		msg += "; actual: " + d()
	}
	t.Fatal(msg)
}

// mix is the legal anomaly mix the live scenarios run under: every link stays
// usable often enough for a retry to get through. Delays stay a few ticks
// long because a pull stacks the answering node's delay on the puller's,
// and the engine's send timeout is not adjustable from here.
func mix() ChaosConfig {
	return ChaosConfig{DropP: 0.3, DupP: 0.3, AckLostP: 0.3, DelayMax: 3 * time.Millisecond}
}

func peersOf(ids []string, self string) []string {
	peers := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}

// countingLink counts the send attempts made through it, so a test can wait
// for a number of gossip rounds to have passed instead of guessing a
// duration.
type countingLink struct {
	artel.Transport
	attempts atomic.Int64
}

func (c *countingLink) Send(ctx context.Context, peerID string, m artel.Message) error {
	c.attempts.Add(1)
	return c.Transport.Send(ctx, peerID, m)
}

func (c *countingLink) waitForAttempts(t *testing.T, n int64) {
	t.Helper()
	target := c.attempts.Load() + n
	waitFor(t, fmt.Sprintf("%d more send attempts", n), func() bool { return c.attempts.Load() >= target })
}

// cutLink fails every send to the named peers and lets the rest through.
type cutLink struct {
	artel.Transport
	cut map[string]bool
}

func (c cutLink) Send(ctx context.Context, peerID string, m artel.Message) error {
	if c.cut[peerID] {
		return fmt.Errorf("simtest: the link to %s is cut", peerID)
	}
	return c.Transport.Send(ctx, peerID, m)
}

// liveCounter is one running counter engine. id is the address; the replica
// id carries the incarnation, so a restarted node never reuses its key.
type liveCounter struct {
	id     string
	rep    *artel.GCounter
	engine *artel.Engine[artel.GCounterState, *artel.GCounter]
}

func startCounter(t *testing.T, id, replica string, tr artel.Transport) *liveCounter {
	t.Helper()
	rep := artel.NewGCounter(replica)
	e := artel.NewEngine(rep, tr, artel.GCounterJSON())
	t.Cleanup(func() { _ = e.Stop(context.Background()) })
	if err := e.Start(context.Background(), liveTick); err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	return &liveCounter{id: id, rep: rep, engine: e}
}

func (n *liveCounter) observe() Observation {
	state, err := artel.GCounterJSON().Encode(n.rep.State())
	if err != nil {
		panic(err)
	}
	return Observation{Node: n.id, State: state, Value: strconv.FormatUint(n.rep.Value(), 10)}
}

func counterValues(nodes ...*liveCounter) string {
	parts := make([]string, 0, len(nodes))
	for _, n := range nodes {
		parts = append(parts, fmt.Sprintf("%s=%d", n.id, n.rep.Value()))
	}
	return strings.Join(parts, " ")
}

func allReach(want uint64, nodes ...*liveCounter) func() bool {
	return func() bool {
		for _, n := range nodes {
			if n.rep.Value() != want {
				return false
			}
		}
		return true
	}
}

// chaosMesh starts one counter engine per id, each behind its own chaos
// transport seeded from its position.
func chaosMesh(t *testing.T, reg *transport.InProcessRegistry, cfg ChaosConfig, ids ...string) []*liveCounter {
	t.Helper()
	nodes := make([]*liveCounter, 0, len(ids))
	for i, id := range ids {
		tr := Chaos(transport.NewInProcess(id, peersOf(ids, id), reg), uint64(i+1), cfg)
		nodes = append(nodes, startCounter(t, id, id, tr))
	}
	return nodes
}

// Three engines under a persistent mix of drops, duplicates, delays and lost
// acks converge on the sum of everything incremented: retain and retry get
// every delta through, and the join absorbs every duplicate. The verdict
// is the oracles', from the op log and the final states.
func TestLiveEnginesConvergeUnderChaos(t *testing.T) {
	ids := []string{"A", "B", "C"}
	nodes := chaosMesh(t, transport.NewInProcessRegistry(), mix(), ids...)

	const perNode = 20
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Go(func() {
			for range perNode {
				n.rep.Increment()
			}
		})
	}
	wg.Wait()

	want := uint64(len(ids) * perNode)
	waitFor(t, fmt.Sprintf("every replica to reach %d", want), allReach(want, nodes...),
		func() string { return counterValues(nodes...) })

	h := History{Nodes: ids}
	for _, id := range ids {
		for range perNode {
			h.Ops = append(h.Ops, Op{Node: id, Op: "inc:1"})
		}
	}
	final := make([]Observation, 0, len(nodes))
	for _, n := range nodes {
		final = append(final, n.observe())
	}
	for _, o := range []Oracle{Convergence(), CounterSum()} {
		if v := o.Check(h, final); len(v) != 0 {
			t.Fatalf("%s: %+v", o.Name(), v)
		}
	}
}

// A node that stops and comes back as a new incarnation catches up on
// everything, under the same chaos: its start-up pull is answered through the
// peers' chaos transports and retried until an answer gets through, and its
// own increments join the total.
func TestLiveRestartCatchesUpUnderChaos(t *testing.T) {
	ids := []string{"A", "B", "C"}
	reg := transport.NewInProcessRegistry()
	nodes := chaosMesh(t, reg, mix(), ids...)
	a, b, c := nodes[0], nodes[1], nodes[2]

	for _, n := range nodes {
		n.rep.IncrementBy(5)
	}
	waitFor(t, "the mesh to reach 15", allReach(15, nodes...), func() string { return counterValues(nodes...) })

	if err := b.engine.Stop(context.Background()); err != nil {
		t.Fatalf("stop B: %v", err)
	}
	restarted := startCounter(t, "B", "B#2", Chaos(transport.NewInProcess("B", peersOf(ids, "B"), reg), 9, mix()))

	a.rep.IncrementBy(2)
	c.rep.IncrementBy(3)
	restarted.rep.Increment()
	waitFor(t, "the restarted B and its peers to reach 21", allReach(21, a, c, restarted),
		func() string { return counterValues(a, c, restarted) })
}

// The liveness hole of direct dissemination, on the real engine. A reaches B
// but its link to C is cut; then A stops for good. B holds the update and
// never relays it, C keeps pulling A and every answer dies on the cut link:
// C never learns the update. The run is held for fifty of C's rounds after
// A stops, and the convergence oracle, told that A is dead, sees B and C
// disagree. This is the documented limit of the protocol, not a defect of
// the engine: a relaying protocol would carry the update from B.
func TestLiveOriginDiesAfterPartialDissemination(t *testing.T) {
	ids := []string{"A", "B", "C"}
	reg := transport.NewInProcessRegistry()
	a := startCounter(t, "A", "A", cutLink{transport.NewInProcess("A", peersOf(ids, "A"), reg), map[string]bool{"C": true}})
	b := startCounter(t, "B", "B", transport.NewInProcess("B", peersOf(ids, "B"), reg))
	cLink := &countingLink{Transport: transport.NewInProcess("C", peersOf(ids, "C"), reg)}
	c := startCounter(t, "C", "C", cLink)

	a.rep.IncrementBy(4)
	waitFor(t, "B to receive A's update", func() bool { return b.rep.Value() == 4 })
	if err := a.engine.Stop(context.Background()); err != nil {
		t.Fatalf("stop A: %v", err)
	}

	cLink.waitForAttempts(t, 50)
	if got := c.rep.Value(); got != 0 {
		t.Fatalf("C reads %d: the update reached it without a carrier", got)
	}

	h := History{Nodes: ids, Crashed: []string{"A"}}
	violations := Convergence().Check(h, []Observation{a.observe(), b.observe(), c.observe()})
	if len(violations) != 1 || !strings.Contains(violations[0].Detail, "C") {
		t.Fatalf("convergence reported %+v, want C alone against B", violations)
	}
}

// Every push from A is delivered but reported failed, so A retains and
// resends the same delta every round for as long as the test runs. B merges
// each copy into the same value: the join is idempotent, and a lost ack costs
// bandwidth, never correctness. B has no peers of its own on purpose: A's
// pushes are the only route.
func TestLiveRetainSurvivesLostAcks(t *testing.T) {
	reg := transport.NewInProcessRegistry()
	aLink := &countingLink{Transport: Chaos(transport.NewInProcess("A", []string{"B"}, reg), 1, ChaosConfig{AckLostP: 1})}
	a := startCounter(t, "A", "A", aLink)
	b := startCounter(t, "B", "B", transport.NewInProcess("B", nil, reg))

	a.rep.IncrementBy(5)
	waitFor(t, "B to reach 5", func() bool { return b.rep.Value() == 5 })

	aLink.waitForAttempts(t, 20)
	if got := b.rep.Value(); got != 5 {
		t.Fatalf("B reads %d after twenty resends, want 5", got)
	}
}
