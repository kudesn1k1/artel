package artel_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kudesn1k1/artel"
	"github.com/kudesn1k1/artel/transport"
)

// Behavioural spec for the Algorithm-1 anti-entropy engine, driven over the
// synchronous InProcess transport.
//
// Every assertion about what a peer ended up with is EVENTUAL — the engine is
// asynchronous by design and is not reshaped to make tests deterministic. Tests
// live in the external artel_test package and reach the internals only through
// the export_test.go bridges (Round, SetSendTimeout, WorkerCount); everything
// else goes through the public surface and the Transport interface, so they
// survive refactors of the engine's internals.
//
// The real network path (transport/http.go) is covered in transport/http_test.go.

// --- convergence -------------------------------------------------------------

func TestEngineConvergesAcrossFullMesh(t *testing.T) {
	nodes := mesh(t, "A", "B", "C")

	nodes["A"].replica.IncrementBy(3)
	nodes["B"].replica.IncrementBy(2)
	nodes["C"].replica.Increment()

	waitFor(t, "every replica to reach 6", func() bool {
		return nodes["A"].replica.Value() == 6 &&
			nodes["B"].replica.Value() == 6 &&
			nodes["C"].replica.Value() == 6
	}, func() string { return valuesOf(nodes) })
}

// The delta buffer is drained by FlushDelta and the per-peer buffer is cleared
// optimistically, so a mutation landing between a flush and its send is the
// window where an update could go missing. Hammer that window.
func TestEngineLosesNoUpdateWhileGossiping(t *testing.T) {
	const perNode = 500
	nodes := mesh(t, "A", "B", "C")

	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Go(func() {
			for range perNode {
				n.replica.Increment()
			}
		})
	}
	wg.Wait()

	want := uint64(len(nodes) * perNode)
	waitFor(t, fmt.Sprintf("every replica to reach %d", want), func() bool {
		for _, n := range nodes {
			if n.replica.Value() != want {
				return false
			}
		}
		return true
	}, func() string { return valuesOf(nodes) })
}

// Rounds keep firing after convergence, re-delivering states that are already
// merged. Absolute deltas make that idempotent — the value must not drift.
func TestEngineSteadyStateDoesNotDrift(t *testing.T) {
	nodes := mesh(t, "A", "B")
	nodes["A"].replica.IncrementBy(4)
	waitFor(t, "the pair to converge on 4", func() bool {
		return nodes["A"].replica.Value() == 4 && nodes["B"].replica.Value() == 4
	})

	time.Sleep(20 * tick) // ~20 further rounds of re-delivery

	for id, n := range nodes {
		if got := n.replica.Value(); got != 4 {
			t.Fatalf("%s drifted to %d after idle rounds, want 4", id, got)
		}
	}
}

// Convergence must survive round after round, not just the first one. Every
// other convergence test here happens to need a single successful push per
// peer, which would hide any state that is armed once and never cleared.
func TestEngineKeepsConvergingAcrossRounds(t *testing.T) {
	nodes := mesh(t, "A", "B", "C")
	order := []string{"A", "B", "C", "A"}

	var total uint64
	for i, id := range order {
		nodes[id].replica.Increment()
		total++

		waitFor(t, fmt.Sprintf("every replica to reach %d after increment #%d", total, i+1), func() bool {
			for _, n := range nodes {
				if n.replica.Value() != total {
					return false
				}
			}
			return true
		}, func() string { return valuesOf(nodes) })
	}
}

// The engine is generic over the state type; every other test here instantiates
// it with exactly one. This one drives a second type end to end, and with
// decrements, so PNCounterState's own Join/IsBottom/codec ride the same path.
func TestEngineConvergesWithPNCounter(t *testing.T) {
	reg := transport.NewRegistry()

	newPN := func(id string, peers ...string) *artel.PNCounter {
		rep := artel.NewPNCounter(id)
		e := artel.NewEngine(rep, transport.NewInProcess(id, peers, reg))
		t.Cleanup(func() { _ = e.Stop(context.Background()) })
		if err := e.Start(context.Background(), tick); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		return rep
	}

	a := newPN("A", "B")
	b := newPN("B", "A")

	a.IncrementBy(10)
	b.DecrementBy(4)

	waitFor(t, "both replicas to settle on 6", func() bool {
		return a.Value() == 6 && b.Value() == 6
	}, func() string { return fmt.Sprintf("A=%d B=%d", a.Value(), b.Value()) })
}

// Nodes coming up at the same instant Pull each other simultaneously, and with
// InProcess a Pull is answered re-entrantly on the caller's goroutine. Nothing
// may hold a lock across that path — this test hangs (and the suite times out)
// if anything ever does.
func TestEngineStartsConcurrently(t *testing.T) {
	reg := transport.NewRegistry()
	ids := []string{"A", "B", "C", "D"}

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

	errs := make(chan error, len(nodes))
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Go(func() { errs <- n.engine.Start(context.Background(), tick) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent start: %v", err)
		}
	}

	for _, n := range nodes {
		n.replica.Increment()
	}
	waitFor(t, "every replica to reach 4", func() bool {
		for _, n := range nodes {
			if n.replica.Value() != 4 {
				return false
			}
		}
		return true
	}, func() string { return valuesOf(nodes) })
}

// --- per-peer retain ---------------------------------------------------------

// A failed Send must put the snapshot back into the peer's buffer. Nothing is
// incremented after the link heals, so only retention can still deliver.
func TestEngineRetainsDeltaWhenSendFails(t *testing.T) {
	reg := transport.NewRegistry()

	link := &flakyLink{Transport: transport.NewInProcess("A", []string{"B"}, reg)}
	a := artel.NewGCounter("A")
	ae := artel.NewEngine(a, link)
	t.Cleanup(func() { _ = ae.Stop(context.Background()) })

	// B has no peers of its own on purpose: it never Pulls A, so A's retained
	// push is the ONLY route by which the increment can reach it.
	b := newNode(t, reg, "B")
	b.start(t)

	link.broken.Store(true)
	if err := ae.Start(context.Background(), tick); err != nil {
		t.Fatalf("start A: %v", err)
	}

	a.IncrementBy(5)
	link.waitForAttemptsAfter(t, link.attempts.Load())
	if got := b.replica.Value(); got != 0 {
		t.Fatalf("B saw %d while the link to A was down, want 0", got)
	}

	// Heal the wire. A's delta buffer was already flushed by the failed round,
	// so B can only be reached from A's retain buffer for B.
	link.broken.Store(false)
	waitFor(t, "B to receive the retained delta", func() bool {
		return b.replica.Value() == 5
	})
}

// Same mechanism, network-shaped: a peer that was never reachable (not yet
// registered) must still receive everything once it shows up.
func TestEngineDeliversToPeerThatJoinsLate(t *testing.T) {
	reg := transport.NewRegistry()

	// The wrapper is only here to count attempts — it never breaks the link;
	// sends fail on their own because "B" is not registered yet.
	link := &flakyLink{Transport: transport.NewInProcess("A", []string{"B"}, reg)}
	a := artel.NewGCounter("A")
	ae := artel.NewEngine(a, link)
	t.Cleanup(func() { _ = ae.Stop(context.Background()) })

	if err := ae.Start(context.Background(), tick); err != nil {
		t.Fatalf("start A: %v", err)
	}
	a.IncrementBy(4)
	link.waitForAttemptsAfter(t, link.attempts.Load())

	b := newNode(t, reg, "B", "A")
	b.start(t)

	waitFor(t, "the late joiner to receive A's retained delta", func() bool {
		return b.replica.Value() == 4
	})
}

// A round that skips a peer because a push to it is still in flight must still
// park the freshly flushed delta in that peer's buffer. The delta has already
// been drained out of the replica by FlushDelta, so a round that merely skips
// leaves it nowhere at all.
func TestEngineKeepsDeltasFlushedWhileAPushIsInFlight(t *testing.T) {
	reg := transport.NewRegistry()

	gate := newGatedLink(transport.NewInProcess("A", []string{"B"}, reg))
	a := artel.NewGCounter("A")
	ae := artel.NewEngine(a, gate)
	ae.SetSendTimeout(neverTimeOut) // this test is about a busy peer, not about deadlines
	t.Cleanup(func() { _ = ae.Stop(context.Background()) })
	t.Cleanup(gate.open) // LIFO: the gate opens before Stop waits on the workers

	// B has no peers: it never Pulls, so A's pushes are the only route to it.
	b := newNode(t, reg, "B")
	b.start(t)

	a.IncrementBy(1)
	if err := ae.Start(context.Background(), tick); err != nil {
		t.Fatalf("start A: %v", err)
	}

	// Start's first round shipped {A:1} and the worker is now parked inside Send.
	// This increment lands while that push is in flight, so every round until the
	// gate opens finds B busy and skips it.
	a.IncrementBy(2)
	time.Sleep(10 * tick)

	gate.open()
	waitFor(t, "B to receive both increments", func() bool {
		return b.replica.Value() == 3
	})
}

// An unreachable peer parks a worker inside Send for as long as it stays
// unreachable. The healthy peers must keep receiving — a shared queue drained by
// a fixed pool is only safe while stalled peers cannot occupy the whole pool.
//
// This one pins queue fairness with the deadline deliberately switched off, so a
// stalled peer really does hold a worker for the whole test: what is guarded here
// is that a shared queue drained by a fixed pool still reaches the healthy peer.
// The companion test below covers what happens once stalled peers outnumber the
// pool — that case needs deadlines, and nothing else can save it.
func TestEngineKeepsServingHealthyPeersWhileOneStalls(t *testing.T) {
	reg := transport.NewRegistry()

	// The stalled peer is listed first, so its jobs are queued ahead of B's.
	gate := newPartialGate(transport.NewInProcess("A", []string{"stalled", "B"}, reg), "stalled")
	a := artel.NewGCounter("A")
	ae := artel.NewEngine(a, gate)
	ae.SetSendTimeout(neverTimeOut)
	t.Cleanup(func() { _ = ae.Stop(context.Background()) })
	t.Cleanup(gate.open) // LIFO: the gate opens before Stop waits on the workers

	b := newNode(t, reg, "B")
	b.start(t)

	a.IncrementBy(9)
	if err := ae.Start(context.Background(), tick); err != nil {
		t.Fatalf("start A: %v", err)
	}

	waitFor(t, "B to be served while another peer is stalled", func() bool {
		return b.replica.Value() == 9
	}, func() string { return fmt.Sprintf("B=%d", b.replica.Value()) })
}

// Head-of-line starvation, the version no amount of queue fairness can fix: each
// unreachable peer occupies a worker for as long as it stays unreachable, and it
// occupies TWO of them (its push job and its pull job), so workerCount/2 dead
// peers park the entire pool and the healthy peer is never reached at all.
//
// A per-send deadline is the only thing that breaks this: a worker must come back
// on its own, without the peer's cooperation. Give the pool more stalled peers
// than it has workers and check the healthy one is still served.
func TestEngineKeepsServingWhenStalledPeersOutnumberTheWorkers(t *testing.T) {
	reg := transport.NewRegistry()

	stalled := make([]string, 0, artel.WorkerCount)
	for i := range artel.WorkerCount {
		stalled = append(stalled, fmt.Sprintf("stalled-%d", i))
	}
	// Stalled peers first: their jobs are queued ahead of B's, so every worker
	// picks one of them before B's job is reachable.
	peers := append(append([]string{}, stalled...), "B")

	gate := newPartialGate(transport.NewInProcess("A", peers, reg), stalled...)
	a := artel.NewGCounter("A")
	ae := artel.NewEngine(a, gate)
	ae.SetSendTimeout(20 * tick) // short enough that several deadline cycles fit in waitDeadline
	t.Cleanup(func() { _ = ae.Stop(context.Background()) })
	t.Cleanup(gate.open)

	b := newNode(t, reg, "B")
	b.start(t)

	a.IncrementBy(9)
	if err := ae.Start(context.Background(), tick); err != nil {
		t.Fatalf("start A: %v", err)
	}

	waitFor(t, "B to be served although every worker is stuck on a dead peer", func() bool {
		return b.replica.Value() == 9
	}, func() string { return fmt.Sprintf("B=%d", b.replica.Value()) })
}

// A send killed by its own deadline is a failed send: the snapshot it carried was
// already drained out of the replica, so it has to go back into the peer's buffer
// exactly as it does on a transport error. Nothing is incremented after the wire
// heals — only retention can still deliver.
func TestEngineRetainsDeltaWhenASendTimesOut(t *testing.T) {
	reg := transport.NewRegistry()

	gate := newGatedLink(transport.NewInProcess("A", []string{"B"}, reg))
	a := artel.NewGCounter("A")
	ae := artel.NewEngine(a, gate)
	ae.SetSendTimeout(20 * tick)
	t.Cleanup(func() { _ = ae.Stop(context.Background()) })
	t.Cleanup(gate.open)

	// B has no peers of its own: it never Pulls A, so A's retained push is the
	// only route by which the increment can reach it.
	b := newNode(t, reg, "B")
	b.start(t)

	a.IncrementBy(5)
	if err := ae.Start(context.Background(), tick); err != nil {
		t.Fatalf("start A: %v", err)
	}

	// Two attempts means at least one send has certainly been killed by its
	// deadline and its round has moved on.
	waitFor(t, "a send to be abandoned on its deadline", func() bool {
		return gate.attempts.Load() >= 2
	})
	if got := b.replica.Value(); got != 0 {
		t.Fatalf("B saw %d while every send to it was timing out, want 0", got)
	}

	gate.open()
	waitFor(t, "B to receive the delta retained across the timeouts", func() bool {
		return b.replica.Value() == 5
	})
}

// --- catch-up after state loss -----------------------------------------------

// Retention does not cover a peer that ACKed everything and then lost its state:
// A's buffer for B is empty and A has nothing left to resend. Nothing is
// incremented after the restart — an absolute delta would otherwise heal B on
// the next increment and hide the missing catch-up. Only a Pull of full state on
// (re)join fixes this.
func TestEngineCatchesUpAfterRestart(t *testing.T) {
	reg := transport.NewRegistry()
	a := newNode(t, reg, "A", "B")
	b := newNode(t, reg, "B", "A")
	a.start(t)
	b.start(t)

	a.replica.IncrementBy(3)
	b.replica.IncrementBy(2)
	waitFor(t, "the pair to converge on 5", func() bool {
		return a.replica.Value() == 5 && b.replica.Value() == 5
	})

	if err := b.engine.Stop(context.Background()); err != nil {
		t.Fatalf("stop B: %v", err)
	}

	// B comes back at the same address with an empty in-memory replica and a fresh
	// incarnation id — that is what a restart looks like here.
	restarted := newNodeAs(t, reg, "B", "B#2", "A")
	restarted.start(t)

	waitFor(t, "the restarted B to catch up to 5", func() bool {
		return restarted.replica.Value() == 5
	})
}

// Why the replica id is an incarnation and not the node id.
//
// A restarted node comes back with an empty state, and its own entry in the
// lattice is the one thing no peer can correct for it: the join takes the maximum
// per key, so an update made under a reused id BEFORE the catch-up lands is not
// merged, it is silently eaten — max(1, 2) = 2. A fresh id makes it a new key
// that nothing can shadow.
//
// This test increments before Start on purpose: that is the window, and doing it
// there makes it deterministic instead of a race the test would usually lose.
func TestEngineNewIncarnationKeepsAnUpdateMadeBeforeCatchUp(t *testing.T) {
	reg := transport.NewRegistry()
	a := newNode(t, reg, "A", "B")
	b := newNode(t, reg, "B", "A")
	a.start(t)
	b.start(t)

	a.replica.IncrementBy(3)
	b.replica.IncrementBy(2)
	waitFor(t, "the pair to converge on 5", func() bool {
		return a.replica.Value() == 5 && b.replica.Value() == 5
	})

	if err := b.engine.Stop(context.Background()); err != nil {
		t.Fatalf("stop B: %v", err)
	}

	restarted := newNodeAs(t, reg, "B", "B#2", "A")
	restarted.replica.Increment() // before any catch-up: the dangerous window
	restarted.start(t)

	waitFor(t, "the cluster to reach 6 — the pre-catch-up increment must survive", func() bool {
		return restarted.replica.Value() == 6 && a.replica.Value() == 6
	}, func() string {
		return fmt.Sprintf("A=%d B#2=%d (5 means the increment was swallowed by the join)",
			a.replica.Value(), restarted.replica.Value())
	})
}

// A Pull carries no payload: consume must dispatch on Kind and answer with a
// Push of the local full state instead of trying to merge an empty message.
func TestEngineAnswersPullWithFullState(t *testing.T) {
	reg := transport.NewRegistry()
	a := newNode(t, reg, "A") // no peers: this is about the inbound path only
	a.start(t)
	a.replica.IncrementBy(7)

	p := newProbe(t, reg, "probe", "A")
	if err := p.send("A", transport.Message{From: "probe", Kind: transport.Pull}); err != nil {
		t.Fatalf("A rejected a Pull: %v", err)
	}

	waitFor(t, "A to answer the Pull with a Push", func() bool {
		return len(p.pushes()) > 0
	})

	answer := p.pushes()[0]
	if answer.From != "A" {
		t.Fatalf("Push came from %q, want %q", answer.From, "A")
	}
	state, err := decodeGCounter(answer.Payload)
	if err != nil {
		t.Fatalf("A's Push payload does not decode: %v", err)
	}
	mirror := artel.NewGCounter("mirror")
	mirror.Merge(state)
	if got := mirror.Value(); got != 7 {
		t.Fatalf("Pull answered with a state worth %d, want A's full state 7", got)
	}
}

// A Pull that could not be handed to the workers — the queue was full when its
// round ran — must be retried, not dropped. A peer that never receives a Pull
// never answers with its full state, and nothing else will ever ship us another
// replica's keys: deltas only carry keys their sender mutated.
func TestEnginePullSurvivesAFullSendQueue(t *testing.T) {
	tr := newManyPeers(150) // 150 peers vs a 100-slot queue: one round cannot cover them all
	e := artel.NewEngine(artel.NewGCounter("A"), tr)
	t.Cleanup(func() { _ = e.Stop(context.Background()) })

	if err := e.Start(context.Background(), tick); err != nil {
		t.Fatalf("start: %v", err)
	}

	deadline := time.Now().Add(waitDeadline)
	for time.Now().Before(deadline) {
		if tr.pulledCount() == len(tr.ids) {
			return
		}
		time.Sleep(tick / 2)
	}
	t.Fatalf("only %d of %d peers ever received a Pull: the ones that did not fit in the queue were dropped",
		tr.pulledCount(), len(tr.ids))
}

// --- lifecycle ---------------------------------------------------------------

func TestEngineStop(t *testing.T) {
	t.Run("is idempotent", func(t *testing.T) {
		reg := transport.NewRegistry()
		n := newNode(t, reg, "A")
		n.start(t)

		if err := n.engine.Stop(context.Background()); err != nil {
			t.Fatalf("first Stop: %v", err)
		}
		if err := n.engine.Stop(context.Background()); err != nil {
			t.Fatalf("second Stop: %v", err)
		}
	})

	// A round that cannot hand its jobs to the workers must not outlive the
	// engine: it has already flushed the local delta and cleared the per-peer
	// buffers, so a round parked forever on the queue leaks a goroutine AND
	// silently drops those deltas.
	t.Run("releases a round blocked on a full send queue", func(t *testing.T) {
		// Workers are deliberately not started — nothing drains the queue.
		e := artel.NewEngine(artel.NewGCounter("A"), newManyPeers(150))

		returned := make(chan struct{})
		go func() {
			e.Round()
			close(returned)
		}()

		if err := e.Stop(context.Background()); err != nil {
			t.Fatalf("stop: %v", err)
		}
		select {
		case <-returned:
		case <-time.After(waitDeadline):
			t.Fatalf("round() still blocked %v after Stop", waitDeadline)
		}
	})

	// Stop waits for the workers, so a worker parked inside a Send holds shutdown
	// hostage for exactly as long as the peer chooses. Cancelling the engine's
	// context has to reach into the transport and abort the send in progress —
	// note the gate is never opened here, so nothing else can release it, and the
	// deadline is set past the test's own patience so it cannot be the rescuer.
	t.Run("releases a send parked in the transport", func(t *testing.T) {
		reg := transport.NewRegistry()
		gate := newGatedLink(transport.NewInProcess("A", []string{"B"}, reg))
		a := artel.NewGCounter("A")
		ae := artel.NewEngine(a, gate)
		ae.SetSendTimeout(neverTimeOut)
		t.Cleanup(gate.open) // only so a failing run cannot wedge the suite

		b := newNode(t, reg, "B")
		b.start(t)

		a.IncrementBy(1)
		if err := ae.Start(context.Background(), tick); err != nil {
			t.Fatalf("start A: %v", err)
		}
		waitFor(t, "a send to park inside the transport", func() bool {
			return gate.parked.Load() > 0
		})

		stopped := make(chan error, 1)
		go func() { stopped <- ae.Stop(context.Background()) }()

		select {
		case err := <-stopped:
			if err != nil {
				t.Fatalf("Stop: %v", err)
			}
		case <-time.After(waitDeadline):
			t.Fatalf("Stop still waiting after %v: cancellation never reached the parked send", waitDeadline)
		}
	})
}
