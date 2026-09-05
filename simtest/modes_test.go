package simtest

import (
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"github.com/kudesn1k1/artel"
)

// Trust tests for the modes (D11b): a zero-CRDT subject whose failure is a
// plain function of the scenario's ops, so every verdict below is predicted
// by hand.
//
// Stress is pinned in full. Shrink is pinned only where every deletion-based
// minimizer must agree — the result still fails the SAME oracles, stays
// connected when the input was, is smaller, is valid, is minimal for a
// monotone failure, never touches Seed, Interval or Settle, is deterministic,
// and leaves a passing scenario alone. Left open on purpose: the mutation
// order and the number of Run calls, and which of several equivalent nodes
// survives.

// failIfOpCountAtLeast builds nodes that all observe "calm" — until a node
// has applied at least k ops, when it observes "tripped:<id>" instead. With
// two or more nodes Convergence then fails exactly when some node applied ≥k
// ops: monotone in the ops, blind to the network, k=0 fails always. Nodes
// gossip pings so the network is exercised without touching the state.
func failIfOpCountAtLeast(k int) Subject { return thresholdSubject{k: k} }

type thresholdSubject struct{ k int }

func (s thresholdSubject) NewNode(id string, _ int, peers []string) Node {
	return &thresholdNode{core: &pingCore{self: id, peers: peers}, k: s.k}
}

type thresholdNode struct {
	core   *pingCore
	k, ops int
}

func (n *thresholdNode) Core() artel.Core { return n.core }

func (n *thresholdNode) Apply(string) error { n.ops++; return nil }

func (n *thresholdNode) Observe() Observation {
	s := "calm"
	if n.ops >= n.k {
		s = "tripped:" + n.core.self
	}
	return Observation{Node: n.core.self, State: []byte(s), Value: s}
}

func stressProfile(maxOps int) Profile {
	return Profile{
		NodesMin: 2, NodesMax: 3, MaxOps: maxOps, MaxFaults: 2,
		OpGen:      func(*rand.Rand, int) string { return "noop" },
		FaultKinds: []FaultKind{FaultDrop, FaultDelay, FaultDup, FaultAckLost},
		Interval:   5, Horizon: 30, Settle: 10,
	}
}

// oracleNames is the set of oracles that fired, sorted.
func oracleNames(vs []Violation) []string {
	var out []string
	for _, v := range vs {
		if !slices.Contains(out, v.Oracle) {
			out = append(out, v.Oracle)
		}
	}
	slices.Sort(out)
	return out
}

func childSeeds(fs []Failure) []uint64 {
	out := make([]uint64, len(fs))
	for i, f := range fs {
		out[i] = f.ChildSeed
	}
	return out
}

// Every failure is a complete reproduction kit: the child seed regenerates
// the scenario, and running it again yields the same result, byte for byte.
func TestStressReproducesEveryFailure(t *testing.T) {
	p := stressProfile(3)
	sub := failIfOpCountAtLeast(0) // fails on every scenario with ≥2 nodes
	failures := Stress(11, 10, p, sub, Convergence())

	if len(failures) != 10 {
		t.Fatalf("%d failures, want 10: a subject that always fails must fail every child", len(failures))
	}
	seen := map[uint64]bool{}
	for _, f := range failures {
		if seen[f.ChildSeed] {
			t.Fatalf("child seed %d reported twice", f.ChildSeed)
		}
		seen[f.ChildSeed] = true

		if want := GenScenario(f.ChildSeed, p); !reflect.DeepEqual(f.Scenario, want) {
			t.Fatalf("seed %d: Failure.Scenario differs from GenScenario(seed, p):\n got  %+v\n want %+v", f.ChildSeed, f.Scenario, want)
		}
		if len(f.Result.Violations) == 0 {
			t.Fatalf("seed %d: a Failure with no violations", f.ChildSeed)
		}
		if again := Run(f.Scenario, sub, Convergence()); !reflect.DeepEqual(again, f.Result) {
			t.Fatalf("seed %d: rerunning the failure does not reproduce its result", f.ChildSeed)
		}
	}
}

func TestStressIsDeterministic(t *testing.T) {
	p := stressProfile(3)
	sub := failIfOpCountAtLeast(0)
	a := Stress(11, 8, p, sub, Convergence())
	b := Stress(11, 8, p, sub, Convergence())
	if !reflect.DeepEqual(a, b) {
		t.Fatal("two Stress runs with one master seed differ")
	}
	c := Stress(12, 8, p, sub, Convergence())
	if reflect.DeepEqual(childSeeds(a), childSeeds(c)) {
		t.Fatal("masters 11 and 12 derived the same child seeds")
	}
}

// Stress reports exactly the failing children — none invented, none dropped.
// The always-failing subject exposes every child of the same master, and the
// threshold subject's verdict on each is predictable: with k=1 a child fails
// iff it has at least one op.
func TestStressReportsExactlyTheFailingChildren(t *testing.T) {
	p := stressProfile(3)
	const master, n = 21, 30
	every := Stress(master, n, p, failIfOpCountAtLeast(0), Convergence())
	if len(every) != n {
		t.Fatalf("%d children exposed, want %d", len(every), n)
	}
	got := childSeeds(Stress(master, n, p, failIfOpCountAtLeast(1), Convergence()))

	var want []uint64
	for _, f := range every {
		if len(f.Scenario.Ops) >= 1 {
			want = append(want, f.ChildSeed)
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("failing children %v, want %v (children with ≥1 op)", got, want)
	}
	if len(want) == 0 || len(want) == n {
		t.Fatalf("%d of %d children fail: the mix does not exercise both outcomes, pick another master", len(want), n)
	}
}

func TestStressWithNothingToReport(t *testing.T) {
	p := stressProfile(3)
	if fs := Stress(11, 0, p, failIfOpCountAtLeast(0), Convergence()); len(fs) != 0 {
		t.Fatalf("n=0 reported %d failures", len(fs))
	}
	if fs := Stress(11, 10, p, failIfOpCountAtLeast(p.MaxOps+1), Convergence()); len(fs) != 0 {
		t.Fatalf("a subject that never trips reported %d failures: %+v", len(fs), fs)
	}
}

// requireRunnable runs the scenario and fails the test with the scenario in
// hand if Run rejects it: a shrinker must leave a valid scenario behind.
func requireRunnable(t *testing.T, s Scenario, sub Subject, oracles ...Oracle) (res Result) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("shrunk scenario is not valid: %v\n%+v", r, s)
		}
	}()
	return Run(s, sub, oracles...)
}

// shrinkScenario: four nodes, ten ops (five on n0, three on n1, two on n2),
// three fault windows that never touch the state. With k=3, n0 and n1 trip.
func shrinkScenario() Scenario {
	return Scenario{
		Seed: 5, Nodes: 4, Topology: FullMesh(4), Interval: 5, Horizon: 40, Settle: 10,
		Ops: []OpEntry{
			{At: 2, Node: 0, Op: "noop"}, {At: 4, Node: 0, Op: "noop"}, {At: 6, Node: 0, Op: "noop"},
			{At: 8, Node: 0, Op: "noop"}, {At: 10, Node: 0, Op: "noop"},
			{At: 12, Node: 1, Op: "noop"}, {At: 14, Node: 1, Op: "noop"}, {At: 16, Node: 1, Op: "noop"},
			{At: 18, Node: 2, Op: "noop"}, {At: 20, Node: 2, Op: "noop"},
		},
		Faults: []FaultEntry{
			{At: 0, Until: 10, Kind: FaultDrop, P: 0.5},
			{At: 10, Until: 20, Kind: FaultDup, P: 0.5},
			{At: 20, Until: 30, Kind: FaultDelay, MinD: 1, MaxD: 6},
		},
	}
}

// The failure is monotone in the ops (a node with ≥3 of them), so every
// minimizer that deletes until nothing more can go must arrive at the same
// place: exactly three ops on one node, no faults, the two nodes Convergence
// needs to disagree — and it must still fail the same oracle.
func TestShrinkMinimizesToTheThreshold(t *testing.T) {
	s := shrinkScenario()
	sub := failIfOpCountAtLeast(3)
	before := Run(s, sub, Convergence())
	if len(before.Violations) == 0 {
		t.Fatal("the scenario under test does not fail before shrinking")
	}

	shrunk, res := Shrink(s, sub, Convergence())

	if len(shrunk.Ops) != 3 {
		t.Fatalf("shrunk to %d ops, want 3: %+v", len(shrunk.Ops), shrunk.Ops)
	}
	for _, op := range shrunk.Ops {
		if op.Node != shrunk.Ops[0].Node {
			t.Fatalf("the surviving ops are spread over nodes, want all on one: %+v", shrunk.Ops)
		}
		// Node indexes are renumbered when a node is cut, so an op is
		// recognised by what and when, not by whose.
		if !slices.ContainsFunc(s.Ops, func(o OpEntry) bool { return o.At == op.At && o.Op == op.Op }) {
			t.Fatalf("shrunk op %+v is not one of the original ops", op)
		}
	}
	if len(shrunk.Faults) != 0 {
		t.Fatalf("%d faults survived, want 0 (none affects the failure): %+v", len(shrunk.Faults), shrunk.Faults)
	}
	if shrunk.Nodes != 2 {
		t.Fatalf("%d nodes survived, want 2 (Convergence needs a disagreeing pair)", shrunk.Nodes)
	}
	if shrunk.Seed != s.Seed || shrunk.Interval != s.Interval || shrunk.Settle != s.Settle || shrunk.Horizon > s.Horizon {
		t.Fatalf("shrinking changed the seed, interval or settle, or grew the horizon: %+v", shrunk)
	}

	if !reflect.DeepEqual(oracleNames(res.Violations), oracleNames(before.Violations)) {
		t.Fatalf("shrunk scenario fails %v, the original failed %v", oracleNames(res.Violations), oracleNames(before.Violations))
	}
	if again := requireRunnable(t, shrunk, sub, Convergence()); !reflect.DeepEqual(again, res) {
		t.Fatal("the returned Result is not the result of running the shrunk scenario")
	}
}

// A shrinker that only asks "does it still fail?" drifts into a different
// bug: cutting a node can disconnect the graph and make EventualDelivery fire
// for its own reasons. Here n2 is the hub between n0 and n1; the original
// fails Convergence only (pings relay through n2), and any mutation that
// changes the set of firing oracles must be refused.
func TestShrinkKeepsTheSameFailure(t *testing.T) {
	s := Scenario{
		Seed: 3, Nodes: 3, Topology: [][2]int{{0, 2}, {1, 2}}, Interval: 5, Horizon: 30, Settle: 15,
		Ops: []OpEntry{{At: 3, Node: 0, Op: "noop"}, {At: 7, Node: 0, Op: "noop"}},
	}
	sub := failIfOpCountAtLeast(2)
	oracles := []Oracle{Convergence(), EventualDelivery(OriginAlive, Relay)}

	before := Run(s, sub, oracles...)
	if want := []string{Convergence().Name()}; !reflect.DeepEqual(oracleNames(before.Violations), want) {
		t.Fatalf("precondition: the original must fail %v only, got %+v", want, before.Violations)
	}

	shrunk, res := Shrink(s, sub, oracles...)

	if !reflect.DeepEqual(oracleNames(res.Violations), oracleNames(before.Violations)) {
		t.Fatalf("shrunk scenario fails %v, the original failed %v: the shrinker drifted", oracleNames(res.Violations), oracleNames(before.Violations))
	}
	if !graphIsConnected(shrunk.Nodes, shrunk.Topology) {
		t.Fatalf("shrunk topology is disconnected: %d nodes, %v", shrunk.Nodes, shrunk.Topology)
	}
	if shrunk.Nodes < 2 || len(shrunk.Ops) != 2 {
		t.Fatalf("shrunk to %d nodes and %d ops, want ≥2 nodes and exactly the 2 ops the failure needs", shrunk.Nodes, len(shrunk.Ops))
	}
	requireRunnable(t, shrunk, sub, oracles...)
}

// With only Convergence attached nothing but the shrinker itself stands
// between a cut and a trivial failure: on the path n0–n1–n2, cutting the
// middle leaves two islands that "never converge" for no reason of the
// subject's. A connected input must stay connected.
func TestShrinkRefusesToDisconnect(t *testing.T) {
	s := Scenario{Seed: 3, Nodes: 3, Topology: [][2]int{{0, 1}, {1, 2}}, Interval: 5, Horizon: 20, Settle: 10,
		Ops: []OpEntry{{At: 3, Node: 0, Op: "noop"}}}
	sub := failIfOpCountAtLeast(1)

	shrunk, res := Shrink(s, sub, Convergence())

	if !graphIsConnected(shrunk.Nodes, shrunk.Topology) {
		t.Fatalf("shrunk topology is disconnected: %d nodes, %v", shrunk.Nodes, shrunk.Topology)
	}
	if shrunk.Nodes != 2 || len(shrunk.Ops) != 1 {
		t.Fatalf("shrunk to %d nodes and %d ops, want 2 and 1", shrunk.Nodes, len(shrunk.Ops))
	}
	if want := []string{Convergence().Name()}; !reflect.DeepEqual(oracleNames(res.Violations), want) {
		t.Fatalf("shrunk scenario fails %v, want %v", oracleNames(res.Violations), want)
	}
	requireRunnable(t, shrunk, sub, Convergence())
}

func TestShrinkReturnsAPassingScenarioAsIs(t *testing.T) {
	s := shrinkScenario()
	sub := failIfOpCountAtLeast(100) // never trips

	shrunk, res := Shrink(s, sub, Convergence())
	if !reflect.DeepEqual(shrunk, s) {
		t.Fatalf("a passing scenario was changed:\n got  %+v\n want %+v", shrunk, s)
	}
	if len(res.Violations) != 0 || !reflect.DeepEqual(res, Run(s, sub, Convergence())) {
		t.Fatalf("the result of a passing scenario is not its plain Run result: %+v", res.Violations)
	}
}

// Nothing can be cut: the single op sits at the horizon, the only other node
// is what Convergence disagrees with, there is no settle and no fault.
func TestShrinkLeavesAMinimalScenarioAlone(t *testing.T) {
	s := Scenario{Seed: 1, Nodes: 2, Topology: FullMesh(2), Interval: 5, Horizon: 10, Settle: 0,
		Ops: []OpEntry{{At: 10, Node: 0, Op: "noop"}}}
	sub := failIfOpCountAtLeast(1)

	shrunk, res := Shrink(s, sub, Convergence())
	if !reflect.DeepEqual(shrunk, s) {
		t.Fatalf("a minimal scenario was changed:\n got  %+v\n want %+v", shrunk, s)
	}
	if !reflect.DeepEqual(res, Run(s, sub, Convergence())) {
		t.Fatal("the returned Result is not the result of running the scenario")
	}
}

func TestShrinkIsDeterministic(t *testing.T) {
	sub := failIfOpCountAtLeast(3)
	s1, r1 := Shrink(shrinkScenario(), sub, Convergence())
	s2, r2 := Shrink(shrinkScenario(), sub, Convergence())
	if !reflect.DeepEqual(s1, s2) || !reflect.DeepEqual(r1, r2) {
		t.Fatal("two Shrink runs over one scenario differ")
	}
}
