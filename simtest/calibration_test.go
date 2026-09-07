package simtest

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
)

// Calibration: the harness proves it can see. Positive — the reference core
// over the reference counters survives the full mix of legal anomalies with
// no oracle firing. Negative — every mutant is killed on a scenario the
// reference passes, by exactly the oracles its fault is visible to. A gate
// that stops firing means the harness went blind, not that a mutant got
// fixed.

func refOracles() []Oracle {
	return []Oracle{Convergence(), CounterSum(), EventualDelivery(OriginAlive, Direct)}
}

// refProfile bounds scenarios so that a correct core can finish. GenScenario
// draws send times and delays below Horizon, so the last delayed delivery is
// at 2·Horizon−2 at the latest; the ok it carries clears the in-flight slot,
// and one more tick before Horizon+Settle must ship what coalesced behind it.
// Settle = Horizon + 2·Interval leaves that tick. A shorter settle fails a
// correct core — the scenario's fault, not the subject's.
func refProfile() Profile {
	return Profile{
		NodesMin: 2, NodesMax: 4, MaxOps: 12, MaxFaults: 4,
		OpGen:      func(r *rand.Rand, _ int) string { return fmt.Sprintf("inc:%d", 1+r.IntN(5)) },
		FaultKinds: []FaultKind{FaultDrop, FaultDelay, FaultDup, FaultPartition, FaultAckLost},
		Interval:   5, Horizon: 40, Settle: 50,
	}
}

// pnProfile is refProfile with decrements in the mix.
func pnProfile() Profile {
	p := refProfile()
	p.OpGen = func(r *rand.Rand, _ int) string {
		if r.IntN(2) == 0 {
			return fmt.Sprintf("dec:%d", 1+r.IntN(5))
		}
		return fmt.Sprintf("inc:%d", 1+r.IntN(5))
	}
	return p
}

// calibrationScenario: three nodes, seven ops summing to 28, every legal
// anomaly in overlapping windows, all healed by Horizon.
func calibrationScenario() Scenario {
	return Scenario{
		Seed: 7, Nodes: 3, Topology: FullMesh(3), Interval: 5, Horizon: 40, Settle: 50,
		Ops: []OpEntry{
			{At: 1, Node: 0, Op: "inc:1"}, {At: 3, Node: 1, Op: "inc:2"}, {At: 8, Node: 2, Op: "inc:3"},
			{At: 12, Node: 0, Op: "inc:4"}, {At: 21, Node: 1, Op: "inc:5"}, {At: 33, Node: 2, Op: "inc:6"},
			{At: 40, Node: 0, Op: "inc:7"},
		},
		Faults: []FaultEntry{
			{At: 0, Until: 10, Kind: FaultDrop, P: 0.5},
			{At: 5, Until: 25, Kind: FaultDelay, MinD: 1, MaxD: 8},
			{At: 10, Until: 30, Kind: FaultDup, P: 0.5},
			{At: 15, Until: 25, Kind: FaultPartition, Group: []int{0}},
			{At: 25, Until: 40, Kind: FaultAckLost, P: 0.5},
		},
	}
}

var referenceSubjects = []struct {
	name    string
	subject Subject
	profile Profile
}{
	{"gcounter", gCounterSubject{}, refProfile()},
	{"pncounter", pnCounterSubject{}, pnProfile()},
}

func TestReferenceSurvivesTheAnomalyMix(t *testing.T) {
	s := calibrationScenario()
	for _, ref := range referenceSubjects {
		t.Run(ref.name, func(t *testing.T) {
			res := Run(s, ref.subject, refOracles()...)
			if len(res.Violations) != 0 {
				t.Fatalf("the reference core fails the mix scenario: %+v\n%s", res.Violations, res.Trace)
			}
			expectValues(t, res, "28", "28", "28")
			for _, kind := range []EventKind{EventDrop, EventDup} {
				if len(ofKind(res.Trace.Events, kind)) == 0 {
					t.Fatalf("no %s row in the trace: the mix did not exercise it, pick another seed", kind)
				}
			}
			RequireDeterministic(t, s, ref.subject)
		})
	}
}

func TestReferenceSurvivesStress(t *testing.T) {
	const n = 1000
	for _, ref := range referenceSubjects {
		t.Run(ref.name, func(t *testing.T) {
			failures := Stress(1, n, ref.profile, ref.subject, refOracles()...)
			if len(failures) == 0 {
				return
			}
			f := failures[0]
			shrunk, res := Shrink(f.Scenario, ref.subject, refOracles()...)
			t.Fatalf("%d of %d scenarios fail on the reference core; child seed %d shrinks to:\n%+v\n%+v\n%s",
				len(failures), n, f.ChildSeed, shrunk, res.Violations, res.Trace)
		})
	}
}

// gate runs one scenario on a reference subject and on a mutant of it. The
// reference must pass, else the scenario and not the mutant is at fault; the
// mutant must fail exactly the named oracles.
func gate(t *testing.T, s Scenario, ref, mutant Subject, want ...string) Result {
	t.Helper()
	if res := Run(s, ref, refOracles()...); len(res.Violations) != 0 {
		t.Fatalf("the reference fails the gate scenario, so it cannot judge a mutant: %+v\n%s", res.Violations, res.Trace)
	}
	res := Run(s, mutant, refOracles()...)
	if got := oracleNames(res.Violations); !reflect.DeepEqual(got, want) {
		t.Fatalf("mutant fails %v, want %v (a blind spot if fewer):\n%+v\n%s", got, want, res.Violations, res.Trace)
	}
	return res
}

func expectValues(t *testing.T, res Result, want ...string) {
	t.Helper()
	for i, o := range res.Final {
		if o.Value != want[i] {
			t.Fatalf("%s observes %s, want %s:\n%s", o.Node, o.Value, want[i], res.Trace)
		}
	}
}

func twoNodes(ops []OpEntry, faults ...FaultEntry) Scenario {
	return Scenario{Seed: 1, Nodes: 2, Topology: FullMesh(2), Interval: 5, Horizon: 20, Settle: 20, Ops: ops, Faults: faults}
}

// The reference core keeps one push in flight per link and sends the next
// only after an outcome, so per-link delivery is FIFO and the network alone
// cannot reorder what reaches the type. Non-commutativity shows in the retain
// path instead: the push at 5 is delivered at 12 with a lost ack, the op at 6
// has coalesced {n0:2} into pending by then, and rejoining the in-flight
// {n0:1} overwrites it. Tick 15 ships the stale value; n0 = 2, n1 = 1.
// Carriers existed for both ops, so the delivery oracle stays silent.
func TestGateNonCommutativeMerge(t *testing.T) {
	s := twoNodes(
		[]OpEntry{{At: 1, Node: 0, Op: "inc:1"}, {At: 6, Node: 0, Op: "inc:1"}},
		FaultEntry{At: 5, Until: 6, Kind: FaultAckLost, P: 1},
		FaultEntry{At: 5, Until: 6, Kind: FaultDelay, MinD: 7, MaxD: 7},
	)
	res := gate(t, s, gCounterSubject{}, mutantType(lwwMerge), Convergence().Name(), CounterSum().Name())
	expectValues(t, res, "2", "1")
}

// A merge that is not idempotent over-counts on every second arrival of the
// same delta: a duplicated push, or a resend after a lost ack.
func TestGateNonIdempotentMerge(t *testing.T) {
	ops := []OpEntry{{At: 1, Node: 0, Op: "inc:1"}}
	for _, c := range []struct {
		name  string
		fault FaultEntry
	}{
		{"dup", FaultEntry{At: 5, Until: 6, Kind: FaultDup, P: 1}},
		{"acklost", FaultEntry{At: 5, Until: 6, Kind: FaultAckLost, P: 1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := twoNodes(ops, c.fault)
			res := gate(t, s, gCounterSubject{}, mutantType(sumMerge), Convergence().Name(), CounterSum().Name())
			expectValues(t, res, "1", "2")
		})
	}
}

// A core that never retains loses the dropped push for good. With the origin
// quiet afterwards no carrier follows, so the delivery oracle fires as well.
func TestGateLeakyCore(t *testing.T) {
	s := twoNodes(
		[]OpEntry{{At: 1, Node: 0, Op: "inc:1"}},
		FaultEntry{At: 5, Until: 6, Kind: FaultDrop, P: 1},
	)
	res := gate(t, s, gCounterSubject{}, leaky(gCounterSubject{}),
		Convergence().Name(), CounterSum().Name(), EventualDelivery(OriginAlive, Direct).Name())
	expectValues(t, res, "1", "0")
}

// A core that never clears the in-flight slot after a failure starves the
// link: the second op is coalesced behind the stuck push and never ships.
func TestGateStuckCore(t *testing.T) {
	s := twoNodes(
		[]OpEntry{{At: 1, Node: 0, Op: "inc:1"}, {At: 7, Node: 0, Op: "inc:1"}},
		FaultEntry{At: 5, Until: 6, Kind: FaultDrop, P: 1},
	)
	res := gate(t, s, gCounterSubject{}, stuck(gCounterSubject{}),
		Convergence().Name(), CounterSum().Name(), EventualDelivery(OriginAlive, Direct).Name())
	expectValues(t, res, "2", "0")
}

// GCounter ships the absolute value of its key, so the next op's push
// subsumes a leaked one: the leak heals and no oracle can see it — a fact
// about the type, not a blind spot. Separating a leak from a stuck link needs
// a type whose later delta does not carry the earlier one.
func TestLeakOnAbsoluteDeltasSelfHeals(t *testing.T) {
	s := twoNodes(
		[]OpEntry{{At: 1, Node: 0, Op: "inc:1"}, {At: 7, Node: 0, Op: "inc:1"}},
		FaultEntry{At: 5, Until: 6, Kind: FaultDrop, P: 1},
	)
	res := Run(s, leaky(gCounterSubject{}), refOracles()...)
	if len(res.Violations) != 0 {
		t.Fatalf("the leak did not heal: %+v\n%s", res.Violations, res.Trace)
	}
	expectValues(t, res, "2", "2")
}

// PNCounter keeps increments and decrements apart, so a later dec does not
// carry an earlier inc: the leak stays visible while carriers still flow,
// which is what separates a leaking core from a stuck one. Leaky: n1 never
// learns the inc, applies the dec and reads -1 against n0's 1; convergence
// and the sum fire, delivery is silent because the dec's push reached n1 after
// both ops. Stuck: nothing reaches n1 at all, so delivery fires too.
func TestGateLeakVersusStuckOnIndependentDeltas(t *testing.T) {
	s := twoNodes(
		[]OpEntry{{At: 1, Node: 0, Op: "inc:2"}, {At: 7, Node: 0, Op: "dec:1"}},
		FaultEntry{At: 5, Until: 6, Kind: FaultDrop, P: 1},
	)
	t.Run("leaky", func(t *testing.T) {
		res := gate(t, s, pnCounterSubject{}, leaky(pnCounterSubject{}), Convergence().Name(), CounterSum().Name())
		expectValues(t, res, "1", "-1")
	})
	t.Run("stuck", func(t *testing.T) {
		res := gate(t, s, pnCounterSubject{}, stuck(pnCounterSubject{}),
			Convergence().Name(), CounterSum().Name(), EventualDelivery(OriginAlive, Direct).Name())
		expectValues(t, res, "1", "0")
	})
}

// Every mutant dies somewhere in a stress sweep of its anomaly: the gates
// above pin one hand-checked kill each, the sweep shows the kill is not an
// artifact of one scenario. The non-commutative merge under dup+delay dies
// only through a stale duplicate arriving after a newer push — the reporting
// copy fast, the other slow — the one network-side reorder the reference core
// lets through.
func TestStressKillsEveryMutant(t *testing.T) {
	profile := func(base Profile, kinds ...FaultKind) Profile {
		base.FaultKinds = kinds
		base.MaxFaults = 3
		return base
	}
	cases := []struct {
		name    string
		subject Subject
		profile Profile
	}{
		{"lww under dup+delay", mutantType(lwwMerge), profile(refProfile(), FaultDup, FaultDelay)},
		{"lww under acklost+delay", mutantType(lwwMerge), profile(refProfile(), FaultAckLost, FaultDelay)},
		{"sum under dup", mutantType(sumMerge), profile(refProfile(), FaultDup)},
		{"sum under acklost", mutantType(sumMerge), profile(refProfile(), FaultAckLost)},
		{"leaky under drop", leaky(gCounterSubject{}), profile(refProfile(), FaultDrop)},
		{"leaky under partition", leaky(gCounterSubject{}), profile(refProfile(), FaultPartition)},
		{"leaky under drop, pncounter", leaky(pnCounterSubject{}), profile(pnProfile(), FaultDrop)},
		{"stuck under drop", stuck(gCounterSubject{}), profile(refProfile(), FaultDrop)},
	}
	const n = 100
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if len(Stress(1, n, c.profile, c.subject, refOracles()...)) == 0 {
				t.Fatalf("no kill in %d scenarios: the harness cannot see this mutant", n)
			}
		})
	}
}
