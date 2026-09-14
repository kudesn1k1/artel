package simtest

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
)

// Causal calibration: the delta OR-Set over the reference core. The set
// refuses a delta whose dots do not continue what it has seen from their
// origin and counts the refusal; the core is push-only and clears what was
// delivered, so a refused delta is never sent again. The tests below pin
// what the harness sees of that — which oracle fires, what each node reads,
// how many refusals each node counted — with every expectation computed by
// hand from the tick order and the fault windows. Ticks at one instant apply
// in node order, so at equal times n0's push leaves before n1's and lands
// first.
//
// Two claims are kept apart. Deltas that name only their origin's own dots
// arrive gap-free under this core, whatever the legal anomalies: that is the
// per-origin claim, and it holds. A delta that names another replica's dots
// can outrun the delta that introduced them, and the set refuses it: that is
// why the set is not on this protocol yet.

func setOracles() []Oracle {
	return []Oracle{Convergence(), EventualDelivery(OriginAlive, Direct)}
}

func expectOracles(t *testing.T, res Result, want ...string) {
	t.Helper()
	if got := oracleNames(res.Violations); !reflect.DeepEqual(got, want) {
		t.Fatalf("oracles fired: %v, want %v:\n%+v\n%s", got, want, res.Violations, res.Trace)
	}
}

func expectRejections(t *testing.T, sub *orsetSubject, want ...int) {
	t.Helper()
	if got := sub.rejections(); !slices.Equal(got, want) {
		t.Fatalf("refusals by node are %v, want %v", got, want)
	}
}

// n1 adds f and e and pushes both at t=5; a partition around n2 for that
// instant loses the copy to n2, and n0 receives it at t=6. n0 removes e at
// t=7: a delta that names e's dot alone, the second of n1's. At t=10 n0's
// push leaves before n1's retry and both land at t=11 in that order. n2
// meets the remove first, has no first dot of n1 to continue from, and
// refuses it; the retry then brings f and e. The remove was delivered, so n0
// clears it and never sends it again: n2 keeps e for good.
//
// A vector-only context turns the same remove into "all of n1 up to two".
// n2 accepts it, and when the retry arrives both dots read as already seen
// and deleted; n1 loses f to the same claim. Nothing was refused, and the
// harness sees only the divergence.
func TestORSetRefusesARemoveThatOutranItsAdd(t *testing.T) {
	s := Scenario{
		Seed: 1, Nodes: 3, Topology: FullMesh(3), Interval: 5, Horizon: 20, Settle: 20,
		Ops: []OpEntry{
			{At: 1, Node: 1, Op: "add:f"}, {At: 2, Node: 1, Op: "add:e"},
			{At: 7, Node: 0, Op: "rm:e"},
		},
		Faults: []FaultEntry{{At: 5, Until: 6, Kind: FaultPartition, Group: []int{2}}},
	}
	t.Run("exact context", func(t *testing.T) {
		sub := &orsetSubject{}
		res := Run(s, sub, setOracles()...)
		expectOracles(t, res, Convergence().Name())
		expectValues(t, res, "{f}", "{f}", "{e,f}")
		expectRejections(t, sub, 0, 0, 1)
	})
	t.Run("vector-only context", func(t *testing.T) {
		sub := &orsetSubject{codec: vectorOnlyJSON()}
		res := Run(s, sub, setOracles()...)
		expectOracles(t, res, Convergence().Name())
		expectValues(t, res, "{f}", "{}", "{}")
		expectRejections(t, sub, 0, 0, 0)
	})
}

// n0 adds f and pushes it at t=5. The push is lost, but n0 hears success —
// from a transport that lies, or from a core that reports every outcome as
// one — and clears it. n0 adds e at t=7; the push at t=10 carries e's dot
// alone, the second of n0's, and n1 has never seen the first: the set
// refuses it and stays empty. n0 pushed after both ops, so the delivery
// oracle is silent; Convergence is what shows the gap. A vector-only context
// accepts the same delta and n1 ends with e and no f: the gap is swallowed
// and only the divergence remains.
func TestORSetRefusesAcrossAGap(t *testing.T) {
	ops := []OpEntry{{At: 1, Node: 0, Op: "add:f"}, {At: 7, Node: 0, Op: "add:e"}}
	cases := []struct {
		name string
		s    Scenario
		wrap func(Subject) Subject
	}{
		{"a transport that lies", twoNodes(ops, FaultEntry{At: 5, Until: 6, Kind: FaultAckLie, P: 1}), nil},
		{"a core that forgets", twoNodes(ops, FaultEntry{At: 5, Until: 6, Kind: FaultDrop, P: 1}), leaky},
	}
	for _, c := range cases {
		subject := func(sub *orsetSubject) Subject {
			if c.wrap == nil {
				return sub
			}
			return c.wrap(sub)
		}
		t.Run(c.name+"/exact context", func(t *testing.T) {
			sub := &orsetSubject{}
			res := Run(c.s, subject(sub), setOracles()...)
			expectOracles(t, res, Convergence().Name())
			expectValues(t, res, "{e,f}", "{}")
			expectRejections(t, sub, 0, 1)
		})
		t.Run(c.name+"/vector-only context", func(t *testing.T) {
			sub := &orsetSubject{codec: vectorOnlyJSON()}
			res := Run(c.s, subject(sub), setOracles()...)
			expectOracles(t, res, Convergence().Name())
			expectValues(t, res, "{e,f}", "{e}")
			expectRejections(t, sub, 0, 0)
		})
	}
}

// No anomaly at all: n0 adds f and e, n1 receives both at t=6 and removes e
// at t=7, and the remove reaches n0 at t=11. With an exact context the
// remove names e's dot alone and n0 keeps f. A vector-only context claims
// n0's first dot as well, and n0's join deletes its own f: the origin loses
// an element nobody removed, on an ideal network.
func TestGateVectorOnlyContext(t *testing.T) {
	s := twoNodes([]OpEntry{
		{At: 1, Node: 0, Op: "add:f"}, {At: 2, Node: 0, Op: "add:e"},
		{At: 7, Node: 1, Op: "rm:e"},
	})
	ref := &orsetSubject{}
	res := Run(s, ref, setOracles()...)
	expectOracles(t, res)
	expectValues(t, res, "{f}", "{f}")
	expectRejections(t, ref, 0, 0)

	mutant := &orsetSubject{codec: vectorOnlyJSON()}
	res = Run(s, mutant, setOracles()...)
	expectOracles(t, res, Convergence().Name())
	expectValues(t, res, "{}", "{f}")
	expectRejections(t, mutant, 0, 0)
}

// Every node touches only its own elements, so every delta names only its
// origin's dots, and the anomaly mix of the counter calibration cannot open a
// gap: no refusal, no violation, one value everywhere. n0: a, b, then a
// removed; n1: c removed and added again; n2: d added and removed.
func TestORSetSurvivesTheAnomalyMixOnOwnElements(t *testing.T) {
	s := calibrationScenario()
	s.Ops = []OpEntry{
		{At: 1, Node: 0, Op: "add:a"}, {At: 3, Node: 1, Op: "add:c"}, {At: 8, Node: 2, Op: "add:d"},
		{At: 12, Node: 0, Op: "add:b"}, {At: 21, Node: 1, Op: "rm:c"}, {At: 33, Node: 2, Op: "rm:d"},
		{At: 35, Node: 1, Op: "add:c"}, {At: 40, Node: 0, Op: "rm:a"},
	}
	sub := &orsetSubject{}
	res := Run(s, sub, setOracles()...)
	expectOracles(t, res)
	expectValues(t, res, "{b,c}", "{b,c}", "{b,c}")
	expectRejections(t, sub, 0, 0, 0)
	for _, kind := range []EventKind{EventDrop, EventDup} {
		if len(ofKind(res.Trace.Events, kind)) == 0 {
			t.Fatalf("no %s row in the trace: the mix did not exercise it, pick another seed", kind)
		}
	}
	RequireDeterministic(t, s, &orsetSubject{})
}

// ownElementsProfile draws adds and removes of three elements per node, each
// named after the node that owns it.
func ownElementsProfile() Profile {
	p := refProfile()
	p.OpGen = func(r *rand.Rand, node int) string {
		e := fmt.Sprintf("n%d.%c", node, 'a'+rune(r.IntN(3)))
		if r.IntN(3) == 0 {
			return "rm:" + e
		}
		return "add:" + e
	}
	return p
}

// sharedElementsProfile draws adds and removes of two elements every node
// shares.
func sharedElementsProfile() Profile {
	p := refProfile()
	p.OpGen = func(r *rand.Rand, _ int) string {
		e := string(rune('a' + r.IntN(2)))
		if r.IntN(3) == 0 {
			return "rm:" + e
		}
		return "add:" + e
	}
	return p
}

// Per-origin delivery is gap-free under the reference core: over a thousand
// scenarios of legal anomalies, deltas that name only their origin's dots are
// never refused and every run converges.
func TestORSetOwnElementsNeverRefusedUnderStress(t *testing.T) {
	const n = 1000
	sub := &orsetSubject{}
	failures := Stress(1, n, ownElementsProfile(), sub, setOracles()...)
	if len(failures) != 0 {
		f := failures[0]
		shrunk, res := Shrink(f.Scenario, &orsetSubject{}, setOracles()...)
		t.Fatalf("%d of %d scenarios fail on own elements; child seed %d shrinks to:\n%+v\n%+v\n%s",
			len(failures), n, f.ChildSeed, shrunk, res.Violations, res.Trace)
	}
	if got := sub.rejected(); got != 0 {
		t.Fatalf("%d deltas refused across %d converging scenarios: a gap the oracles did not see", got, n)
	}
}

// With shared elements a remove or a re-add names dots of other replicas,
// and under the reference core such a delta can outrun the one that
// introduced them. The sweep pins two things: the race happens, and it is
// the only way this subject diverges — a run that fails Convergence always
// counted a refusal, and no other oracle ever fires. The converse does not
// hold: a refused remove can still be made good by a later remove of the
// same element that carries the missing dots.
func TestORSetDivergesOnlyByRefusal(t *testing.T) {
	const n = 300
	diverged := 0
	for seed := uint64(1); seed <= n; seed++ {
		s := GenScenario(seed, sharedElementsProfile())
		sub := &orsetSubject{}
		res := Run(s, sub, setOracles()...)
		if len(res.Violations) == 0 {
			continue
		}
		diverged++
		if got, want := oracleNames(res.Violations), []string{Convergence().Name()}; !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d: oracles fired %v, want %v:\n%+v\n%s", seed, got, want, res.Violations, res.Trace)
		}
		if sub.rejected() == 0 {
			t.Fatalf("seed %d diverges with no refusal:\n%+v\n%s", seed, res.Violations, res.Trace)
		}
	}
	if diverged == 0 {
		t.Fatalf("no divergence in %d scenarios: the race did not occur, widen the profile", n)
	}
}
