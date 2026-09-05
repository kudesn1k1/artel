package simtest

import (
	"reflect"
	"strings"
	"testing"
)

// Trust tests for the oracles (D11b, "oracles without the sim"): every
// history and every final below is built by hand, every verdict is computed
// by hand. Convergence and CounterSum judge the outcome; EventualDelivery
// judges the dissemination pattern the subject claims — Direct: the origin
// pushes to every peer itself; Relay: a causal chain of pushes suffices. A
// send carries an update only if it was emitted after the sender learned it
// (row order, never virtual time), and a pull carries no state. Crashed
// lists the nodes dead at the END of the run: what they did while alive
// stands.

func obs(node, v string) Observation {
	return Observation{Node: node, State: []byte(v), Value: v}
}

// finals builds observations for nodes "n0".."n{k-1}", index-aligned with the
// rosters the tests use.
func finals(vs ...string) []Observation {
	out := make([]Observation, len(vs))
	for i, v := range vs {
		out[i] = obs(nodeID(i), v)
	}
	return out
}

func push(sent, seq uint64, from, to string) Delivery {
	return Delivery{Seq: seq, Sent: sent, From: from, To: to, Kind: "push"}
}

// requireViolations asserts the count and that every listed word is on some
// Detail (the wording is the implementation's; the facts are not).
func requireViolations(t *testing.T, got []Violation, wantN int, mentions ...string) {
	t.Helper()
	if len(got) != wantN {
		t.Fatalf("%d violations, want %d: %+v", len(got), wantN, got)
	}
	for _, word := range mentions {
		found := false
		for _, v := range got {
			if strings.Contains(v.Detail, word) {
				found = true
			}
		}
		if !found {
			t.Fatalf("no violation mentions %q: %+v", word, got)
		}
	}
}

func TestConvergence(t *testing.T) {
	three := History{Nodes: []string{"n0", "n1", "n2"}}
	o := Convergence()

	requireViolations(t, o.Check(three, finals("5", "5", "5")), 0)
	requireViolations(t, o.Check(three, finals("5", "4", "5")), 1, "n1", "4")

	t.Run("state decides, not value", func(t *testing.T) {
		// Two counters worth 5 with the contributions swapped: equal values,
		// different states — not converged.
		final := []Observation{
			{Node: "n0", State: []byte(`{"n0":3,"n1":2}`), Value: "5"},
			{Node: "n1", State: []byte(`{"n0":2,"n1":3}`), Value: "5"},
		}
		requireViolations(t, o.Check(History{Nodes: []string{"n0", "n1"}}, final), 1)
	})

	t.Run("a dead node is not compared", func(t *testing.T) {
		h := History{Nodes: []string{"n0", "n1", "n2"}, Crashed: []string{"n1"}}
		requireViolations(t, o.Check(h, finals("5", "garbage", "5")), 0)
	})

	t.Run("silent alone and on nothing", func(t *testing.T) {
		requireViolations(t, o.Check(History{Nodes: []string{"n0"}}, finals("5")), 0)
		requireViolations(t, o.Check(History{}, nil), 0)
	})
}

func TestCounterSum(t *testing.T) {
	two := []string{"n0", "n1"}
	ops := []Op{{Seq: 1, Node: "n0", Op: "inc:2"}, {Seq: 2, Node: "n1", Op: "inc:3"}}
	o := CounterSum()

	requireViolations(t, o.Check(History{Nodes: two, Ops: ops}, finals("5", "5")), 0)
	requireViolations(t, o.Check(History{Nodes: two, Ops: ops}, finals("5", "4")), 1, "n1", "4", "5")

	t.Run("dec is the family's other op", func(t *testing.T) {
		ops := []Op{{Seq: 1, Node: "n0", Op: "inc:5"}, {Seq: 2, Node: "n1", Op: "dec:2"}}
		requireViolations(t, o.Check(History{Nodes: two, Ops: ops}, finals("3", "3")), 0)
		requireViolations(t, o.Check(History{Nodes: two, Ops: ops}, finals("3", "5")), 1, "n1")
	})

	t.Run("no ops means zero", func(t *testing.T) {
		requireViolations(t, o.Check(History{Nodes: two}, finals("0", "0")), 0)
		requireViolations(t, o.Check(History{Nodes: two}, finals("0", "1")), 1, "n1")
	})

	t.Run("a dead node is not read", func(t *testing.T) {
		h := History{Nodes: two, Ops: ops, Crashed: []string{"n1"}}
		requireViolations(t, o.Check(h, finals("5", "")), 0)
	})

	t.Run("cannot vouch for what it cannot read", func(t *testing.T) {
		// A wrong attachment is reported, not hidden: an op outside the counter
		// vocabulary, or a value that is not a number.
		h := History{Nodes: two, Ops: []Op{{Seq: 1, Node: "n0", Op: "add:x"}}}
		requireViolations(t, o.Check(h, finals("0", "0")), 1, "add:x")
		requireViolations(t, o.Check(History{Nodes: two, Ops: ops}, finals("5", "five")), 1, "n1", "five")
	})

	requireViolations(t, o.Check(History{}, nil), 0)
}

// The delivery table: one op by n0 (row 1 unless stated), three nodes unless
// stated. want lists the nodes a verdict must name as missing, per
// strength and pattern; nil = silent.
type deliveryCase struct {
	name string
	h    History
	want map[Liveness]map[Pattern][]string
}

func both(direct, relay []string) map[Pattern][]string {
	return map[Pattern][]string{Direct: direct, Relay: relay}
}

func TestEventualDelivery(t *testing.T) {
	n3 := []string{"n0", "n1", "n2"}
	op := []Op{{Seq: 1, Node: "n0", Op: "inc:1"}}
	silent := map[Liveness]map[Pattern][]string{OriginAlive: both(nil, nil), AnySurvivor: both(nil, nil)}
	missing := func(ids ...string) map[Liveness]map[Pattern][]string {
		return map[Liveness]map[Pattern][]string{OriginAlive: both(ids, ids), AnySurvivor: both(ids, ids)}
	}

	cases := []deliveryCase{
		{
			name: "the origin pushes to everyone",
			h:    History{Nodes: n3, Ops: op, Deliveries: []Delivery{push(2, 3, "n0", "n1"), push(4, 5, "n0", "n2")}},
			want: silent,
		},
		{
			name: "a node that receives nothing is missing, even if it never appears in a row",
			h:    History{Nodes: n3, Ops: op, Deliveries: []Delivery{push(2, 3, "n0", "n1")}},
			want: missing("n2"),
		},
		{
			name: "a push sent before the op carries nothing, even when delivered after it",
			// rows: 2 = n0 sends to n2, 5 = the op, 7 = the delayed push lands on n2
			h: History{Nodes: n3, Ops: []Op{{Seq: 5, Node: "n0", Op: "inc:1"}},
				Deliveries: []Delivery{push(2, 7, "n0", "n2"), push(6, 8, "n0", "n1")}},
			want: missing("n2"),
		},
		{
			name: "a pull carries nothing",
			h: History{Nodes: n3, Ops: op, Deliveries: []Delivery{
				push(2, 3, "n0", "n1"),
				{Seq: 5, Sent: 4, From: "n0", To: "n2", Kind: "pull"},
			}},
			want: missing("n2"),
		},
		{
			name: "relay: a peer forwards after it learned",
			h:    History{Nodes: n3, Ops: op, Deliveries: []Delivery{push(2, 3, "n0", "n1"), push(4, 5, "n1", "n2")}},
			want: map[Liveness]map[Pattern][]string{OriginAlive: both([]string{"n2"}, nil), AnySurvivor: both([]string{"n2"}, nil)},
		},
		{
			name: "relay: a forward sent before the peer learned carries nothing",
			// rows: 2 = n0 sends, 3 = n1 sends, 4 = n1 receives, 5 = n2 receives
			h: History{Nodes: n3, Ops: op, Deliveries: []Delivery{
				{Seq: 4, Sent: 2, From: "n0", To: "n1", Kind: "push"},
				{Seq: 5, Sent: 3, From: "n1", To: "n2", Kind: "push"},
			}},
			want: missing("n2"),
		},
		{
			name: "relay: two hops",
			h: History{Nodes: []string{"n0", "n1", "n2", "n3"}, Ops: op, Deliveries: []Delivery{
				push(2, 3, "n0", "n1"), push(4, 5, "n1", "n2"), push(6, 7, "n2", "n3"),
			}},
			want: map[Liveness]map[Pattern][]string{OriginAlive: both([]string{"n2", "n3"}, nil), AnySurvivor: both([]string{"n2", "n3"}, nil)},
		},
		{
			name: "traffic from a node that has not learned is not delivery",
			h:    History{Nodes: n3, Ops: op, Deliveries: []Delivery{push(2, 3, "n1", "n2"), push(4, 5, "n1", "n0")}},
			want: missing("n1", "n2"),
		},
		{
			name: "dead origin reached one survivor: only AnySurvivor asks for the rest",
			h:    History{Nodes: n3, Ops: op, Crashed: []string{"n0"}, Deliveries: []Delivery{push(2, 3, "n0", "n1")}},
			want: map[Liveness]map[Pattern][]string{OriginAlive: both(nil, nil), AnySurvivor: both([]string{"n2"}, []string{"n2"})},
		},
		{
			name: "dead origin, the survivor relays: the direct pattern cannot fulfil AnySurvivor",
			h: History{Nodes: n3, Ops: op, Crashed: []string{"n0"}, Deliveries: []Delivery{
				push(2, 3, "n0", "n1"), push(4, 5, "n1", "n2"),
			}},
			want: map[Liveness]map[Pattern][]string{OriginAlive: both(nil, nil), AnySurvivor: both([]string{"n2"}, nil)},
		},
		{
			name: "dead origin that reached no survivor owes nothing",
			h:    History{Nodes: n3, Ops: op, Crashed: []string{"n0"}},
			want: silent,
		},
		{
			name: "a dead receiver is not owed",
			h:    History{Nodes: n3, Ops: op, Crashed: []string{"n2"}, Deliveries: []Delivery{push(2, 3, "n0", "n1")}},
			want: silent,
		},
		{
			name: "a relay that died at the end still relayed while alive",
			h: History{Nodes: n3, Ops: op, Crashed: []string{"n1"}, Deliveries: []Delivery{
				push(2, 3, "n0", "n1"), push(4, 5, "n1", "n2"),
			}},
			want: map[Liveness]map[Pattern][]string{OriginAlive: both([]string{"n2"}, nil), AnySurvivor: both([]string{"n2"}, nil)},
		},
		{
			name: "alone",
			h:    History{Nodes: []string{"n0"}, Ops: op},
			want: silent,
		},
		{
			name: "nothing",
			h:    History{},
			want: silent,
		},
	}

	for _, c := range cases {
		for strength, byPattern := range c.want {
			for pattern, miss := range byPattern {
				o := EventualDelivery(strength, pattern)
				t.Run(c.name+"/"+o.Name(), func(t *testing.T) {
					final := make([]Observation, len(c.h.Nodes))
					got := o.Check(c.h, final)
					if miss == nil {
						requireViolations(t, got, 0)
						return
					}
					// One verdict per op, naming the origin, the op and every
					// missing node.
					requireViolations(t, got, 1, append([]string{"n0", "inc:1"}, miss...)...)
				})
			}
		}
	}
}

func TestEventualDeliveryJudgesEachOp(t *testing.T) {
	// n0's op reaches everyone; n1's op (row 6) reaches n0 only — and n0's
	// push to n2 (row 4) predates n0 learning it (row 8), so even a relay
	// has no chain to n2.
	h := History{
		Nodes: []string{"n0", "n1", "n2"},
		Ops:   []Op{{Seq: 1, Node: "n0", Op: "inc:1"}, {Seq: 6, Node: "n1", Op: "inc:2"}},
		Deliveries: []Delivery{
			push(2, 3, "n0", "n1"), push(4, 5, "n0", "n2"),
			push(7, 8, "n1", "n0"),
		},
	}
	for _, strength := range []Liveness{OriginAlive, AnySurvivor} {
		for _, pattern := range []Pattern{Direct, Relay} {
			got := EventualDelivery(strength, pattern).Check(h, make([]Observation, 3))
			requireViolations(t, got, 1, "n1", "inc:2", "n2")
		}
	}
}

func TestEventualDeliveryNamesTellTheFlavoursApart(t *testing.T) {
	seen := map[string]bool{}
	for _, strength := range []Liveness{OriginAlive, AnySurvivor} {
		for _, pattern := range []Pattern{Direct, Relay} {
			name := EventualDelivery(strength, pattern).Name()
			if name == "" || seen[name] {
				t.Fatalf("EventualDelivery(%v, %v).Name() = %q: empty or not distinct", strength, pattern, name)
			}
			seen[name] = true
		}
	}
}

// History is the trace distilled for oracles: the node set from the observe
// rows, accepted ops (an op row with Err was refused by the subject), every
// deliver row with its send link, and nothing else — the scheduler's
// vocabulary stays in the trace.
func TestTraceHistory(t *testing.T) {
	tr := Trace{Events: []Event{
		{T: 0, Seq: 0, Kind: EventTick, Node: "n0"},
		{T: 0, Seq: 1, Kind: EventSend, Node: "n0", Peer: "n1", MsgKind: "push", Size: 3},
		{T: 1, Seq: 2, Kind: EventDeliver, Node: "n1", Peer: "n0", MsgKind: "push", Size: 3, Sent: 1},
		{T: 1, Seq: 3, Kind: EventSendResult, Node: "n0", Peer: "n1", MsgKind: "push", Sent: 1},
		{T: 2, Seq: 4, Kind: EventOp, Node: "n1", Op: "inc:2"},
		{T: 2, Seq: 5, Kind: EventOp, Node: "n0", Op: "inc:9", Err: "refused"},
		{T: 3, Seq: 6, Kind: EventSend, Node: "n1", Peer: "n0", MsgKind: "pull"},
		{T: 3, Seq: 7, Kind: EventDup, Node: "n1", Peer: "n0", MsgKind: "pull", Sent: 6},
		{T: 4, Seq: 8, Kind: EventDeliver, Node: "n0", Peer: "n1", MsgKind: "pull", Sent: 6},
		{T: 4, Seq: 9, Kind: EventDeliver, Node: "n0", Peer: "n1", MsgKind: "pull", Sent: 6},
		{T: 4, Seq: 10, Kind: EventSendResult, Node: "n1", Peer: "n0", MsgKind: "pull", Sent: 6, Err: "failed to ack: lost"},
		{T: 5, Seq: 11, Kind: EventObserve, Node: "n0"},
		{T: 5, Seq: 12, Kind: EventObserve, Node: "n1"},
	}}
	h := tr.History()

	if want := []string{"n0", "n1"}; !reflect.DeepEqual(h.Nodes, want) {
		t.Fatalf("Nodes = %v, want %v", h.Nodes, want)
	}
	if want := []Op{{T: 2, Seq: 4, Node: "n1", Op: "inc:2"}}; !reflect.DeepEqual(h.Ops, want) {
		t.Fatalf("Ops = %+v, want %+v (the refused op must be dropped)", h.Ops, want)
	}
	wantD := []Delivery{
		{T: 1, Seq: 2, Sent: 1, From: "n0", To: "n1", Kind: "push"},
		{T: 4, Seq: 8, Sent: 6, From: "n1", To: "n0", Kind: "pull"},
		{T: 4, Seq: 9, Sent: 6, From: "n1", To: "n0", Kind: "pull"},
	}
	if !reflect.DeepEqual(h.Deliveries, wantD) {
		t.Fatalf("Deliveries = %+v, want %+v", h.Deliveries, wantD)
	}
	if len(h.Crashed) != 0 {
		t.Fatalf("Crashed = %v, want none (the DES has no crash events)", h.Crashed)
	}

	empty := Trace{}.History()
	if len(empty.Nodes)+len(empty.Ops)+len(empty.Deliveries)+len(empty.Crashed) != 0 {
		t.Fatalf("an empty trace distils to %+v, want an empty history", empty)
	}
}

// stubOracle records what Run hands it and always reports one violation,
// naming itself as every oracle does: attribution has to survive a direct
// Check call, since a hand-built History never passes through Run.
type stubOracle struct {
	h     History
	final []Observation
}

func (s *stubOracle) Name() string { return "stub" }

func (s *stubOracle) Check(h History, final []Observation) []Violation {
	s.h, s.final = h, final
	return []Violation{{Oracle: s.Name(), Detail: "always"}}
}

func TestRunAttachesOracles(t *testing.T) {
	// Ticks at 0 and 10, deliveries at 1 and 11: both nodes count 2 pings, but
	// n1 alone applies an op — the ping states diverge, and Convergence sees
	// it without knowing what a ping is.
	s := Scenario{Seed: 1, Nodes: 2, Topology: FullMesh(2), Interval: 10, Horizon: 12,
		Ops: []OpEntry{{At: 5, Node: 1, Op: "noop"}}}
	stub := &stubOracle{}
	res := Run(s, &pingSubject{}, stub, Convergence())

	if want := []string{"n0", "n1"}; !reflect.DeepEqual(stub.h.Nodes, want) {
		t.Fatalf("the oracle saw Nodes %v, want %v", stub.h.Nodes, want)
	}
	if len(stub.h.Ops) != 1 || stub.h.Ops[0].Node != "n1" || stub.h.Ops[0].Op != "noop" || stub.h.Ops[0].T != 5 {
		t.Fatalf("the oracle saw Ops %+v, want n1's noop at t=5 in id space", stub.h.Ops)
	}
	if got, want := len(stub.h.Deliveries), len(ofKind(res.Trace.Events, EventDeliver)); got != want {
		t.Fatalf("the oracle saw %d deliveries, the trace has %d", got, want)
	}
	if !reflect.DeepEqual(stub.final, res.Final) {
		t.Fatalf("the oracle saw finals %+v, Run returned %+v", stub.final, res.Final)
	}

	if len(res.Violations) != 2 {
		t.Fatalf("Violations = %+v, want the stub's and Convergence's, in attachment order", res.Violations)
	}
	if v := res.Violations[0]; v.Oracle != "stub" || v.Detail != "always" {
		t.Fatalf("Violations[0] = %+v, want the stub's, carrying its name", v)
	}
	if v := res.Violations[1]; v.Oracle != Convergence().Name() || !strings.Contains(v.Detail, "n1") {
		t.Fatalf("Violations[1] = %+v, want Convergence naming n1", v)
	}
}
