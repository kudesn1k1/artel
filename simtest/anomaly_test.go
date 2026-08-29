package simtest

import (
	"reflect"
	"testing"

	"github.com/kudesn1k1/artel"
)

// Trust tests for the anomaly policy (D11b): hand-computed traces over the
// zero-CRDT ping fixture. The tables are NORMATIVE — they pin the semantics:
//
//   - a fault window is half-open [At, Until) in SEND time: the fate of a
//     message is decided when the core emits it, never at delivery;
//   - network verdicts on a send are trace rows written right after the send
//     row, in the send's shape (Node = sender, Peer = receiver): "drop" = the
//     message is lost, "dup" = a second copy is queued;
//   - loss is fast-fail: the sender gets SendResult(err) at t+1;
//   - delay REPLACES the ideal latency: delivery at t+d, d drawn in [MinD, MaxD];
//   - dup: two deliver rows, one SendResult(ok);
//   - AckLost: delivered, yet SendResult(err) at delivery time;
//   - AckLie: "drop" row, no delivery, SendResult(ok) at t+1 (the lie);
//   - partition: Group versus the rest; crossing messages are dropped;
//   - every random choice comes from the scenario seed: one scenario ⇒ one
//     trace, another seed ⇒ another trace.

func at(events []Event, t Dur) []Event {
	var out []Event
	for _, e := range events {
		if e.T == t {
			out = append(out, e)
		}
	}
	return out
}

func ofKind(events []Event, k EventKind) []Event {
	var out []Event
	for _, e := range events {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return out
}

// shape strips Seq and collapses any error text to "<err>": the tables pin
// structure, not the policy's wording.
func shape(events []Event) []Event {
	out := make([]Event, len(events))
	for i, e := range events {
		e.Seq = 0
		if e.Err != "" {
			e.Err = "<err>"
		}
		out[i] = e
	}
	return out
}

func expectShape(t *testing.T, got, want []Event) {
	t.Helper()
	got = shape(got)
	for i := 0; i < len(got) && i < len(want); i++ {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("rows diverge at %d:\n got  %+v\n want %+v", i, got[i], want[i])
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%d rows, want %d (first %d match):\n got %+v", len(got), len(want), min(len(got), len(want)), got)
	}
}

func expectOutcomes(t *testing.T, sub *pingSubject, pings, oks, errs int) {
	t.Helper()
	for i, n := range sub.nodes {
		c := n.core
		if c.pings != pings || c.oks != oks || c.errs != errs {
			t.Fatalf("n%d: pings=%d oks=%d errs=%d, want pings=%d oks=%d errs=%d",
				i, c.pings, c.oks, c.errs, pings, oks, errs)
		}
	}
}

func TestDropWindowLosesMessagesAndFastFails(t *testing.T) {
	// Ticks at 0, 10, 20; only the t=10 pings fall inside [10, 20).
	s := Scenario{Seed: 1, Nodes: 2, Topology: FullMesh(2), Interval: 10, Horizon: 25,
		Faults: []FaultEntry{{At: 10, Until: 20, Kind: FaultDrop, P: 1}}}
	sub := &pingSubject{}
	res := Run(s, sub)

	expectShape(t, at(res.Trace.Events, 10), []Event{
		{T: 10, Kind: EventTick, Node: "n0"},
		{T: 10, Kind: EventSend, Node: "n0", Peer: "n1", MsgKind: "push"},
		{T: 10, Kind: EventDrop, Node: "n0", Peer: "n1", MsgKind: "push"},
		{T: 10, Kind: EventTick, Node: "n1"},
		{T: 10, Kind: EventSend, Node: "n1", Peer: "n0", MsgKind: "push"},
		{T: 10, Kind: EventDrop, Node: "n1", Peer: "n0", MsgKind: "push"},
	})
	expectShape(t, at(res.Trace.Events, 11), []Event{
		{T: 11, Kind: EventSendResult, Node: "n0", Peer: "n1", MsgKind: "push", Err: "<err>"},
		{T: 11, Kind: EventSendResult, Node: "n1", Peer: "n0", MsgKind: "push", Err: "<err>"},
	})
	// t=20 is outside the window: delivered as on the ideal network.
	if got := len(ofKind(at(res.Trace.Events, 21), EventDeliver)); got != 2 {
		t.Fatalf("%d deliveries at t=21, want 2 (the window is half-open)", got)
	}
	expectOutcomes(t, sub, 2, 2, 1)
}

func TestDelayShiftsDeliveryExactly(t *testing.T) {
	// Horizon 26, not 25: the last deliveries land at t=25 and must not share
	// the instant with the observe rows.
	s := Scenario{Seed: 1, Nodes: 2, Topology: FullMesh(2), Interval: 10, Horizon: 26,
		Faults: []FaultEntry{{At: 0, Until: 100, Kind: FaultDelay, MinD: 5, MaxD: 5}}}
	sub := &pingSubject{}
	res := Run(s, sub)

	for _, tick := range []Dur{0, 10, 20} {
		if rows := at(res.Trace.Events, tick+1); len(rows) != 0 {
			t.Fatalf("rows at t=%d: %+v — the ideal latency must be replaced, not added to", tick+1, rows)
		}
		expectShape(t, at(res.Trace.Events, tick+5), []Event{
			{T: tick + 5, Kind: EventDeliver, Node: "n1", Peer: "n0", MsgKind: "push"},
			{T: tick + 5, Kind: EventDeliver, Node: "n0", Peer: "n1", MsgKind: "push"},
			{T: tick + 5, Kind: EventSendResult, Node: "n0", Peer: "n1", MsgKind: "push"},
			{T: tick + 5, Kind: EventSendResult, Node: "n1", Peer: "n0", MsgKind: "push"},
		})
	}
	expectOutcomes(t, sub, 3, 3, 0)
}

// Jitter is reordering: with sized pings (Size = send ordinal, sent one per
// instant) a descent in Size along n1's deliveries is a message overtaking an
// earlier one. Asserted on the trace, not on probability: across 20 seeds at
// least one run must reorder, and every delivery must respect its bounds.
// Settle is 0 so ticks stop at Horizon (20 pings, sent at t=0..19); the late
// deliveries are handled by the drain phase.
func TestJitterReordersMessagesWithinBounds(t *testing.T) {
	reordered := false
	for seed := uint64(1); seed <= 20; seed++ {
		s := Scenario{Seed: seed, Nodes: 2, Topology: FullMesh(2), Interval: 1, Horizon: 20, Settle: 0,
			Faults: []FaultEntry{{At: 0, Until: 100, Kind: FaultDelay, MinD: 1, MaxD: 20}}}
		res := Run(s, &pingSubject{sized: true})

		var got []Event
		for _, e := range ofKind(res.Trace.Events, EventDeliver) {
			if e.Node == "n1" {
				got = append(got, e)
			}
		}
		if len(got) != 20 {
			t.Fatalf("seed %d: n1 received %d pings, want 20 (ticks at 0..19)", seed, len(got))
		}
		for i, e := range got {
			sentAt := Dur(e.Size - 1) // the k-th ping was sent at t=k-1
			if e.T < sentAt+1 || e.T > sentAt+20 {
				t.Fatalf("seed %d: ping %d delivered at t=%d, outside [%d, %d]", seed, e.Size, e.T, sentAt+1, sentAt+20)
			}
			if i > 0 && e.Size < got[i-1].Size {
				reordered = true
			}
		}
	}
	if !reordered {
		t.Fatal("jitter in [1, 20] over 20 back-to-back pings never reordered a pair across 20 seeds")
	}
}

func TestDupDeliversTwiceAndAcksOnce(t *testing.T) {
	s := Scenario{Seed: 1, Nodes: 2, Topology: FullMesh(2), Interval: 10, Horizon: 25,
		Faults: []FaultEntry{{At: 0, Until: 100, Kind: FaultDup, P: 1}}}
	sub := &pingSubject{}
	res := Run(s, sub)

	expectShape(t, at(res.Trace.Events, 0), []Event{
		{T: 0, Kind: EventTick, Node: "n0"},
		{T: 0, Kind: EventSend, Node: "n0", Peer: "n1", MsgKind: "push"},
		{T: 0, Kind: EventDup, Node: "n0", Peer: "n1", MsgKind: "push"},
		{T: 0, Kind: EventTick, Node: "n1"},
		{T: 0, Kind: EventSend, Node: "n1", Peer: "n0", MsgKind: "push"},
		{T: 0, Kind: EventDup, Node: "n1", Peer: "n0", MsgKind: "push"},
	})
	expectShape(t, at(res.Trace.Events, 1), []Event{
		{T: 1, Kind: EventDeliver, Node: "n1", Peer: "n0", MsgKind: "push"},
		{T: 1, Kind: EventDeliver, Node: "n1", Peer: "n0", MsgKind: "push"},
		{T: 1, Kind: EventDeliver, Node: "n0", Peer: "n1", MsgKind: "push"},
		{T: 1, Kind: EventDeliver, Node: "n0", Peer: "n1", MsgKind: "push"},
		{T: 1, Kind: EventSendResult, Node: "n0", Peer: "n1", MsgKind: "push"},
		{T: 1, Kind: EventSendResult, Node: "n1", Peer: "n0", MsgKind: "push"},
	})
	expectOutcomes(t, sub, 6, 3, 0)
}

func TestPartitionSplitsGroupFromTheRest(t *testing.T) {
	// n0 alone versus {n1, n2} during [10, 20): n0's links are cut both ways,
	// n1<->n2 keeps flowing; ticks at 0 and 20 are unaffected.
	s := Scenario{Seed: 1, Nodes: 3, Topology: FullMesh(3), Interval: 10, Horizon: 25,
		Faults: []FaultEntry{{At: 10, Until: 20, Kind: FaultPartition, Group: []int{0}}}}
	sub := &pingSubject{}
	res := Run(s, sub)

	expectShape(t, at(res.Trace.Events, 10), []Event{
		{T: 10, Kind: EventTick, Node: "n0"},
		{T: 10, Kind: EventSend, Node: "n0", Peer: "n1", MsgKind: "push"},
		{T: 10, Kind: EventDrop, Node: "n0", Peer: "n1", MsgKind: "push"},
		{T: 10, Kind: EventSend, Node: "n0", Peer: "n2", MsgKind: "push"},
		{T: 10, Kind: EventDrop, Node: "n0", Peer: "n2", MsgKind: "push"},
		{T: 10, Kind: EventTick, Node: "n1"},
		{T: 10, Kind: EventSend, Node: "n1", Peer: "n0", MsgKind: "push"},
		{T: 10, Kind: EventDrop, Node: "n1", Peer: "n0", MsgKind: "push"},
		{T: 10, Kind: EventSend, Node: "n1", Peer: "n2", MsgKind: "push"},
		{T: 10, Kind: EventTick, Node: "n2"},
		{T: 10, Kind: EventSend, Node: "n2", Peer: "n0", MsgKind: "push"},
		{T: 10, Kind: EventDrop, Node: "n2", Peer: "n0", MsgKind: "push"},
		{T: 10, Kind: EventSend, Node: "n2", Peer: "n1", MsgKind: "push"},
	})
	// t=11 interleaves by scheduling order: fast-fail errs were queued at send
	// time, each ok is queued by its delivery.
	expectShape(t, at(res.Trace.Events, 11), []Event{
		{T: 11, Kind: EventSendResult, Node: "n0", Peer: "n1", MsgKind: "push", Err: "<err>"},
		{T: 11, Kind: EventSendResult, Node: "n0", Peer: "n2", MsgKind: "push", Err: "<err>"},
		{T: 11, Kind: EventSendResult, Node: "n1", Peer: "n0", MsgKind: "push", Err: "<err>"},
		{T: 11, Kind: EventDeliver, Node: "n2", Peer: "n1", MsgKind: "push"},
		{T: 11, Kind: EventSendResult, Node: "n2", Peer: "n0", MsgKind: "push", Err: "<err>"},
		{T: 11, Kind: EventDeliver, Node: "n1", Peer: "n2", MsgKind: "push"},
		{T: 11, Kind: EventSendResult, Node: "n1", Peer: "n2", MsgKind: "push"},
		{T: 11, Kind: EventSendResult, Node: "n2", Peer: "n1", MsgKind: "push"},
	})
	want := []string{"pings:4 ops:0", "pings:5 ops:0", "pings:5 ops:0"}
	for i, o := range res.Final {
		if o.Human != want[i] {
			t.Fatalf("n%d observed %q, want %q", i, o.Human, want[i])
		}
	}
	for i, errs := range []int{2, 1, 1} {
		if got := sub.nodes[i].core.errs; got != errs {
			t.Fatalf("n%d saw %d failed sends, want %d", i, got, errs)
		}
	}
}

func TestAckLostDeliversButReportsFailure(t *testing.T) {
	s := Scenario{Seed: 1, Nodes: 2, Topology: FullMesh(2), Interval: 10, Horizon: 25,
		Faults: []FaultEntry{{At: 0, Until: 100, Kind: FaultAckLost, P: 1}}}
	sub := &pingSubject{}
	res := Run(s, sub)

	if drops := ofKind(res.Trace.Events, EventDrop); len(drops) != 0 {
		t.Fatalf("AckLost must not lose messages, got drops %+v", drops)
	}
	results := ofKind(res.Trace.Events, EventSendResult)
	if len(results) != 6 {
		t.Fatalf("%d sendresult rows, want 6", len(results))
	}
	for _, r := range results {
		if r.Err == "" {
			t.Fatalf("sendresult without an error under AckLost: %+v", r)
		}
	}
	expectOutcomes(t, sub, 3, 0, 3)
}

func TestAckLieReportsSuccessWithoutDelivering(t *testing.T) {
	s := Scenario{Seed: 1, Nodes: 2, Topology: FullMesh(2), Interval: 10, Horizon: 25,
		Faults: []FaultEntry{{At: 0, Until: 100, Kind: FaultAckLie, P: 1}}}
	sub := &pingSubject{}
	res := Run(s, sub)

	if delivers := ofKind(res.Trace.Events, EventDeliver); len(delivers) != 0 {
		t.Fatalf("AckLie must not deliver, got %+v", delivers)
	}
	if drops := ofKind(res.Trace.Events, EventDrop); len(drops) != 6 {
		t.Fatalf("%d drop rows, want 6 (the loss is still visible in the trace)", len(drops))
	}
	expectShape(t, at(res.Trace.Events, 1), []Event{
		{T: 1, Kind: EventSendResult, Node: "n0", Peer: "n1", MsgKind: "push"},
		{T: 1, Kind: EventSendResult, Node: "n1", Peer: "n0", MsgKind: "push"},
	})
	expectOutcomes(t, sub, 0, 3, 0)
}

func mixScenario(seed uint64) Scenario {
	return Scenario{
		Seed: seed, Nodes: 4, Topology: FullMesh(4), Interval: 3, Horizon: 60, Settle: 40,
		Ops: []OpEntry{{At: 4, Node: 2, Op: "noop"}, {At: 31, Node: 0, Op: "noop"}},
		Faults: []FaultEntry{
			{At: 0, Until: 20, Kind: FaultDrop, P: 0.3},
			{At: 5, Until: 15, Kind: FaultAckLie, P: 0.2},
			{At: 10, Until: 40, Kind: FaultDelay, MinD: 1, MaxD: 8},
			{At: 20, Until: 50, Kind: FaultDup, P: 0.5},
			{At: 30, Until: 45, Kind: FaultPartition, Group: []int{0, 1}},
			{At: 40, Until: 60, Kind: FaultAckLost, P: 0.3},
		},
	}
}

func TestAnomalyMixIsSeedDeterministic(t *testing.T) {
	a := Run(mixScenario(7), &pingSubject{})
	b := Run(mixScenario(7), &pingSubject{})
	if !reflect.DeepEqual(a.Trace.Events, b.Trace.Events) || !reflect.DeepEqual(a.Final, b.Final) {
		t.Fatal("two runs of one scenario under the full anomaly mix diverged")
	}
	requireMonotoneT(t, a.Trace.Events)

	c := Run(mixScenario(8), &pingSubject{})
	if reflect.DeepEqual(a.Trace.Events, c.Trace.Events) {
		t.Fatal("seeds 7 and 8 produced identical traces: the policy is not driven by the scenario seed")
	}
}

// A failed send is reported with the kind that was sent: the zero Kind is
// KindPush, so a sendresult event that forgets to carry the message would
// turn a failed pull into a failed push — both in the trace and in the core.
func TestFailedSendKeepsItsKind(t *testing.T) {
	s := Scenario{Seed: 1, Nodes: 2, Topology: FullMesh(2), Interval: 10, Horizon: 25,
		Faults: []FaultEntry{{At: 0, Until: 100, Kind: FaultDrop, P: 1}}}
	sub := &pingSubject{kind: artel.KindPull}
	res := Run(s, sub)

	for _, kind := range []EventKind{EventSend, EventDrop, EventSendResult} {
		rows := ofKind(res.Trace.Events, kind)
		if len(rows) != 6 {
			t.Fatalf("%d %s rows, want 6", len(rows), kind)
		}
		for _, r := range rows {
			if r.MsgKind != "pull" {
				t.Fatalf("%s row carries MsgKind %q, want \"pull\": %+v", kind, r.MsgKind, r)
			}
		}
	}
	for i, n := range sub.nodes {
		if len(n.core.resultKinds) != 3 {
			t.Fatalf("n%d got %d results, want 3", i, len(n.core.resultKinds))
		}
		for _, k := range n.core.resultKinds {
			if k != artel.KindPull {
				t.Fatalf("n%d: SendResult reported kind %v, want pull", i, k)
			}
		}
	}
}

// Two partitions with identical windows are two independent cuts, not one:
// isolating n0 and isolating n1 at the same time leaves no link at all.
func TestOverlappingPartitionsBothApply(t *testing.T) {
	s := Scenario{Seed: 1, Nodes: 3, Topology: FullMesh(3), Interval: 10, Horizon: 25,
		Faults: []FaultEntry{
			{At: 10, Until: 20, Kind: FaultPartition, Group: []int{0}},
			{At: 10, Until: 20, Kind: FaultPartition, Group: []int{1}},
		}}
	sub := &pingSubject{}
	res := Run(s, sub)

	if rows := ofKind(at(res.Trace.Events, 11), EventDeliver); len(rows) != 0 {
		t.Fatalf("deliveries at t=11 under two partitions: %+v (one cut overwrote the other?)", rows)
	}
	if drops := ofKind(at(res.Trace.Events, 10), EventDrop); len(drops) != 6 {
		t.Fatalf("%d drops at t=10, want 6 (every link is cut)", len(drops))
	}
	expectOutcomes(t, sub, 4, 4, 2) // pings from t=0 and t=20 only
}
