package simtest

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/kudesn1k1/artel"
)

// Trust tests for the DES scheduler (D11b): a zero-CRDT fixture and
// hand-computed expected traces. The tables below are NORMATIVE — they pin
// the event semantics from the plan: first tick of every node at t=0 and
// every Interval after; ideal network (Task 3) delivers with a fixed delay
// of 1; a successful delivery schedules SendResult(ok) to the sender at the
// same instant (later seq); at equal instants events apply in scheduling
// order (node 0 before node 1, scenario ops in list order); after
// Horizon+Settle the queue is drained, then every node is observed.

// pingCore: on every Tick sends one ping (KindPush) to every peer; Deliver
// counts pings; SendResult only counts outcomes (a fire-and-forget core).
// With sized set, the k-th ping carries a k-byte payload, so Event.Size
// becomes a message identity in the trace (anomaly tests: reorder, bounds).
type pingCore struct {
	self  string
	peers []string
	sized bool
	kind  artel.Kind // what the pings are sent as (zero = KindPush)
	sent  int
	pings int
	oks   int
	errs  int
	// resultKinds records the Kind reported by every SendResult call, so a
	// test can check that a failed pull is reported as a pull.
	resultKinds []artel.Kind
}

func (c *pingCore) Tick() []artel.Envelope {
	out := make([]artel.Envelope, 0, len(c.peers))
	for _, p := range c.peers {
		msg := artel.Message{From: c.self, Kind: c.kind}
		if c.sized {
			c.sent++
			msg.Payload = make([]byte, c.sent)
		}
		out = append(out, artel.Envelope{To: p, Msg: msg})
	}
	return out
}

func (c *pingCore) Deliver(artel.Message) []artel.Envelope { c.pings++; return nil }

func (c *pingCore) SendResult(_ string, kind artel.Kind, err error) {
	c.resultKinds = append(c.resultKinds, kind)
	if err != nil {
		c.errs++
	} else {
		c.oks++
	}
}

type pingNode struct {
	core *pingCore
	ops  int
}

func (n *pingNode) Core() artel.Core { return n.core }

func (n *pingNode) Apply(string) error { n.ops++; return nil }

func (n *pingNode) Observe() Observation {
	s := fmt.Sprintf("pings:%d ops:%d", n.core.pings, n.ops)
	return Observation{State: []byte(s), Human: s}
}

// pingSubject keeps every node it creates (creation order, n0 first) so
// tests can read core counters after Run.
type pingSubject struct {
	sized bool
	kind  artel.Kind
	nodes []*pingNode
}

func (s *pingSubject) NewNode(id string, _ int, peers []string) Node {
	n := &pingNode{core: &pingCore{self: id, peers: peers, sized: s.sized, kind: s.kind}}
	s.nodes = append(s.nodes, n)
	return n
}

// expectTrace numbers the rows and compares, reporting the first divergence.
func expectTrace(t *testing.T, got []Event, want []Event) {
	t.Helper()
	for i := range want {
		want[i].Seq = uint64(i)
	}
	for i := 0; i < len(got) && i < len(want); i++ {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("trace diverges at row %d:\n got  %+v\n want %+v", i, got[i], want[i])
		}
	}
	if len(got) != len(want) {
		t.Fatalf("trace has %d events, want %d (first %d rows match)", len(got), len(want), min(len(got), len(want)))
	}
}

func TestRunTraceTableOnAnIdealNetwork(t *testing.T) {
	s := Scenario{Seed: 1, Nodes: 2, Interval: 10, Horizon: 25, Settle: 0}
	res := Run(s, &pingSubject{})

	var want []Event
	for _, tick := range []Dur{0, 10, 20} {
		want = append(want,
			Event{T: tick, Kind: EventTick, Node: "n0"},
			Event{T: tick, Kind: EventSend, Node: "n0", Peer: "n1", MsgKind: "push"},
			Event{T: tick, Kind: EventTick, Node: "n1"},
			Event{T: tick, Kind: EventSend, Node: "n1", Peer: "n0", MsgKind: "push"},
			// node n0's ping was scheduled first, so it is delivered first;
			// each delivery schedules the sender's ok at the same instant.
			Event{T: tick + 1, Kind: EventDeliver, Node: "n1", Peer: "n0", MsgKind: "push"},
			Event{T: tick + 1, Kind: EventDeliver, Node: "n0", Peer: "n1", MsgKind: "push"},
			Event{T: tick + 1, Kind: EventSendResult, Node: "n0", Peer: "n1", MsgKind: "push"},
			Event{T: tick + 1, Kind: EventSendResult, Node: "n1", Peer: "n0", MsgKind: "push"},
		)
	}
	want = append(want,
		Event{T: 25, Kind: EventObserve, Node: "n0"},
		Event{T: 25, Kind: EventObserve, Node: "n1"},
	)
	expectTrace(t, res.Trace.Events, want)

	if len(res.Final) != 2 {
		t.Fatalf("Final has %d observations, want 2", len(res.Final))
	}
	for i, o := range res.Final {
		if o.Human != "pings:3 ops:0" {
			t.Fatalf("node %d observed %q, want \"pings:3 ops:0\"", i, o.Human)
		}
	}
	if len(res.Violations) != 0 {
		t.Fatalf("no oracles were attached, yet Violations=%v", res.Violations)
	}
}

func TestRunIsDeterministic(t *testing.T) {
	s := Scenario{
		Seed:     9,
		Nodes:    3,
		Interval: 7,
		Horizon:  50,
		Settle:   20,
		Ops: []OpEntry{
			{At: 5, Node: 1, Op: "noop"},
			{At: 5, Node: 0, Op: "noop"}, // same instant: list order must hold
			{At: 33, Node: 2, Op: "noop"},
		},
	}
	a := Run(s, &pingSubject{})
	b := Run(s, &pingSubject{})
	if !reflect.DeepEqual(a.Trace.Events, b.Trace.Events) {
		t.Fatal("two runs of one scenario produced different traces")
	}
	if !reflect.DeepEqual(a.Final, b.Final) {
		t.Fatalf("two runs of one scenario observed different finals: %v vs %v", a.Final, b.Final)
	}
}

func TestRunAppliesOpsOnSchedule(t *testing.T) {
	s := Scenario{
		Seed:     1,
		Nodes:    2,
		Interval: 10,
		Horizon:  12,
		Settle:   0,
		Ops:      []OpEntry{{At: 5, Node: 1, Op: "noop"}},
	}
	res := Run(s, &pingSubject{})

	var ops []Event
	for _, e := range res.Trace.Events {
		if e.Kind == EventOp {
			ops = append(ops, e)
		}
	}
	want := Event{T: 5, Seq: ops[0].Seq, Kind: EventOp, Node: "n1", Op: "noop"}
	if len(ops) != 1 || !reflect.DeepEqual(ops[0], want) {
		t.Fatalf("op events %+v, want exactly one %+v", ops, want)
	}
	if res.Final[0].Human != "pings:2 ops:0" || res.Final[1].Human != "pings:2 ops:1" {
		t.Fatalf("finals %q / %q, want \"pings:2 ops:0\" / \"pings:2 ops:1\"",
			res.Final[0].Human, res.Final[1].Human)
	}
}

// Scenarios are a testing tool: a malformed one is a programmer error and
// panics with a message, mirroring GenScenario's profile validation.
func TestRunRejectsAnInvalidScenario(t *testing.T) {
	cases := map[string]Scenario{
		"op beyond horizon": {Seed: 1, Nodes: 2, Interval: 10, Horizon: 25, Ops: []OpEntry{{At: 30, Node: 0, Op: "noop"}}},
		// settle is a quiet window: ops end at Horizon, not at Horizon+Settle.
		"op during settle":       {Seed: 1, Nodes: 2, Interval: 10, Horizon: 25, Settle: 10, Ops: []OpEntry{{At: 30, Node: 0, Op: "noop"}}},
		"op node out of range":   {Seed: 1, Nodes: 2, Interval: 10, Horizon: 25, Ops: []OpEntry{{At: 5, Node: 5, Op: "noop"}}},
		"op node == Nodes":       {Seed: 1, Nodes: 2, Interval: 10, Horizon: 25, Ops: []OpEntry{{At: 5, Node: 2, Op: "noop"}}},
		"op node negative":       {Seed: 1, Nodes: 2, Interval: 10, Horizon: 25, Ops: []OpEntry{{At: 5, Node: -1, Op: "noop"}}},
		"topology node == Nodes": {Seed: 1, Nodes: 2, Interval: 10, Horizon: 25, Topology: [][2]int{{0, 2}}},
		"topology node negative": {Seed: 1, Nodes: 2, Interval: 10, Horizon: 25, Topology: [][2]int{{-1, 1}}},
		"no nodes":               {Seed: 1, Nodes: 0, Interval: 10, Horizon: 25},
		"no interval":            {Seed: 1, Nodes: 2, Interval: 0, Horizon: 25},
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("Run accepted a scenario with %s", name)
				}
			}()
			Run(s, &pingSubject{})
		})
	}
}

// echoCore answers every delivered ping with a ping back to its sender, up to
// budget replies (budget < 0 = unbounded). On an ideal network this is the
// only way to keep the queue busy past Horizon+Settle, which is exactly what
// the drain phase exists for.
type echoCore struct {
	pingCore
	budget int
}

func (c *echoCore) Deliver(m artel.Message) []artel.Envelope {
	c.pings++
	if c.budget == 0 {
		return nil
	}
	if c.budget > 0 {
		c.budget--
	}
	return []artel.Envelope{{To: m.From, Msg: artel.Message{From: c.self, Kind: artel.KindPush}}}
}

type echoNode struct{ core *echoCore }

func (n *echoNode) Core() artel.Core { return n.core }

func (n *echoNode) Apply(string) error { return nil }

func (n *echoNode) Observe() Observation {
	s := fmt.Sprintf("pings:%d ops:0", n.core.pings)
	return Observation{State: []byte(s), Human: s}
}

type echoSubject struct{ budget int }

func (e echoSubject) NewNode(id string, _ int, peers []string) Node {
	return &echoNode{core: &echoCore{pingCore: pingCore{self: id, peers: peers}, budget: e.budget}}
}

func requireMonotoneT(t *testing.T, events []Event) {
	t.Helper()
	for i := 1; i < len(events); i++ {
		if events[i].T < events[i-1].T {
			t.Fatalf("trace time goes backwards at row %d: %+v after %+v", i, events[i], events[i-1])
		}
	}
}

// Drain: a message chain that outlives Horizon+Settle is still applied
// (cutting the queue by time would turn a delay into a silent loss), no new
// ticks are scheduled, and observation happens after the last applied event.
func TestRunDrainsTheQueueAfterSettle(t *testing.T) {
	// One tick at t=0 (0+10 < 5 fails), then 8 replies per node: the last
	// deliveries land at t=9, well past Horizon+Settle=5.
	s := Scenario{Seed: 1, Nodes: 2, Interval: 10, Horizon: 5, Settle: 0}
	res := Run(s, echoSubject{budget: 8})

	ticks, late := 0, 0
	for _, e := range res.Trace.Events {
		switch {
		case e.Kind == EventTick:
			ticks++
		case e.Kind == EventDeliver && e.T > 5:
			late++
		}
	}
	if ticks != 2 {
		t.Fatalf("%d ticks, want 2 (one per node at t=0, none during drain)", ticks)
	}
	if late == 0 {
		t.Fatal("no delivery was applied after Horizon+Settle: the queue was cut, not drained")
	}
	requireMonotoneT(t, res.Trace.Events)

	n := len(res.Trace.Events)
	last := res.Trace.Events[n-2:]
	if last[0].Kind != EventObserve || last[1].Kind != EventObserve || last[0].T != 9 || last[1].T != 9 {
		t.Fatalf("trace must end with both observations at t=9 (last applied event), got %+v", last)
	}
	for i, o := range res.Final {
		if o.Human != "pings:9 ops:0" {
			t.Fatalf("node %d observed %q, want \"pings:9 ops:0\"", i, o.Human)
		}
	}
}

func TestRunPanicsWhenTheCoreNeverSettles(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("an unbounded ping-pong past Horizon+Settle must trip the drain cap")
		}
	}()
	Run(Scenario{Seed: 1, Nodes: 2, Interval: 10, Horizon: 5, Settle: 0}, echoSubject{budget: -1})
}
