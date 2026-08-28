package simtest

import (
	"container/heap"
	"errors"
	"fmt"
	"slices"

	"github.com/kudesn1k1/artel"
)

type Result struct {
	Trace      Trace
	Final      []Observation
	Violations []Violation
}

type Violation struct {
	Oracle, Detail string
}

var (
	errDropped = errors.New("failed to deliver: dropped")
	errAckLost = errors.New("failed to ack: lost")
)

// drainCap bounds the events applied past Horizon+Settle: a core that keeps
// the network busy forever is a bug, not a slow settle.
const drainCap = 10_000

// Run simulates one scenario over a subject and returns the trace and the
// final observations. A malformed scenario panics: scenarios are a testing
// tool, and a bad one is a programmer error.
func Run(s Scenario, sub Subject) Result {
	validateScenario(s)
	r := newRun(s, sub)
	r.loop()
	return r.observe()
}

// run is the state of one simulation: the virtual clock, the event queue,
// the nodes and the trace being written. Everything that happens goes
// through the queue — a core is only ever called from step.
type run struct {
	s      Scenario
	end    Dur // Horizon+Settle: no tick is scheduled at or after it
	ids    []string
	nodes  map[string]Node
	policy faultPolicy
	q      eventHeap
	seq    uint64 // insertion counter: the tie-break at equal instants
	now    Dur
	trace  Trace
}

func newRun(s Scenario, sub Subject) *run {
	r := &run{
		s:      s,
		end:    s.Horizon + s.Settle,
		ids:    make([]string, s.Nodes),
		nodes:  make(map[string]Node, s.Nodes),
		policy: newFaultPolicy(s.Seed, s.Faults),
		trace:  Trace{Events: []Event{}},
	}
	g := buildNodeGraph(s)
	for i := range s.Nodes {
		id := nodeID(i)
		r.ids[i] = id
		r.nodes[id] = sub.NewNode(id, 1, toPeers(g[i]))
	}

	// Seed: the first tick of every node at t=0, then the scenario's ops.
	for _, id := range r.ids {
		r.push(queuedEvent{at: 0, kind: EventTick, node: id})
	}
	for _, op := range s.Ops {
		r.push(queuedEvent{at: op.At, kind: EventOp, node: r.ids[op.Node], op: op.Op})
	}
	return r
}

// push schedules an event, stamping it with the next insertion number.
func (r *run) push(e queuedEvent) {
	e.seq = r.seq
	r.seq++
	heap.Push(&r.q, e)
}

func (r *run) loop() {
	drained := 0
	for r.q.Len() > 0 {
		e := heap.Pop(&r.q).(queuedEvent)
		r.now = e.at
		if r.now > r.end {
			drained++
			if drained > drainCap {
				panic("simtest: core does not settle")
			}
		}
		r.step(e)
	}
}

// step applies one event: records its row, calls into the node and
// schedules the consequences.
func (r *run) step(e queuedEvent) {
	row := r.trace.add(toEvent(e))
	node := r.nodes[e.node]

	switch e.kind {
	case EventTick:
		r.send(e.node, node.Core().Tick())
		if r.now+r.s.Interval < r.end {
			r.push(queuedEvent{at: r.now + r.s.Interval, kind: EventTick, node: e.node})
		}
	case EventDeliver:
		r.send(e.node, node.Core().Deliver(e.msg))
		if e.reports {
			var err error
			if !e.acked {
				err = errAckLost
			}
			r.push(resultAt(r.now, e.peer, e.node, e.msg, err))
		}
	case EventSendResult:
		node.Core().SendResult(e.peer, e.msg.Kind, e.err)
	case EventOp:
		if err := node.Apply(e.op); err != nil {
			r.trace.Events[row].Err = err.Error()
		}
	}
}

// send records the envelopes a core emitted and hands each to the fault
// policy: a delivered copy becomes a deliver event; a lost one leaves a drop
// row and a fast-fail result at now+1.
func (r *run) send(from string, envs []artel.Envelope) {
	for _, env := range envs {
		r.trace.add(msgRow(EventSend, r.now, from, env.To, env.Msg))

		fates := r.policy.fate(from, env.To, r.now)
		if len(fates) > 1 {
			r.trace.add(msgRow(EventDup, r.now, from, env.To, env.Msg))
		}
		for i, f := range fates {
			if f.delivered {
				r.push(queuedEvent{
					at:      r.now + max(1, f.delay),
					kind:    EventDeliver,
					node:    env.To,
					peer:    from,
					msg:     env.Msg,
					acked:   f.acked,
					reports: i == 0, // one outcome per send, even when duplicated
				})
				continue
			}
			r.trace.add(msgRow(EventDrop, r.now, from, env.To, env.Msg))
			var err error
			if !f.acked {
				err = errDropped
			}
			r.push(resultAt(r.now+1, from, env.To, env.Msg, err))
		}
	}
}

// observe runs after the queue is empty: every node is observed at the latter
// of Horizon+Settle and the last applied event.
func (r *run) observe() Result {
	at := max(r.end, r.now)
	final := make([]Observation, 0, len(r.ids))
	for _, id := range r.ids {
		final = append(final, r.nodes[id].Observe())
		r.trace.add(Event{T: at, Kind: EventObserve, Node: id})
	}
	return Result{Trace: r.trace, Final: final, Violations: []Violation{}}
}

func validateScenario(s Scenario) {
	if s.Interval <= 0 {
		panic("simtest: scenario interval must be > 0")
	}
	if s.Nodes <= 0 {
		panic("simtest: scenario nodes must be > 0")
	}
	for _, op := range s.Ops {
		if op.Node < 0 || op.Node >= s.Nodes {
			panic("simtest: op node out of range")
		}
		if op.At > s.Horizon {
			panic("simtest: op at out of range")
		}
	}
	for _, e := range s.Topology {
		if e[0] >= s.Nodes || e[1] >= s.Nodes || e[0] < 0 || e[1] < 0 {
			panic("simtest: topology node out of range")
		}
	}
}

// queuedEvent is a scheduled step. It is a tagged union by kind: which of
// the fields below matter depends on it.
type queuedEvent struct {
	at   Dur
	seq  uint64
	kind EventKind
	node string // whose history the event lives in (see Event)

	// send, deliver, sendresult
	peer string
	msg  artel.Message

	// deliver
	acked   bool // the sender will learn ok (false: the ack is lost → err)
	reports bool // this copy reports the outcome (only the first of duplicates)

	// sendresult
	err error

	// op
	op string
}

// resultAt is the sender learning the outcome of one send.
func resultAt(at Dur, sender, peer string, msg artel.Message, err error) queuedEvent {
	return queuedEvent{at: at, kind: EventSendResult, node: sender, peer: peer, msg: msg, err: err}
}

// msgRow is a trace row about one message, seen from node's side.
func msgRow(kind EventKind, at Dur, node, peer string, msg artel.Message) Event {
	return Event{T: at, Kind: kind, Node: node, Peer: peer, MsgKind: msg.Kind.String(), Size: len(msg.Payload)}
}

// toEvent projects a queued event onto its trace row.
func toEvent(q queuedEvent) Event {
	var e Event
	switch q.kind {
	case EventSend, EventDeliver, EventSendResult:
		e = msgRow(q.kind, q.at, q.node, q.peer, q.msg)
	case EventOp:
		e = Event{T: q.at, Kind: q.kind, Node: q.node, Op: q.op}
	default:
		e = Event{T: q.at, Kind: q.kind, Node: q.node}
	}
	if q.err != nil {
		e.Err = q.err.Error()
	}
	return e
}

func buildNodeGraph(s Scenario) [][]int {
	g := make([][]int, s.Nodes)
	if s.Topology == nil {
		for i := range g {
			g[i] = make([]int, 0, s.Nodes-1)
			for j := range s.Nodes {
				if j != i {
					g[i] = append(g[i], j)
				}
			}
		}
	} else {
		for _, e := range s.Topology {
			a, b := e[0], e[1]
			g[a] = append(g[a], b)
			g[b] = append(g[b], a)
		}
	}

	for i := range g {
		slices.Sort(g[i])
	}

	return g
}

func toPeers(nodes []int) []string {
	peers := make([]string, len(nodes))
	for i, n := range nodes {
		peers[i] = nodeID(n)
	}
	return peers
}

func nodeID(node int) string {
	return fmt.Sprintf("n%d", node)
}

type eventHeap []queuedEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at == h[j].at {
		return h[i].seq < h[j].seq
	}
	return h[i].at < h[j].at
}
func (h eventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *eventHeap) Push(x any) {
	*h = append(*h, x.(queuedEvent))
}
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

var _ heap.Interface = (*eventHeap)(nil)
