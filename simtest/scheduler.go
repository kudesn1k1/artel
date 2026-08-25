package simtest

import (
	"container/heap"
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

const drainCap = 10_000

func Run(s Scenario, sub Subject) Result {
	validateScenario(s)

	res := Result{
		Trace: Trace{
			Events: []Event{},
		},
		Final:      []Observation{},
		Violations: []Violation{},
	}

	g := buildNodeGraph(s)
	nodes := make(map[string]Node, s.Nodes)
	ids := make([]string, s.Nodes)
	for i := range s.Nodes {
		id := nodeID(i)
		nodes[id] = sub.NewNode(id, 1, toPeers(g[i]))
		ids[i] = id
	}

	q := &eventHeap{}
	heap.Init(q)
	now := Dur(0)
	endOfTime := s.Horizon + s.Settle
	seq := uint64(0)

	for _, i := range ids {
		heap.Push(q, queuedEvent{
			at:   now,
			seq:  seq,
			kind: EventTick,
			node: i,
		})
		seq++
	}

	for _, op := range s.Ops {
		heap.Push(q, queuedEvent{
			at:   op.At,
			seq:  seq,
			kind: EventOp,
			node: ids[op.Node],
			op:   op,
		})
		seq++
	}

	emitSend := func(at Dur, from string, envs []artel.Envelope) {
		for _, envelope := range envs {
			res.Trace.add(toEvent(queuedEvent{
				at:   at,
				kind: EventSend,
				node: from,
				peer: envelope.To,
				msg:  envelope.Msg,
			}))

			heap.Push(q, queuedEvent{
				at:   at + 1, //TODO: anomalies handling
				seq:  seq,
				kind: EventDeliver,
				node: envelope.To,
				peer: from,
				msg:  envelope.Msg,
			})
			seq++
		}
	}

	drain := 0
	for q.Len() > 0 {
		queued := heap.Pop(q).(queuedEvent)
		now = queued.at

		if now > endOfTime {
			drain++
			if drain > drainCap {
				panic("simtest: core does not settle")
			}
		}

		eventIdx := res.Trace.add(toEvent(queued))

		switch queued.kind {
		case EventTick:
			envelopes := nodes[queued.node].Core().Tick()
			emitSend(now, queued.node, envelopes)

			if now+s.Interval < endOfTime {
				heap.Push(q, queuedEvent{
					at:   now + s.Interval,
					seq:  seq,
					kind: EventTick,
					node: queued.node,
				})
				seq++
			}
		case EventDeliver:
			envelopes := nodes[queued.node].Core().Deliver(queued.msg)
			emitSend(now, queued.node, envelopes)

			heap.Push(q, queuedEvent{
				at:   now,
				seq:  seq,
				kind: EventSendResult,
				node: queued.peer,
				peer: queued.node,
				msg:  queued.msg,
			})
			seq++
		case EventSendResult:
			nodes[queued.node].Core().SendResult(queued.peer, queued.msg.Kind, nil)
		case EventOp:
			err := nodes[queued.node].Apply(queued.op.Op)
			if err != nil {
				res.Trace.Events[eventIdx].Err = err.Error()
			}
		}
	}

	finishTime := max(endOfTime, now)
	for _, id := range ids {
		res.Final = append(res.Final, nodes[id].Observe())
		res.Trace.add(Event{
			T:    finishTime,
			Kind: EventObserve,
			Node: id,
		})
	}
	return res
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

func toEvent(q queuedEvent) Event {
	e := Event{
		T:    q.at,
		Kind: q.kind,
		Node: q.node,
	}

	switch q.kind {
	case EventOp:
		e.Op = q.op.Op
	case EventSend, EventDeliver, EventSendResult:
		e.Peer = q.peer
		e.MsgKind = q.msg.Kind.String()
		e.Size = len(q.msg.Payload)
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

type queuedEvent struct {
	at   Dur
	seq  uint64
	kind EventKind
	node string
	peer string
	msg  artel.Message
	op   OpEntry
	err  error
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
