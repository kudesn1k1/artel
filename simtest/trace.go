package simtest

import (
	"slices"
)

// EventKind names one applied step of a simulation run.
type EventKind string

const (
	EventTick       EventKind = "tick"
	EventOp         EventKind = "op"
	EventSend       EventKind = "send"
	EventDeliver    EventKind = "deliver"
	EventDrop       EventKind = "drop"
	EventDup        EventKind = "dup"
	EventSendResult EventKind = "sendresult"
	EventObserve    EventKind = "observe"
)

// Event is one applied step. Field order is canonical: encoding/json marshals
// struct fields in declaration order, so the serialized stream is
// deterministic by construction (serialization lands with the JSONL writer).
//
// Seq is the ordinal of the event in the trace (0-based, monotone) — a
// stable identity that does not leak scheduler internals.
//
// Node and Peer are protocol ids ("n0".."n{k-1}", the stable "n%d" mapping of
// scenario indexes): the trace records protocol history, and the protocol
// addresses peers by string. Node is the node in whose history the event
// lives: the actor for tick/op/send, the receiver for deliver, the sender
// learning the outcome for sendresult, and the sender whose message the
// network dropped or duplicated for drop/dup (a verdict on a send keeps the
// send's shape). Peer is empty when the event has no counterparty.
type Event struct {
	T    Dur       `json:"t"`
	Seq  uint64    `json:"seq"`
	Kind EventKind `json:"kind"`
	Node string    `json:"node"`
	Peer string    `json:"peer,omitempty"`
	// MsgKind and Size describe a carried message without interpreting it:
	// the harness is CRDT-blind (D11), payload bytes are reproduced by the
	// seed, and size alone separates a full-state answer from a small delta.
	MsgKind string `json:"msg_kind,omitempty"` // artel kind on the wire: "push"/"pull"
	Size    int    `json:"size,omitempty"`     // payload bytes
	// Sent links a row about a message (deliver, drop, dup, sendresult) to the
	// send row it is about: that send's Seq. A send is never row 0 — every
	// trace opens with a tick — so zero means "not about a message".
	Sent uint64 `json:"sent,omitempty"`
	Op   string `json:"op,omitempty"`
	Err  string `json:"err,omitempty"`
}

// Trace is the append-only log of one run: the replay artifact, the oracle
// input and the future timeline export are all this one object.
type Trace struct {
	Events []Event
}

func (t *Trace) add(e Event) int {
	e.Seq = uint64(len(t.Events))
	t.Events = append(t.Events, e)
	return len(t.Events) - 1
}

// History distils the trace for oracles: the nodes (one observe row each),
// the accepted ops and every delivery with its send link. Crashed is empty —
// the DES has no crash events yet.
func (t Trace) History() History {
	nodesMap := make(map[string]struct{})
	ops := make([]Op, 0, len(t.Events))
	dels := make([]Delivery, 0, len(t.Events))

	for _, ev := range t.Events {
		if ev.Kind == EventObserve {
			nodesMap[ev.Node] = struct{}{}
		}

		if ev.Kind == EventOp && ev.Err == "" {
			ops = append(ops, Op{T: ev.T, Seq: ev.Seq, Node: ev.Node, Op: ev.Op})
		}
		if ev.Kind == EventDeliver {
			dels = append(dels, Delivery{T: ev.T, Seq: ev.Seq, Sent: ev.Sent, From: ev.Peer, To: ev.Node, Kind: ev.MsgKind})
		}
	}

	nodes := make([]string, 0, len(nodesMap))
	for n := range nodesMap {
		nodes = append(nodes, n)
	}
	slices.Sort(nodes)

	return History{
		Nodes:      nodes,
		Ops:        ops,
		Deliveries: dels,
		Crashed:    nil, // TODO: set crashed nodes, empty in v0.2
	}
}
