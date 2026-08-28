package simtest

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
	Op      string `json:"op,omitempty"`
	Err     string `json:"err,omitempty"`
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
