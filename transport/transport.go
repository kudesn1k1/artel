// Package transport moves gossip messages between replica nodes. It is the seam
// between the anti-entropy engine and the outside world: the engine is generic
// over []byte and never learns transport details, so one engine runs unchanged
// over a real network (HTTP) or a synchronous in-memory switchboard (InProcess,
// used by the deterministic convergence tests).
package transport

// Kind tags what a message asks the receiver to do.
type Kind uint8

const (
	// Push carries a serialized delta-state in Payload; the receiver merges it.
	// A full state is just the "extreme" delta, so a single Push covers both
	// steady-state gossip (a small delta) and a state transfer (the whole state).
	Push Kind = iota

	// Pull asks the receiver to send its full state back (Payload is empty). It
	// is the catch-up handshake a new or restarted node fires at its peers on
	// join; each peer answers with a Push of its current state.
	Pull
)

// Message is one gossip datagram. Payload is an OPAQUE serialized delta-state
// (a state type's MarshalBinary output) — transport never interprets it.
type Message struct {
	From    string
	Kind    Kind
	Payload []byte
}

// Handler consumes an inbound message. The engine registers one via Serve, and
// the transport calls it once per received message.
type Handler func(Message) error

// Transport is a node's link to its peers. Implementations must tolerate Send
// being called from several goroutines at once (the gossip loop and an inbound
// handler answering a Pull both send).
type Transport interface {
	// Send delivers m to the peer named peerID. Fire-and-forget: it does not
	// wait for or return an application reply (a Pull's answer arrives later as a
	// separate inbound Push). It returns an error only if the message could not
	// be handed off (unknown peer, network failure).
	Send(peerID string, m Message) error

	ID() string

	// Peers lists the ids this node gossips to.
	Peers() []string

	// Serve begins delivering inbound messages to h. It is NON-blocking: it
	// returns once the node is ready to receive (e.g. the socket is bound), so
	// the caller does not need its own goroutine.
	Serve(h Handler) error

	// Close stops serving and releases resources.
	Close() error
}
