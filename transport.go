package artel

import "context"

// Kind tags what a message asks the receiver to do.
type Kind uint8

const (
	// KindPush carries a serialized delta-state in Payload; the receiver merges
	// it. A full state is just the "extreme" delta, so a single push covers both
	// steady-state gossip (a small delta) and a state transfer (the whole state).
	KindPush Kind = iota

	// KindPull asks the receiver to send its full state back (Payload is empty).
	// It is the catch-up handshake a new or restarted node fires at its peers on
	// join; each peer answers with a push of its current state.
	KindPull
)

// Message is one gossip datagram. Payload is an OPAQUE serialized delta-state
// (a state type's MarshalBinary output) — transports never interpret it.
type Message struct {
	From    string
	Kind    Kind
	Payload []byte
}

// Handler consumes an inbound message. The engine registers one via Start, and
// the transport calls it once per received message.
type Handler func(ctx context.Context, m Message) error

// Transport is a node's link to its peers. It is the seam between the
// anti-entropy engine and the outside world: the engine is generic over []byte
// and never learns transport details. Implementations must tolerate Send being
// called from several goroutines at once (the gossip loop and an inbound
// handler answering a pull both send).
type Transport interface {
	// Send delivers m to the peer named peerID. Fire-and-forget: it does not
	// wait for or return an application reply (a pull's answer arrives later as a
	// separate inbound push). It returns an error only if the message could not
	// be handed off (unknown peer, network failure).
	Send(ctx context.Context, peerID string, m Message) error

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
