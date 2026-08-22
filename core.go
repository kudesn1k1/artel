package artel

// Envelope is an intent to send: a Core never performs I/O, it asks its
// runtime (shell) to.
type Envelope struct {
	To  string
	Msg Message
}

// Core is the sans-IO protocol state machine: single-threaded, no clocks,
// no goroutines. Shells (the production engine at v0.3, the simtest
// scheduler now) feed it events and execute the envelopes it returns.
type Core interface {
	Tick() []Envelope
	Deliver(msg Message) []Envelope

	// SendResult reports the outcome of one envelope. The envelope is not handed
	// back: a core keeps its own in-flight state. (to, kind) is unambiguous only
	// under at most one outstanding send per peer per kind.
	SendResult(to string, kind Kind, err error)
}
