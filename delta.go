package artel

// StateReplica is a CRDT replica. State returns its convergent state — the
// join-semilattice element that travels and merges — excluding replica-local
// machinery. Merge folds an incoming state into the receiver in
// place.
type StateReplica[S any] interface {
	State() S
	Merge(other S)
}

// DeltaReplica is a delta-state CRDT replica. On top of the full-state contract
// (State/Merge) it exposes the delta-group accumulated by local delta-mutations,
// so the anti-entropy layer can ship small deltas instead of the whole state.
//
// Merge is the join and serves BOTH incoming payloads: a full State (first
// contact / state transfer) and a Delta (steady-state gossip) are both elements
// of the same join-semilattice S, so a single Merge handles either.
type DeltaReplica[S DeltaState[S]] interface {
	StateReplica[S]

	// Delta returns the changes applied locally since the last FlushDelta,
	// joined into one state rather than kept as a list: repeated increments,
	// for instance, collapse into a single entry.
	Delta() S

	// FlushDelta returns those changes and starts collecting anew. The engine
	// calls it once per gossip round.
	FlushDelta() S
}

// DeltaState is the state of a delta-state CRDT: an element of a
// join-semilattice, so a full state and any delta of it are the same type and
// merge the same way.
type DeltaState[S any] interface {
	// Join merges two states into a new one. It is commutative, associative
	// and idempotent, and leaves both arguments as they were.
	Join(S) S

	// IsBottom reports whether the state holds nothing at all.
	IsBottom() bool
}
