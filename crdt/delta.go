package crdt

// DeltaReplica is a delta-state CRDT replica. On top of the full-state contract
// (State/Merge) it exposes the delta-group accumulated by local delta-mutations,
// so the anti-entropy layer can ship small deltas instead of the whole state.
//
// Merge is the join and serves BOTH incoming payloads: a full State (first
// contact / state transfer) and a Delta (steady-state gossip) are both elements
// of the same join-semilattice S, so a single Merge handles either.
type DeltaReplica[S DeltaState[S]] interface {
	StateReplica[S]

	ID() string
	// Delta returns the accumulated delta-group to disseminate: the join of
	// every delta-mutation applied since the last ResetDelta. It is an S, not a
	// list — successive mutations are joined into it (Algorithm 1: Di = Di ⊔ d),
	// so e.g. repeated increments collapse into a single entry.
	Delta() S

	// FlushDelta returns the delta and clears the delta buffer to bottom after a successful send
	// (Algorithm 1: Di = ⊥).
	FlushDelta() S
}

type DeltaState[S any] interface {
	Join(S) S
}
