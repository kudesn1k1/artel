// Package crdt holds the replica contracts of the delta-state toolkit.
package crdt

// StateReplica is a CRDT replica. State returns its convergent state — the
// join-semilattice element that travels and merges — excluding replica-local
// machinery. Merge folds an incoming state into the receiver in
// place.
type StateReplica[S any] interface {
	State() S
	Merge(other S)
}
