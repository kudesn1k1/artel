// Package crdt is a scratch playground for state-based CRDTs plus a tiny
// deterministic convergence simulator. v0: sync is instantaneous and in-memory
// — no queue, no delivery delay, no wire (see docs/playground-v0-design.md).
package crdt

import "reflect"

// StateReplica is a CRDT replica. State returns its convergent state — the
// join-semilattice element that travels and merges — excluding replica-local
// machinery. Merge folds an incoming state into the receiver in
// place.
type StateReplica[S any] interface {
	State() S
	Merge(other S)
}

// Network is the v0 simulator: named replicas, all of one CRDT type. R is the
// replica type, S the state it ships.
type Network[R StateReplica[S], S any] struct {
	replicas map[string]R
}

func NewNetwork[R StateReplica[S], S any]() *Network[R, S] {
	return &Network[R, S]{replicas: make(map[string]R)}
}

func (n *Network[R, S]) Add(id string, r R) { n.replicas[id] = r }
func (n *Network[R, S]) Get(id string) R    { return n.replicas[id] }

// Sync delivers from's state into to. Instantaneous in v0.
func (n *Network[R, S]) Sync(from, to string) { n.replicas[to].Merge(n.replicas[from].State()) }

// SyncAll brings every replica up to the join of all — the final reconcile.
// Iteration order is unspecified; the result is the same regardless, which is
// exactly the commutativity/associativity the types must satisfy.
func (n *Network[R, S]) SyncAll() {
	var sink R
	first := true
	for _, r := range n.replicas {
		if first {
			sink, first = r, false
			continue
		}
		sink.Merge(r.State())
	}
	for _, r := range n.replicas {
		r.Merge(sink.State())
	}
}

// Converged reports whether all replicas hold an equal state. Works now that
// State drops the replica-local id/clock that broke the old whole-struct oracle.
func (n *Network[R, S]) Converged() bool {
	var first S
	seen := false
	for _, r := range n.replicas {
		s := r.State()
		if !seen {
			first, seen = s, true
			continue
		}
		if !reflect.DeepEqual(first, s) {
			return false
		}
	}
	return true
}
