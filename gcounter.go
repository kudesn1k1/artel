package artel

import (
	"maps"
	"sync"
)

// GCounter is a grow-only counter: every replica counts its own increments,
// and the value is their sum. It is safe for concurrent use.
type GCounter struct {
	id    string
	state GCounterState
	delta GCounterState
	mutex sync.Mutex
}

// GCounterState is the state of a GCounter, one count per replica; a delta
// is the same type holding only the replicas that changed.
type GCounterState struct {
	values map[string]uint64
}

// IsBottom reports whether the state holds no counts.
func (s GCounterState) IsBottom() bool {
	return len(s.values) == 0
}

// GCounterWire is the wire form of a GCounterState: one count per replica,
// ordered by replica id.
type GCounterWire struct {
	Counts []ReplicaCount `json:"counts,omitzero"`
}

// Wire returns the state's wire form.
func (s GCounterState) Wire() GCounterWire {
	return GCounterWire{Counts: sortedCounts(s.values)}
}

// GCounterStateFromWire rebuilds a state from its wire form.
func GCounterStateFromWire(w GCounterWire) GCounterState {
	return GCounterState{values: countsToMap(w.Counts)}
}

// GCounterJSON returns the JSON codec for GCounterState.
func GCounterJSON() Codec[GCounterState] {
	return JSONCodec(GCounterState.Wire, GCounterStateFromWire)
}

var _ DeltaState[GCounterState] = GCounterState{}
var _ DeltaReplica[GCounterState] = (*GCounter)(nil)

// NewGCounter returns an empty counter owned by the replica id. Every
// replica needs its own id; a restarted replica should take a fresh one.
func NewGCounter(id string) *GCounter {
	return &GCounter{
		id:    id,
		state: GCounterState{make(map[string]uint64)},
		delta: GCounterState{make(map[string]uint64)},
	}
}

// Join merges two states into a new one, taking the higher count per replica.
func (s GCounterState) Join(other GCounterState) GCounterState {
	//TODO: try to reduce allocations
	out := make(map[string]uint64, len(s.values)+len(other.values))
	maps.Copy(out, s.values)
	for k, v := range other.values {
		out[k] = max(s.values[k], v)
	}
	return GCounterState{out}
}

// Merge folds an incoming state or delta into the counter.
func (g *GCounter) Merge(other GCounterState) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.state = g.state.Join(other)
}

// State returns a snapshot of the full state.
func (g *GCounter) State() GCounterState {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	return GCounterState{maps.Clone(g.state.values)}
}

// Delta returns the increments made locally since the last FlushDelta.
func (g *GCounter) Delta() GCounterState {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	return g.delta // works assuming that delta is never mutated
}

// FlushDelta returns the increments made locally since the last call and
// starts collecting anew.
func (g *GCounter) FlushDelta() GCounterState {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	delta := g.delta // works assuming that delta is never mutated
	g.delta = GCounterState{make(map[string]uint64)}
	return delta
}

// IncrementBy adds x to the counter.
func (g *GCounter) IncrementBy(x uint64) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.state.values[g.id] += x
	g.delta = g.delta.Join(GCounterState{values: map[string]uint64{g.id: g.state.values[g.id]}}) // could just do max inline but decided to make proper join of deltas
}

// Increment adds one to the counter.
func (g *GCounter) Increment() {
	g.IncrementBy(1)
}

// Value returns the sum of every replica's increments seen so far.
func (g *GCounter) Value() (sum uint64) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	for _, v := range g.state.values {
		sum += v
	}
	return
}
