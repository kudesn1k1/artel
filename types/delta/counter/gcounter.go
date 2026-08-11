package counter

import (
	"encoding"
	"encoding/json"
	"maps"
	"sync"
)

import "crdtlab/crdt"

type GCounter struct {
	id    string
	state GCounterState
	delta GCounterState
	mutex sync.Mutex
}

type GCounterState struct {
	values map[string]uint64
}

func (s GCounterState) MarshalBinary() (data []byte, err error) {
	return json.Marshal(s.values)
}

func (s *GCounterState) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &s.values)
}

var _ encoding.BinaryMarshaler = GCounterState{}
var _ encoding.BinaryUnmarshaler = (*GCounterState)(nil)

var _ crdt.DeltaState[GCounterState] = (*GCounterState)(nil)
var _ crdt.DeltaReplica[GCounterState] = (*GCounter)(nil)

func NewGCounter(id string) *GCounter {
	return &GCounter{
		id:    id,
		state: GCounterState{make(map[string]uint64)},
		delta: GCounterState{make(map[string]uint64)},
	}
}

func (s GCounterState) Join(other GCounterState) GCounterState {
	//TODO: try to reduce allocations
	out := make(map[string]uint64, len(s.values)+len(other.values))
	maps.Copy(out, s.values)
	for k, v := range other.values {
		out[k] = max(s.values[k], v)
	}
	return GCounterState{out}
}

func (g *GCounter) Merge(other GCounterState) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.state = g.state.Join(other)
}

func (g *GCounter) State() GCounterState {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	return GCounterState{maps.Clone(g.state.values)}
}

func (g *GCounter) Delta() GCounterState {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	return g.delta // works assuming that delta is never mutated
}

func (g *GCounter) FlushDelta() GCounterState {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	delta := g.delta // works assuming that delta is never mutated
	g.delta = GCounterState{make(map[string]uint64)}
	return delta
}

func (g *GCounter) IncrementBy(x uint64) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.state.values[g.id] += x
	g.delta = g.delta.Join(GCounterState{values: map[string]uint64{g.id: g.state.values[g.id]}}) // could just do max inline but decided to make proper join of deltas
}

func (g *GCounter) Increment() {
	g.IncrementBy(1)
}

func (g *GCounter) Value() (sum uint64) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	for _, v := range g.state.values {
		sum += v
	}
	return
}
