package counter

import "crdtlab/crdt"

type GCounter struct {
	id    string
	state GCounterState
}

type GCounterState struct {
	values map[string]uint64
}

var _ crdt.StateReplica[GCounterState] = (*GCounter)(nil)

func NewGCounter(id string) *GCounter {
	return &GCounter{
		id:    id,
		state: GCounterState{make(map[string]uint64)},
	}
}

func (g *GCounter) Merge(other GCounterState) {
	if other.values[g.id] > g.state.values[g.id] {
		panic("GCounter: Merge: other replica has a higher value for this replica's id")
	}
	for id, v := range other.values {
		g.state.values[id] = max(g.state.values[id], v)
	}
}

func (g *GCounter) IncrementBy(x uint64) {
	g.state.values[g.id] += x
}

func (g *GCounter) Increment() {
	g.IncrementBy(1)
}

func (g *GCounter) Value() (sum uint64) {
	for _, v := range g.state.values {
		sum += v
	}
	return
}

func (g *GCounter) State() GCounterState {
	return g.state
}
