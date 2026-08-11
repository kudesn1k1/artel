package counter

import "crdtlab/crdt"

type PNCounter struct {
	id    string
	state PNCounterState
}

type PNCounterState struct {
	inc map[string]uint64
	dec map[string]uint64
}

var _ crdt.StateReplica[PNCounterState] = (*PNCounter)(nil)

func NewPNCounter(id string) *PNCounter {
	return &PNCounter{
		id: id,
		state: PNCounterState{
			inc: make(map[string]uint64),
			dec: make(map[string]uint64),
		},
	}
}

func (p *PNCounter) Merge(other PNCounterState) {
	if other.inc[p.id] > p.state.inc[p.id] {
		panic("PNCounter: Merge: other replica has a higher inc value for this replica's id")
	}
	if other.dec[p.id] > p.state.dec[p.id] {
		panic("PNCounter: Merge: other replica has a higher dec value for this replica's id")
	}
	for id, v := range other.inc {
		p.state.inc[id] = max(p.state.inc[id], v)
	}
	for id, v := range other.dec {
		p.state.dec[id] = max(p.state.dec[id], v)
	}
}

func (p PNCounter) State() PNCounterState {
	return p.state
}

func (p *PNCounter) IncrementBy(x uint64) {
	p.state.inc[p.id] += x
}

func (p *PNCounter) Increment() {
	p.IncrementBy(1)
}

func (p *PNCounter) DecrementBy(x uint64) {
	p.state.dec[p.id] += x
}

func (p *PNCounter) Decrement() {
	p.DecrementBy(1)
}

func (p PNCounter) Value() (sum int64) {
	for _, v := range p.state.inc {
		sum += int64(v)
	}
	for _, v := range p.state.dec {
		sum -= int64(v)
	}
	return
}
