package artel

import (
	"maps"
	"sync"
)

type PNCounter struct {
	id    string
	state PNCounterState
	delta PNCounterState
	mutex sync.Mutex
}

type PNCounterState struct {
	inc map[string]uint64
	dec map[string]uint64
}

func (s PNCounterState) IsBottom() bool {
	return len(s.inc) == 0 && len(s.dec) == 0
}

// PNCounterWire is the wire form of a PNCounterState: the increments and the
// decrements, one count per replica each, ordered by replica id. Codecs
// encode this, never the state itself.
type PNCounterWire struct {
	Inc []ReplicaCount `json:"inc,omitzero"`
	Dec []ReplicaCount `json:"dec,omitzero"`
}

// Wire returns the state's wire form.
func (s PNCounterState) Wire() PNCounterWire {
	return PNCounterWire{Inc: sortedCounts(s.inc), Dec: sortedCounts(s.dec)}
}

// PNCounterStateFromWire rebuilds a state from its wire form.
func PNCounterStateFromWire(w PNCounterWire) PNCounterState {
	return PNCounterState{inc: countsToMap(w.Inc), dec: countsToMap(w.Dec)}
}

// PNCounterJSON returns the JSON codec for PNCounterState.
func PNCounterJSON() Codec[PNCounterState] {
	return JSON(PNCounterState.Wire, PNCounterStateFromWire)
}

var _ DeltaState[PNCounterState] = PNCounterState{}
var _ DeltaReplica[PNCounterState] = (*PNCounter)(nil)

func NewPNCounter(id string) *PNCounter {
	return &PNCounter{
		id: id,
		state: PNCounterState{
			inc: make(map[string]uint64),
			dec: make(map[string]uint64),
		},
		delta: PNCounterState{
			inc: make(map[string]uint64),
			dec: make(map[string]uint64),
		},
	}
}

func (s PNCounterState) Join(other PNCounterState) PNCounterState {
	out := PNCounterState{
		inc: make(map[string]uint64, len(s.inc)+len(other.inc)),
		dec: make(map[string]uint64, len(s.dec)+len(other.dec)),
	}

	maps.Copy(out.inc, s.inc)
	maps.Copy(out.dec, s.dec)

	for k, v := range other.inc {
		out.inc[k] = max(out.inc[k], v)
	}
	for k, v := range other.dec {
		out.dec[k] = max(out.dec[k], v)
	}

	return out
}

func (p *PNCounter) Merge(other PNCounterState) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.state = p.state.Join(other)
}

func (p *PNCounter) State() PNCounterState {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return PNCounterState{
		inc: maps.Clone(p.state.inc),
		dec: maps.Clone(p.state.dec),
	}
}

func (p *PNCounter) Delta() PNCounterState {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.delta // works assuming that delta is never mutated
}

func (p *PNCounter) FlushDelta() PNCounterState {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	delta := p.delta // works assuming that delta is never mutated
	p.delta = PNCounterState{
		inc: make(map[string]uint64),
		dec: make(map[string]uint64),
	}
	return delta
}

func (p *PNCounter) IncrementBy(x uint64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.state.inc[p.id] += x
	p.delta = p.delta.Join(PNCounterState{
		inc: map[string]uint64{p.id: p.state.inc[p.id]},
		dec: make(map[string]uint64, 0),
	}) // could do inline max but decided to make proper join of deltas
}

func (p *PNCounter) Increment() {
	p.IncrementBy(1)
}

func (p *PNCounter) DecrementBy(x uint64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.state.dec[p.id] += x
	p.delta = p.delta.Join(PNCounterState{
		inc: make(map[string]uint64, 0),
		dec: map[string]uint64{p.id: p.state.dec[p.id]},
	}) // could do inline max but decided to make proper join of deltas
}

func (p *PNCounter) Decrement() {
	p.DecrementBy(1)
}

func (p *PNCounter) Value() (sum int64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, v := range p.state.inc {
		sum += int64(v)
	}
	for _, v := range p.state.dec {
		sum -= int64(v)
	}
	return
}
