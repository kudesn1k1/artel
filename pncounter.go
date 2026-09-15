package artel

import (
	"maps"
	"sync"
)

// PNCounter is a counter that grows and shrinks: every replica counts its own
// increments and decrements apart, and the value is their difference. It is
// safe for concurrent use.
type PNCounter struct {
	id    string
	state PNCounterState
	delta PNCounterState
	mutex sync.Mutex
}

// PNCounterState is the state of a PNCounter, an increment and a decrement
// count per replica; a delta is the same type holding only the replicas
// that changed.
type PNCounterState struct {
	inc map[string]uint64
	dec map[string]uint64
}

// IsBottom reports whether the state holds no counts.
func (s PNCounterState) IsBottom() bool {
	return len(s.inc) == 0 && len(s.dec) == 0
}

// PNCounterWire is the wire form of a PNCounterState: the increments and the
// decrements, one count per replica each, ordered by replica id.
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
	return JSONCodec(PNCounterState.Wire, PNCounterStateFromWire)
}

var _ DeltaState[PNCounterState] = PNCounterState{}
var _ DeltaReplica[PNCounterState] = (*PNCounter)(nil)

// NewPNCounter returns an empty counter owned by the replica id. Every
// replica needs its own id; a restarted replica should take a fresh one.
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

// Join merges two states into a new one, taking the higher count per replica
// on each side.
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

// Merge folds an incoming state or delta into the counter.
func (p *PNCounter) Merge(other PNCounterState) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.state = p.state.Join(other)
}

// State returns a snapshot of the full state.
func (p *PNCounter) State() PNCounterState {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return PNCounterState{
		inc: maps.Clone(p.state.inc),
		dec: maps.Clone(p.state.dec),
	}
}

// Delta returns the changes made locally since the last FlushDelta.
func (p *PNCounter) Delta() PNCounterState {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.delta // works assuming that delta is never mutated
}

// FlushDelta returns the changes made locally since the last call and starts
// collecting anew.
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

// IncrementBy adds x to the counter.
func (p *PNCounter) IncrementBy(x uint64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.state.inc[p.id] += x
	p.delta = p.delta.Join(PNCounterState{
		inc: map[string]uint64{p.id: p.state.inc[p.id]},
		dec: make(map[string]uint64, 0),
	}) // could do inline max but decided to make proper join of deltas
}

// Increment adds one to the counter.
func (p *PNCounter) Increment() {
	p.IncrementBy(1)
}

// DecrementBy subtracts x from the counter.
func (p *PNCounter) DecrementBy(x uint64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.state.dec[p.id] += x
	p.delta = p.delta.Join(PNCounterState{
		inc: make(map[string]uint64, 0),
		dec: map[string]uint64{p.id: p.state.dec[p.id]},
	}) // could do inline max but decided to make proper join of deltas
}

// Decrement subtracts one from the counter.
func (p *PNCounter) Decrement() {
	p.DecrementBy(1)
}

// Value returns the increments minus the decrements seen so far, over every
// replica.
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
