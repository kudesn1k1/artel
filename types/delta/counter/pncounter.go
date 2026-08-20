package counter

import (
	"encoding"
	"encoding/json"
	"maps"
	"sync"
)

import "github.com/kudesn1k1/artel/crdt"

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

var _ encoding.BinaryMarshaler = PNCounterState{}
var _ encoding.BinaryUnmarshaler = (*PNCounterState)(nil)

func (s PNCounterState) MarshalBinary() ([]byte, error) {
	return json.Marshal([]map[string]uint64{s.inc, s.dec})
}

func (s *PNCounterState) UnmarshalBinary(data []byte) error {
	v := make([]map[string]uint64, 2)
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	s.inc = v[0]
	s.dec = v[1]

	return nil
}

var _ crdt.DeltaState[PNCounterState] = (*PNCounterState)(nil)
var _ crdt.DeltaReplica[PNCounterState] = (*PNCounter)(nil)

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
