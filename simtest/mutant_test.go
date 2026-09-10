package simtest

import (
	"fmt"
	"maps"
	"strconv"

	"github.com/kudesn1k1/artel"
)

// Mutants are known-broken subjects for calibration: each seeds one fault the
// harness must be able to see, and a mutant the oracles do not kill is a
// blind spot, not a passing test. Two break the type's merge and two break the
// core's in-flight accounting; the reference protocol and the reference types
// are never edited.

// mutantMerge is the lattice law a mutant counter breaks. The zero value
// belongs to a bottom, which has no rule of its own and adopts its partner's.
type mutantMerge int

const (
	noMerge mutantMerge = iota
	// lwwMerge overwrites per key with the incoming value: idempotent and
	// associative but not commutative. It converges under in-order delivery
	// and breaks when an older value is merged after a newer one.
	lwwMerge
	// sumMerge adds per key and ships relative deltas: commutative and
	// associative but not idempotent. It is exact under exactly-once delivery
	// and over-counts when a delta arrives twice.
	sumMerge
)

type mutantState struct {
	merge mutantMerge
	v     map[string]uint64
}

var _ artel.DeltaState[mutantState] = mutantState{}

func (s mutantState) IsBottom() bool { return len(s.v) == 0 }

// mutantJSON encodes the counts alone: the merge rule is not state, and a
// decoded bottom adopts its partner's.
func mutantJSON() artel.Codec[mutantState] {
	return artel.JSONCodec(
		func(s mutantState) map[string]uint64 { return s.v },
		func(v map[string]uint64) mutantState { return mutantState{v: v} },
	)
}

func (s mutantState) Join(o mutantState) mutantState {
	m := s.merge
	if m == noMerge {
		m = o.merge
	}
	out := make(map[string]uint64, len(s.v)+len(o.v))
	maps.Copy(out, s.v)
	switch m {
	case lwwMerge:
		maps.Copy(out, o.v)
	case sumMerge:
		for k, x := range o.v {
			out[k] += x
		}
	}
	return mutantState{merge: m, v: out}
}

// mutantCounter is a counter replica over mutantState, shaped like GCounter:
// a lww counter ships absolute values, a sum counter ships increments.
type mutantCounter struct {
	id    string
	state mutantState
	delta mutantState
}

var _ artel.DeltaReplica[mutantState] = (*mutantCounter)(nil)

func newMutantCounter(id string, m mutantMerge) *mutantCounter {
	return &mutantCounter{
		id:    id,
		state: mutantState{merge: m, v: map[string]uint64{}},
		delta: mutantState{merge: m, v: map[string]uint64{}},
	}
}

func (c *mutantCounter) State() mutantState { return c.state.Join(mutantState{}) }

func (c *mutantCounter) Merge(o mutantState) { c.state = c.state.Join(o) }

func (c *mutantCounter) Delta() mutantState { return c.delta }

func (c *mutantCounter) FlushDelta() mutantState {
	d := c.delta
	c.delta = mutantState{merge: c.state.merge, v: map[string]uint64{}}
	return d
}

func (c *mutantCounter) IncrementBy(x uint64) {
	c.state.v[c.id] += x
	switch c.state.merge {
	case lwwMerge:
		c.delta.v[c.id] = c.state.v[c.id]
	case sumMerge:
		c.delta.v[c.id] += x
	}
}

func (c *mutantCounter) Value() (sum uint64) {
	for _, x := range c.state.v {
		sum += x
	}
	return sum
}

var _ counterReplica = (*mutantCounter)(nil)

func (c *mutantCounter) apply(n int) error {
	if n < 0 {
		return fmt.Errorf("simtest: mutant counter cannot decrement: %d", n)
	}
	c.IncrementBy(uint64(n))
	return nil
}

func (c *mutantCounter) value() string { return strconv.FormatUint(c.Value(), 10) }

func (c *mutantCounter) snapshot() ([]byte, error) { return mutantJSON().Encode(c.State()) }

// mutantType runs the reference core over a counter whose merge breaks the
// given law.
func mutantType(m mutantMerge) Subject { return mutantTypeSubject{merge: m} }

type mutantTypeSubject struct{ merge mutantMerge }

func (s mutantTypeSubject) NewNode(id string, incarnation int, peers []string) Node {
	rep := newMutantCounter(replicaID(id, incarnation), s.merge)
	return &counterNode{id: id, core: newRefCore(id, rep, peers, mutantJSON()), rep: rep}
}

// coreMutant wraps every core a subject builds; the node keeps the subject's
// Apply and Observe, so only what the scheduler drives changes.
type coreMutant struct {
	Subject
	wrap func(artel.Core) artel.Core
}

func (m coreMutant) NewNode(id string, incarnation int, peers []string) Node {
	n := m.Subject.NewNode(id, incarnation, peers)
	return wrappedNode{Node: n, core: m.wrap(n.Core())}
}

type wrappedNode struct {
	Node
	core artel.Core
}

func (w wrappedNode) Core() artel.Core { return w.core }

// leakyCore reports every outcome as success: a failed push is never
// returned to the buffer — the reference protocol minus retain.
type leakyCore struct{ artel.Core }

func (c leakyCore) SendResult(to string, kind artel.Kind, _ error) {
	c.Core.SendResult(to, kind, nil)
}

// stuckCore never reports a failure: the in-flight slot of a failed push is
// never cleared, so that link carries nothing ever again.
type stuckCore struct{ artel.Core }

func (c stuckCore) SendResult(to string, kind artel.Kind, err error) {
	if err == nil {
		c.Core.SendResult(to, kind, nil)
	}
}

func leaky(sub Subject) Subject {
	return coreMutant{sub, func(c artel.Core) artel.Core { return leakyCore{c} }}
}

func stuck(sub Subject) Subject {
	return coreMutant{sub, func(c artel.Core) artel.Core { return stuckCore{c} }}
}
