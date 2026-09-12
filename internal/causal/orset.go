package causal

import (
	"maps"
	"sync"

	"github.com/kudesn1k1/artel"
)

func _[T comparable]() {
	var _ artel.DeltaReplica[ORSetState[T]] = (*ORSet[T])(nil)
	var _ artel.DeltaState[ORSetState[T]] = ORSetState[T]{}
}

// ORSetState is the state of an [ORSet]: its elements, each tagged by the
// dots of the adds that put it there, and the causal context those dots
// live in. One type carries both a full state and a delta.
type ORSetState[T comparable] struct {
	store DotMap[T]
	cc    CausalContext
}

// NewORSetState returns an empty state.
func NewORSetState[T comparable]() ORSetState[T] {
	return ORSetState[T]{
		store: make(DotMap[T]),
		cc: CausalContext{
			VV:   make(VersionVector),
			Dots: make(DotSet),
		},
	}
}

// Join merges two states into a new one; both arguments stay as they were.
// An element survives while one of its dots does, and a dot survives unless
// the other side has seen it and no longer holds it.
func (s ORSetState[T]) Join(o ORSetState[T]) ORSetState[T] {
	state, cc := Join(s.store, s.cc, o.store, o.cc)
	return ORSetState[T]{state, cc}
}

// IsBottom reports whether the state holds nothing at all: no elements and
// an empty causal context.
func (s ORSetState[T]) IsBottom() bool {
	return len(s.store) == 0 && len(s.cc.VV) == 0 && len(s.cc.Dots) == 0
}

// ORSet is an add-wins observed-remove set: a Remove undoes only the adds
// its replica has observed, so an Add concurrent with a Remove of the same
// element wins. It is safe for concurrent use. T must be usable as a map
// key; to travel through [ORSetJSON] it must also survive JSON.
type ORSet[T comparable] struct {
	id       string
	state    ORSetState[T]
	delta    ORSetState[T]
	rejected int
	mutex    sync.Mutex
}

// NewORSet returns an empty set owned by the replica id.
func NewORSet[T comparable](id string) *ORSet[T] {
	return &ORSet[T]{
		id:    id,
		state: NewORSetState[T](),
		delta: NewORSetState[T](),
	}
}

// Merge folds an incoming state or delta into the set. A delta that skips
// ahead of what the set has already seen from its origin is refused whole
// and counted by [ORSet.Rejected]: the set does not change, and the delta
// has to arrive again once the gap is filled. A full state is never
// refused.
func (o *ORSet[T]) Merge(other ORSetState[T]) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	state := o.state.Join(other)
	if !state.cc.Compact() {
		o.rejected++
		return
	}
	o.state = state
}

// State returns the full state of the set.
func (o *ORSet[T]) State() ORSetState[T] {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.state
}

// Delta returns the changes made locally since the last FlushDelta.
func (o *ORSet[T]) Delta() ORSetState[T] {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.delta
}

// FlushDelta returns the changes made locally since the last call and starts
// collecting anew.
func (o *ORSet[T]) FlushDelta() ORSetState[T] {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	delta := o.delta
	o.delta = NewORSetState[T]()
	return delta
}

// Add puts e into the set, even if it is already there: a fresh add outlives
// any concurrent Remove.
func (o *ORSet[T]) Add(e T) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	old := make(DotSet)
	maps.Copy(old, o.state.store[e])

	dot := o.state.cc.VV.Next(o.id)
	old[dot] = struct{}{}

	delta := ORSetState[T]{
		store: DotMap[T]{
			e: DotSet{dot: struct{}{}},
		},
		cc: CausalContext{
			Dots: old,
		},
	}
	o.state = o.state.Join(delta)
	o.delta = o.delta.Join(delta)
}

// Remove takes e out of the set. Adds of e this replica has not yet seen
// survive.
func (o *ORSet[T]) Remove(e T) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	observedDots := o.state.store[e]

	delta := ORSetState[T]{
		cc: CausalContext{
			Dots: observedDots,
		},
	}
	o.state = o.state.Join(delta)
	o.delta = o.delta.Join(delta)
}

// Contains reports whether e is in the set.
func (o *ORSet[T]) Contains(e T) bool {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	dots, ok := o.state.store[e]
	return ok && len(dots) > 0
}

// Elements returns the elements in no particular order.
func (o *ORSet[T]) Elements() []T {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	els := make([]T, 0, len(o.state.store))
	for e := range o.state.store {
		els = append(els, e)
	}
	return els
}

// Rejected returns how many deltas Merge has refused so far.
func (o *ORSet[T]) Rejected() int {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.rejected
}
