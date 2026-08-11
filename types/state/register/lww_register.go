package register

import (
	"crdtlab/crdt"
	"crdtlab/utils"
)

type LWWRegister[T any] struct {
	id    string
	clock *utils.HLC
	state LWWRegisterState[T]
}

type LWWRegisterState[T any] struct {
	value     T
	timestamp utils.HLCDot
}

var _ crdt.StateReplica[LWWRegisterState[int]] = (*LWWRegister[int])(nil)

func NewLWWRegister[T any](id string, now func() int64) *LWWRegister[T] {
	hlc := utils.NewHLC(now)
	return &LWWRegister[T]{
		id:    id,
		clock: hlc,
	}
}

func (r *LWWRegister[T]) Set(value T) {
	r.state.value = value
	r.state.timestamp = utils.HLCDot{HLCTimestamp: r.clock.Now(), Replica: r.id}
}

func (r *LWWRegister[T]) Value() T {
	return r.state.value
}

func (r *LWWRegister[T]) State() LWWRegisterState[T] {
	return r.state
}

func (r *LWWRegister[T]) Merge(other LWWRegisterState[T]) {
	r.clock.Update(other.timestamp.HLCTimestamp)

	if other.timestamp.Less(r.state.timestamp) {
		return
	}

	r.state = other
}
