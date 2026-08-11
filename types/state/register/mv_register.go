package register

import (
	"crdtlab/crdt"
	"crdtlab/utils"
)

type MVRegister[T any] struct {
	id    string
	state MVRegisterState[T]
}

type MVRegisterState[T any] struct {
	values map[utils.VersionDot]T
	vv     utils.VersionVector
}

var _ crdt.StateReplica[MVRegisterState[int]] = (*MVRegister[int])(nil)

func NewMVRegister[T any](id string) *MVRegister[T] {
	return &MVRegister[T]{
		id:    id,
		state: MVRegisterState[T]{values: make(map[utils.VersionDot]T), vv: make(utils.VersionVector)},
	}
}

func (m *MVRegister[T]) Merge(other MVRegisterState[T]) {
	kept := make(map[utils.VersionDot]T)
	for dot, v := range other.values {
		if _, ok := m.state.values[dot]; ok {
			kept[dot] = v
			continue
		}

		if m.state.vv[dot.Replica] < dot.Counter {
			kept[dot] = v
		}
	}

	for dot, v := range m.state.values {
		if _, ok := other.values[dot]; ok {
			kept[dot] = v
			continue
		}

		if other.vv[dot.Replica] < dot.Counter {
			kept[dot] = v
		}
	}

	m.state.values = kept
	m.state.vv.Merge(other.vv)
}

func (m *MVRegister[T]) State() MVRegisterState[T] {
	return m.state
}

func (m *MVRegister[T]) Set(value T) {
	m.state.vv.Increment(m.id)
	dot := utils.VersionDot{Replica: m.id, Counter: m.state.vv[m.id]}
	m.state.values = map[utils.VersionDot]T{dot: value}
}

func (m *MVRegister[T]) Value() []T {
	vals := make([]T, 0, len(m.state.values))
	for _, val := range m.state.values {
		vals = append(vals, val)
	}

	return vals
}
