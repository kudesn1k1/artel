package set

import "crdtlab/crdt"

type TwoPhaseSet[T comparable] struct {
	id    string
	state TwoPhaseSetState[T]
}

type TwoPhaseSetState[T comparable] struct {
	added   GSetState[T]
	removed GSetState[T]
}

var _ crdt.StateReplica[TwoPhaseSetState[string]] = (*TwoPhaseSet[string])(nil)

func NewTwoPhaseSet[T comparable](id string) *TwoPhaseSet[T] {
	return &TwoPhaseSet[T]{
		id: id,
		state: TwoPhaseSetState[T]{
			added: GSetState[T]{
				values: make(map[T]struct{}),
			},
			removed: GSetState[T]{
				values: make(map[T]struct{}),
			},
		},
	}
}

func (s *TwoPhaseSet[T]) Merge(other TwoPhaseSetState[T]) {
	for v := range other.added.values {
		s.state.added.values[v] = struct{}{}
	}
	for v := range other.removed.values {
		s.state.removed.values[v] = struct{}{}
	}
}

func (s *TwoPhaseSet[T]) State() TwoPhaseSetState[T] {
	return s.state
}

func (s *TwoPhaseSet[T]) Add(v T) {
	s.state.added.values[v] = struct{}{}
}

func (s *TwoPhaseSet[T]) Remove(v T) {
	s.state.removed.values[v] = struct{}{}
}

func (s *TwoPhaseSet[T]) Contains(v T) bool {
	if _, ok := s.state.added.values[v]; !ok {
		return false
	}
	if _, ok := s.state.removed.values[v]; ok {
		return false
	}
	return true
}

func (s *TwoPhaseSet[T]) Values() []T {
	vals := make([]T, 0, len(s.state.added.values))
	for v := range s.state.added.values {
		if _, ok := s.state.removed.values[v]; !ok {
			vals = append(vals, v)
		}
	}
	return vals
}
