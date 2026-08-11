package set

import "crdtlab/crdt"

type GSet[T comparable] struct {
	id    string
	state GSetState[T]
}

type GSetState[T comparable] struct {
	values map[T]struct{}
}

var _ crdt.StateReplica[GSetState[string]] = (*GSet[string])(nil)

func NewGSet[T comparable](id string) *GSet[T] {
	return &GSet[T]{
		id: id,
		state: GSetState[T]{
			values: make(map[T]struct{}),
		},
	}
}

func (s *GSet[T]) Merge(other GSetState[T]) {
	for v := range other.values {
		s.state.values[v] = struct{}{}
	}
}

func (s *GSet[T]) State() GSetState[T] {
	return s.state
}

func (s *GSet[T]) Add(v T) {
	s.state.values[v] = struct{}{}
}

func (s *GSet[T]) Values() []T {
	vals := make([]T, 0, len(s.state.values))
	for v := range s.state.values {
		vals = append(vals, v)
	}

	return vals
}
