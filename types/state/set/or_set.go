package set

import (
	"crdtlab/crdt"
	"crdtlab/utils"
)

type ORSet[T comparable] struct {
	id    string
	state ORSetState[T]
}

type ORSetState[T comparable] struct {
	values map[T]map[utils.VersionDot]struct{}
	vv     utils.VersionVector
}

var _ crdt.StateReplica[ORSetState[string]] = (*ORSet[string])(nil)

func NewORSet[T comparable](id string) *ORSet[T] {
	return &ORSet[T]{
		id: id,
		state: ORSetState[T]{
			values: make(map[T]map[utils.VersionDot]struct{}),
			vv:     make(utils.VersionVector),
		},
	}
}

func (s *ORSet[T]) Merge(other ORSetState[T]) {
	kept := make(map[T]map[utils.VersionDot]struct{})
	keep := func(val T, dot utils.VersionDot) {
		if _, exists := kept[val]; !exists {
			kept[val] = make(map[utils.VersionDot]struct{})
		}
		kept[val][dot] = struct{}{}
	}

	for val, dots := range other.values {
		for dot := range dots {
			if _, ok := s.state.values[val][dot]; ok {
				keep(val, dot)
				continue
			}

			if s.state.vv[dot.Replica] < dot.Counter {
				keep(val, dot)
			}
		}
	}

	for val, dots := range s.state.values {
		for dot := range dots {
			if _, ok := other.values[val][dot]; ok {
				keep(val, dot)
				continue
			}

			if other.vv[dot.Replica] < dot.Counter {
				keep(val, dot)
			}
		}
	}

	s.state.values = kept
	s.state.vv.Merge(other.vv)
}

func (s *ORSet[T]) State() ORSetState[T] {
	return s.state
}

func (s *ORSet[T]) Add(v T) {
	s.state.vv.Increment(s.id)
	dot := utils.VersionDot{Replica: s.id, Counter: s.state.vv[s.id]}
	if _, exists := s.state.values[v]; !exists {
		s.state.values[v] = make(map[utils.VersionDot]struct{})
	}
	s.state.values[v][dot] = struct{}{}
}

func (s *ORSet[T]) Remove(v T) {
	delete(s.state.values, v)
}

func (s *ORSet[T]) Contains(v T) bool {
	dots, exists := s.state.values[v]
	return exists && len(dots) > 0
}

func (s *ORSet[T]) Value() []T {
	vals := make([]T, 0, len(s.state.values))
	for val := range s.state.values {
		vals = append(vals, val)
	}
	return vals
}
