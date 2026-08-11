package graph

import (
	"crdtlab/crdt"
	"crdtlab/utils"
	"sort"
)

// RGA Replicated growable array
type RGA[T any] struct {
	id    string
	state RGAState[T]
	clock *utils.HLC
}

// RGAState is a state of RGA type and stores rgaNodes by their utils.HLCDot timestamp. Zero-value utils.HLCDot is considered a root
type RGAState[T any] struct {
	nodes map[utils.HLCDot]rgaNode[T]
}

type rgaNode[T any] struct {
	value   T
	parent  utils.HLCDot
	deleted bool
}

var _ crdt.StateReplica[RGAState[string]] = (*RGA[string])(nil)

func NewRGA[T any](id string, now func() int64) *RGA[T] {
	return &RGA[T]{
		id: id,
		state: RGAState[T]{
			nodes: make(map[utils.HLCDot]rgaNode[T]),
		},
		clock: utils.NewHLC(now),
	}
}

func (r *RGA[T]) State() RGAState[T] {
	return r.state
}

func (r *RGA[T]) Merge(other RGAState[T]) {
	for ts, n := range other.nodes {
		if existing, ok := r.state.nodes[ts]; !ok {
			r.state.nodes[ts] = n
		} else {
			existing.deleted = existing.deleted || n.deleted
			r.state.nodes[ts] = existing
		}
		r.clock.Update(ts.HLCTimestamp)
	}
}

func (r *RGA[T]) InsertAfter(ts utils.HLCDot, value T) utils.HLCDot {
	newTs := utils.HLCDot{HLCTimestamp: r.clock.Now(), Replica: r.id}
	n := rgaNode[T]{value, ts, false}
	r.state.nodes[newTs] = n
	return newTs
}

func (r *RGA[T]) Delete(ts utils.HLCDot) {
	if n, ok := r.state.nodes[ts]; ok {
		n.deleted = true
		r.state.nodes[ts] = n
	}
}

func (r *RGA[T]) Value() []T {
	children := make(map[utils.HLCDot][]utils.HLCDot)
	for id, n := range r.state.nodes {
		children[n.parent] = append(children[n.parent], id)
	}
	for _, sibs := range children {
		sort.Slice(sibs, func(i, j int) bool { return sibs[j].Less(sibs[i]) })
	}

	var out []T
	var walk func(parent utils.HLCDot)
	walk = func(parent utils.HLCDot) {
		for _, id := range children[parent] {
			n := r.state.nodes[id]
			if !n.deleted {
				out = append(out, n.value)
			}
			walk(id)
		}
	}
	walk(utils.HLCDot{})
	return out
}
