package graph

import (
	"crdtlab/crdt"
	"crdtlab/types/state/set"
)

type TwoPTwoPGraph[T comparable] struct {
	id       string
	vertices *set.TwoPhaseSet[T]
	edges    *set.TwoPhaseSet[[2]T]
}

type TwoPTwoPGraphState[T comparable] struct {
	vertices set.TwoPhaseSetState[T]
	edges    set.TwoPhaseSetState[[2]T]
}

var _ crdt.StateReplica[TwoPTwoPGraphState[string]] = (*TwoPTwoPGraph[string])(nil)

func NewTwoPTwoPGraph[T comparable](id string) *TwoPTwoPGraph[T] {
	return &TwoPTwoPGraph[T]{
		id:       id,
		vertices: set.NewTwoPhaseSet[T](id),
		edges:    set.NewTwoPhaseSet[[2]T](id),
	}
}

func (g *TwoPTwoPGraph[T]) Merge(other TwoPTwoPGraphState[T]) {
	g.vertices.Merge(other.vertices)
	g.edges.Merge(other.edges)
}

func (g *TwoPTwoPGraph[T]) State() TwoPTwoPGraphState[T] {
	return TwoPTwoPGraphState[T]{
		vertices: g.vertices.State(),
		edges:    g.edges.State(),
	}
}

func (g *TwoPTwoPGraph[T]) ContainsVertex(v T) bool {
	return g.vertices.Contains(v)
}

func (g *TwoPTwoPGraph[T]) ContainsEdge(u, v T) bool {
	if !g.vertices.Contains(u) || !g.vertices.Contains(v) {
		return false
	}

	return g.edges.Contains([2]T{u, v})
}

func (g *TwoPTwoPGraph[T]) AddVertex(v T) {
	g.vertices.Add(v)
}

func (g *TwoPTwoPGraph[T]) RemoveVertex(v T) bool {
	for _, e := range g.edges.Values() {
		if e[0] == v || e[1] == v {
			return false
		}
	}
	g.vertices.Remove(v)
	return true
}

func (g *TwoPTwoPGraph[T]) AddEdge(u, v T) bool {
	if !g.vertices.Contains(u) || !g.vertices.Contains(v) {
		return false
	}
	g.edges.Add([2]T{u, v})
	return true
}

func (g *TwoPTwoPGraph[T]) RemoveEdge(u, v T) {
	g.edges.Remove([2]T{u, v})
}
