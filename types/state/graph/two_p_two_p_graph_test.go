package graph

import (
	"testing"

	"crdtlab/crdt"
)

func TestTwoPTwoPGraph(t *testing.T) {
	t.Run("add vertices then an edge", func(t *testing.T) {
		g := NewTwoPTwoPGraph[string]("A")
		g.AddVertex("u")
		g.AddVertex("v")
		if !g.AddEdge("u", "v") {
			t.Fatal("AddEdge should succeed when both endpoints exist")
		}
		if !g.ContainsEdge("u", "v") {
			t.Fatal("edge (u,v) should be present")
		}
	})

	t.Run("addEdge requires both endpoints", func(t *testing.T) {
		g := NewTwoPTwoPGraph[string]("A")
		g.AddVertex("u")
		if g.AddEdge("u", "missing") {
			t.Fatal("AddEdge must fail when an endpoint is absent")
		}
		if g.ContainsEdge("u", "missing") {
			t.Fatal("edge to a missing vertex must not be present")
		}
	})

	t.Run("removeVertex is rejected while an incident edge exists", func(t *testing.T) {
		g := NewTwoPTwoPGraph[string]("A")
		g.AddVertex("u")
		g.AddVertex("v")
		g.AddEdge("u", "v")
		if g.RemoveVertex("u") {
			t.Fatal("RemoveVertex must fail while u has an incident edge")
		}
		if !g.ContainsVertex("u") {
			t.Fatal("u should still be present")
		}
	})

	t.Run("concurrent addEdge vs removeVertex: removeVertex wins, edge invisible", func(t *testing.T) {
		a := NewTwoPTwoPGraph[string]("A")
		a.AddVertex("u")
		a.AddVertex("v")

		b := NewTwoPTwoPGraph[string]("B")
		b.Merge(a.State()) // both know u, v

		// concurrent: A removes u (no edges locally), B adds edge (u,v)
		a.RemoveVertex("u")
		b.AddEdge("u", "v")

		// A receives B's concurrent addEdge:
		a.Merge(b.State())
		if a.ContainsVertex("u") {
			t.Fatal("u was removed, should be absent")
		}
		if a.ContainsEdge("u", "v") {
			t.Fatal("edge to a removed vertex must be invisible (removeVertex wins)")
		}
	})

	t.Run("re-add vertex after remove does not work (2P inheritance)", func(t *testing.T) {
		g := NewTwoPTwoPGraph[string]("A")
		g.AddVertex("u")
		g.RemoveVertex("u")
		g.AddVertex("u") // tombstone is permanent
		if g.ContainsVertex("u") {
			t.Fatal("a removed vertex cannot be re-added")
		}
	})

	t.Run("converges across replicas", func(t *testing.T) {
		net := crdt.NewNetwork[*TwoPTwoPGraph[string], TwoPTwoPGraphState[string]]()
		net.Add("A", NewTwoPTwoPGraph[string]("A"))
		net.Add("B", NewTwoPTwoPGraph[string]("B"))

		a := net.Get("A")
		a.AddVertex("u")
		a.AddVertex("v")
		a.AddVertex("w")
		a.AddEdge("u", "v")
		net.SyncAll() // B learns the graph

		net.Get("B").AddEdge("v", "w")    // B adds another edge
		net.Get("A").RemoveEdge("u", "v") // A removes the first
		net.SyncAll()

		for _, id := range []string{"A", "B"} {
			g := net.Get(id)
			if g.ContainsEdge("u", "v") {
				t.Errorf("replica %s: (u,v) should be removed", id)
			}
			if !g.ContainsEdge("v", "w") {
				t.Errorf("replica %s: (v,w) should be present", id)
			}
		}
	})
}
