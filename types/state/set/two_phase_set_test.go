package set

import (
	"testing"

	"crdtlab/crdt"
)

func TestTwoPhaseSet(t *testing.T) {
	t.Run("add then contains", func(t *testing.T) {
		s := NewTwoPhaseSet[string]("A")
		s.Add("x")
		if !s.Contains("x") {
			t.Fatal("x should be present after Add")
		}
	})

	t.Run("add then remove is absent", func(t *testing.T) {
		s := NewTwoPhaseSet[string]("A")
		s.Add("x")
		s.Remove("x")
		if s.Contains("x") {
			t.Fatal("x should be absent after Remove")
		}
	})

	t.Run("re-add after remove does NOT work (permanent tombstone)", func(t *testing.T) {
		s := NewTwoPhaseSet[string]("A")
		s.Add("x")
		s.Remove("x")
		s.Add("x") // tombstone dominates -> stays absent
		if s.Contains("x") {
			t.Fatal("2P-Set must not allow re-add after remove")
		}
	})

	t.Run("concurrent add and remove -> remove wins", func(t *testing.T) {
		a := NewTwoPhaseSet[string]("A")
		a.Add("x")
		a.Remove("x") // A tombstones x

		b := NewTwoPhaseSet[string]("B")
		b.Add("x") // B adds x concurrently, never having seen A's remove

		a.Merge(b.State())
		b.Merge(a.State())
		if a.Contains("x") || b.Contains("x") {
			t.Fatalf("remove must win on concurrent add/remove: a=%v b=%v",
				a.Contains("x"), b.Contains("x"))
		}
	})

	t.Run("converges across replicas", func(t *testing.T) {
		net := crdt.NewNetwork[*TwoPhaseSet[string], TwoPhaseSetState[string]]()
		net.Add("A", NewTwoPhaseSet[string]("A"))
		net.Add("B", NewTwoPhaseSet[string]("B"))
		net.Add("C", NewTwoPhaseSet[string]("C"))

		net.Get("A").Add("x")
		net.Get("A").Add("y")
		net.Get("B").Add("z")
		net.Sync("A", "B")
		net.Get("B").Remove("x") // tombstone x after observing it
		net.Get("C").Add("w")
		net.SyncAll()

		want := []string{"y", "z", "w"} // x stays removed everywhere
		for _, id := range []string{"A", "B", "C"} {
			if got := net.Get(id).Values(); !sameSet(got, want) {
				t.Errorf("replica %s: Value() = %v, want %v", id, got, want)
			}
		}
	})

	t.Run("merge is idempotent", func(t *testing.T) {
		a := NewTwoPhaseSet[string]("A")
		a.Add("x")
		a.Add("y")
		a.Remove("y")

		once := NewTwoPhaseSet[string]("T")
		once.Merge(a.State())
		twice := NewTwoPhaseSet[string]("T")
		twice.Merge(a.State())
		twice.Merge(a.State())
		if !sameSet(once.Values(), twice.Values()) {
			t.Fatalf("idempotence: once=%v twice=%v", once.Values(), twice.Values())
		}
	})
}
