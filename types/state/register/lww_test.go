package register

import (
	"testing"

	"crdtlab/crdt"
)

func TestLWWRegister(t *testing.T) {
	t.Run("later write wins and converges", func(t *testing.T) {
		pt := int64(0)
		now := func() int64 { return pt }
		net := crdt.NewNetwork[*LWWRegister[string], LWWRegisterState[string]]()
		net.Add("A", NewLWWRegister[string]("A", now))
		net.Add("B", NewLWWRegister[string]("B", now))

		pt = 1
		net.Get("A").Set("a")
		pt = 2
		net.Get("B").Set("b")
		net.SyncAll()

		if !net.Converged() {
			t.Fatal("replicas did not converge")
		}
		for _, id := range []string{"A", "B"} {
			if got := net.Get(id).Value(); got != "b" {
				t.Errorf("replica %s: Value() = %q, want %q", id, got, "b")
			}
		}
	})

	t.Run("equal stamps: higher id wins, deterministically", func(t *testing.T) {
		pt := int64(5)
		now := func() int64 { return pt }
		a := NewLWWRegister[string]("A", now)
		b := NewLWWRegister[string]("B", now)
		a.Set("a")
		b.Set("b") // same stamp -> tie -> higher writer id (B) wins

		// Capture both BEFORE merging, so each sees the other's original
		// concurrent write (a true symmetric exchange, not a sequential one).
		sa, sb := a.State(), b.State()
		a.Merge(sb)
		b.Merge(sa)
		if a.Value() != b.Value() {
			t.Fatalf("equal-stamp tie diverged (writer not recorded?): a=%q b=%q", a.Value(), b.Value())
		}
		if a.Value() != "b" {
			t.Fatalf("tie-break should pick higher writer id B, got %q", a.Value())
		}
	})

	t.Run("a real write beats a freshly-created empty register", func(t *testing.T) {
		pt := int64(1)
		now := func() int64 { return pt }
		w := NewLWWRegister[string]("W", now)
		w.Set("real")

		pt = 100
		fresh := NewLWWRegister[string]("F", now)
		fresh.Merge(w.State())
		if got := fresh.Value(); got != "real" {
			t.Fatalf("fresh register kept its empty value over a real write: %q", got)
		}
	})

	t.Run("adopted value keeps the writer's stamp, not the local clock", func(t *testing.T) {
		// Receiver R has a fast clock and is empty. If Merge re-stamps an adopted
		// value with R's own clock instead of keeping the writer's timestamp, the
		// early write inflates above the later one and wrongly wins.
		mk := func(id, val string, at int64) LWWRegisterState[string] {
			r := NewLWWRegister[string](id, func() int64 { return at })
			r.Set(val)
			return r.State()
		}
		early := mk("A", "early", 5)
		late := mk("C", "late", 50)

		r := NewLWWRegister[string]("R", func() int64 { return 1000 })
		r.Merge(early)
		r.Merge(late)
		if got := r.Value(); got != "late" {
			t.Fatalf("later write must win; got %q (adopted stamp likely inflated by the local clock)", got)
		}
	})

	t.Run("merge advances the clock even when local value wins", func(t *testing.T) {
		r1 := NewLWWRegister[string]("R1", func() int64 { return 100 })
		r1.Set("x")

		pt := int64(1)
		r2 := NewLWWRegister[string]("R2", func() int64 { return pt })
		r2.Set("y")
		r2.Merge(r1.State()) // adopts "x"; r2's clock must catch up to ~100
		pt = 2
		r2.Set("z")          // must land above r1's stamp -> only if the clock advanced
		r2.Merge(r1.State()) // re-deliver old r1: "z" must survive
		if got := r2.Value(); got != "z" {
			t.Fatalf("z did not dominate after a local write; clock did not advance on merge (got %q)", got)
		}
	})

	t.Run("merge is idempotent", func(t *testing.T) {
		pt := int64(7)
		now := func() int64 { return pt }
		src := NewLWWRegister[string]("S", now)
		src.Set("v")

		once := NewLWWRegister[string]("T", now)
		once.Merge(src.State())
		twice := NewLWWRegister[string]("T", now)
		twice.Merge(src.State())
		twice.Merge(src.State())
		if once.Value() != twice.Value() {
			t.Fatalf("idempotence: once=%q twice=%q", once.Value(), twice.Value())
		}
	})

	t.Run("merge is commutative", func(t *testing.T) {
		pt := int64(0)
		now := func() int64 { return pt }
		mk := func(id, val string, at int64) *LWWRegister[string] {
			pt = at
			r := NewLWWRegister[string](id, now)
			r.Set(val)
			return r
		}
		x := mk("X", "x", 1)
		y := mk("Y", "y", 2)
		z := mk("Z", "z", 3) // latest

		left := NewLWWRegister[string]("L", now)
		left.Merge(x.State())
		left.Merge(y.State())
		left.Merge(z.State())
		right := NewLWWRegister[string]("R", now)
		right.Merge(z.State())
		right.Merge(y.State())
		right.Merge(x.State())

		if left.Value() != right.Value() {
			t.Fatalf("commutativity: left=%q right=%q", left.Value(), right.Value())
		}
		if left.Value() != "z" {
			t.Fatalf("want z (latest), got %q", left.Value())
		}
	})
}
