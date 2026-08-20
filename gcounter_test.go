package artel

import (
	"reflect"
	"testing"
)

// RED spec for the delta-state GCounter (M1). Target API (you implement gcounter.go):
//
//	NewGCounter(id string) *GCounter
//	(*GCounter) Increment()                 // return is free — domain value or void
//	(*GCounter) Value() uint64
//	(*GCounter) State() GCounterState        // full state (cheap accessor)
//	(*GCounter) Merge(other GCounterState)   // join — handles full state AND a delta
//	(*GCounter) Delta() GCounterState        // accumulated delta-group to ship
//	(*GCounter) ResetDelta()                 // clear buffer to ⊥ after a send
//
// Also add: var _ crdt.DeltaReplica[GCounterState] = (*GCounter)(nil)

func sameState(a, b GCounterState) bool { return reflect.DeepEqual(a, b) }

func TestDeltaGCounter(t *testing.T) {
	// Shipping+joining the DELTA (not the full state) must converge identically.
	t.Run("delta exchange converges", func(t *testing.T) {
		a := NewGCounter("A")
		b := NewGCounter("B")
		a.Increment()
		a.Increment()
		a.Increment()
		b.Increment()

		dA, dB := a.Delta(), b.Delta()
		a.Merge(dB)
		b.Merge(dA)

		if !sameState(a.State(), b.State()) {
			t.Fatalf("did not converge: A=%v B=%v", a.State(), b.State())
		}
		if a.Value() != 4 || b.Value() != 4 {
			t.Fatalf("want 4/4, got A=%d B=%d", a.Value(), b.Value())
		}
	})

	// A delta is a state (absolute value), so re-joining it is idempotent.
	t.Run("delta join is idempotent", func(t *testing.T) {
		src := NewGCounter("S")
		for range 5 {
			src.Increment()
		}
		d := src.Delta()

		once := NewGCounter("T")
		once.Merge(d)
		twice := NewGCounter("T")
		twice.Merge(d)
		twice.Merge(d) // re-delivered duplicate must change nothing
		if !sameState(once.State(), twice.State()) {
			t.Fatalf("not idempotent: once=%v twice=%v", once.State(), twice.State())
		}
	})

	// Successive local mutations join into ONE delta-group (they must not be lost).
	t.Run("buffer collapses successive increments", func(t *testing.T) {
		a := NewGCounter("A")
		a.Increment()
		a.Increment()
		a.Increment()

		f := NewGCounter("F")
		f.Merge(a.Delta()) // one delta carrying all three
		if f.Value() != 3 {
			t.Fatalf("want 3, got %d", f.Value())
		}
	})

	// The delta ships the ABSOLUTE entry value, so a replica that missed an
	// earlier delta and receives only a later one still catches up fully.
	t.Run("absolute delta tolerates a missed earlier delta", func(t *testing.T) {
		a := NewGCounter("A")
		a.Increment()
		d1 := a.FlushDelta() // grab the first delta AND reset the buffer

		b := NewGCounter("B")
		b.Merge(d1) // B sees the first delta only

		a.Increment()
		a.Increment()
		d3 := a.FlushDelta()

		c := NewGCounter("C")
		c.Merge(d3) // C never saw d1, only the post-reset delta
		if c.Value() != 3 {
			t.Fatalf("absolute delta should carry full count 3, got %d", c.Value())
		}
	})

	// FlushDelta returns the accumulated delta AND clears the buffer to ⊥ — but
	// must NOT touch the replica's state.
	t.Run("FlushDelta returns the delta, empties buffer, keeps state", func(t *testing.T) {
		a := NewGCounter("A")
		a.Increment()
		a.Increment()

		flushed := a.FlushDelta()
		if flushed.values["A"] != 2 {
			t.Fatalf("flushed delta should carry the buffered count 2, got %d", flushed.values["A"])
		}
		if a.Value() != 2 {
			t.Fatalf("flush must not touch state; want 2, got %d", a.Value())
		}

		b := NewGCounter("B")
		before := b.Value()
		b.Merge(a.Delta()) // buffer is empty after the flush → a no-op
		if b.Value() != before {
			t.Fatalf("empty delta after flush should be a no-op; before=%d after=%d", before, b.Value())
		}
	})

	// Join has signature Join(S) S — it must be pure: neither operand may be
	// mutated. (Maps are reference types, so a value receiver is NOT enough.)
	t.Run("Join is pure — does not mutate operands", func(t *testing.T) {
		a := GCounterState{map[string]uint64{"X": 1}}
		b := GCounterState{map[string]uint64{"X": 5, "Y": 2}}

		_ = a.Join(b)

		if a.values["X"] != 1 || len(a.values) != 1 {
			t.Fatalf("Join mutated its receiver: a=%v", a.values)
		}
		if b.values["X"] != 5 || b.values["Y"] != 2 || len(b.values) != 2 {
			t.Fatalf("Join mutated its argument: b=%v", b.values)
		}
	})

	// A grabbed delta must be a stable snapshot: further local mutation of the
	// source replica must not change a delta already handed out.
	t.Run("Delta snapshot is stable across later increments", func(t *testing.T) {
		a := NewGCounter("A")
		a.Increment()
		snap := a.Delta() // {A:1}
		a.Increment()     // must not retro-change snap
		if snap.values["A"] != 1 {
			t.Fatalf("delta snapshot moved under later increment: got %d, want 1", snap.values["A"])
		}
	})
}
