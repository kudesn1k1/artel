package utils

import "testing"

func tsLess(a, b HLCTimestamp) bool { return a.L < b.L || (a.L == b.L && a.C < b.C) }

func TestHLC(t *testing.T) {
	t.Run("c increments while physical time stalls", func(t *testing.T) {
		pt := int64(10)
		h := NewHLC(func() int64 { return pt })
		a, b, c := h.Now(), h.Now(), h.Now()
		if a != (HLCTimestamp{L: 10, C: 0}) || b != (HLCTimestamp{L: 10, C: 1}) || c != (HLCTimestamp{L: 10, C: 2}) {
			t.Fatalf("got %+v %+v %+v", a, b, c)
		}
	})

	t.Run("c resets when physical time advances", func(t *testing.T) {
		pt := int64(10)
		h := NewHLC(func() int64 { return pt })
		h.Now() // (10,0)
		h.Now() // (10,1)
		pt = 20
		if got := h.Now(); got != (HLCTimestamp{L: 20, C: 0}) {
			t.Fatalf("want {20 0}, got %+v", got)
		}
	})

	t.Run("physical clock going backwards keeps l monotonic", func(t *testing.T) {
		pt := int64(10)
		h := NewHLC(func() int64 { return pt })
		h.Now() // (10,0)
		pt = 5  // clock jumps back
		if got := h.Now(); got != (HLCTimestamp{L: 10, C: 1}) {
			t.Fatalf("want {10 1}, got %+v", got)
		}
	})

	t.Run("Update takes the max and stays ahead of remote", func(t *testing.T) {
		pt := int64(1)
		h := NewHLC(func() int64 { return pt })
		h.Now() // (1,0)
		got := h.Update(HLCTimestamp{L: 100, C: 5})
		if got != (HLCTimestamp{L: 100, C: 6}) { // remote l wins -> c = c_m + 1
			t.Fatalf("want {100 6}, got %+v", got)
		}
		if next := h.Now(); !tsLess(got, next) {
			t.Fatalf("Now() %+v not strictly greater than %+v", next, got)
		}
	})

	t.Run("every event is strictly monotonic", func(t *testing.T) {
		pt := int64(0)
		h := NewHLC(func() int64 { pt++; return pt }) // time creeps forward
		prev := h.Now()
		for i := 0; i < 50; i++ {
			cur := h.Now()
			if i%3 == 0 {
				cur = h.Update(HLCTimestamp{L: prev.L + 2, C: 0}) // sometimes ahead of us
			}
			if !tsLess(prev, cur) {
				t.Fatalf("not monotonic: %+v then %+v", prev, cur)
			}
			prev = cur
		}
	})
}
