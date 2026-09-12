package causal

import (
	"cmp"
	"slices"

	"github.com/kudesn1k1/artel"
)

// ORSetWire is the wire form of an ORSetState. Entries are ordered by their
// smallest dot, VV holds the last dot of each replica ordered by replica,
// Dots is sorted: equal states have equal wire forms.
type ORSetWire[T comparable] struct {
	Entries []ORSetEntry[T] `json:"entries,omitzero"`
	VV      []Dot           `json:"vv,omitzero"`
	Dots    []Dot           `json:"dots,omitzero"`
}

type ORSetEntry[T comparable] struct {
	Element T     `json:"element"`
	Dots    []Dot `json:"dots"`
}

func compareDots(a, b Dot) int {
	if c := cmp.Compare(a.Replica, b.Replica); c != 0 {
		return c
	}
	return cmp.Compare(a.N, b.N)
}

func sortedDots(ds DotSet) []Dot {
	var out []Dot
	for d := range ds {
		out = append(out, d)
	}
	slices.SortFunc(out, compareDots)
	return out
}

// Wire returns the state's wire form.
func (s ORSetState[T]) Wire() ORSetWire[T] {
	var w ORSetWire[T]
	for e, ds := range s.store {
		if len(ds) == 0 {
			continue
		}
		w.Entries = append(w.Entries, ORSetEntry[T]{Element: e, Dots: sortedDots(ds)})
	}
	slices.SortFunc(w.Entries, func(a, b ORSetEntry[T]) int { return compareDots(a.Dots[0], b.Dots[0]) })
	for r, n := range s.cc.VV {
		if n != 0 {
			w.VV = append(w.VV, Dot{Replica: r, N: n})
		}
	}
	slices.SortFunc(w.VV, compareDots)
	w.Dots = sortedDots(s.cc.Dots)
	return w
}

// ORSetStateFromWire rebuilds a state from its wire form.
func ORSetStateFromWire[T comparable](w ORSetWire[T]) ORSetState[T] {
	s := NewORSetState[T]()
	for _, en := range w.Entries {
		if len(en.Dots) == 0 {
			continue
		}
		set := make(DotSet, len(en.Dots))
		for _, d := range en.Dots {
			set[d] = struct{}{}
		}
		s.store[en.Element] = set
	}
	for _, d := range w.VV {
		if d.N != 0 {
			s.cc.VV[d.Replica] = d.N
		}
	}
	for _, d := range w.Dots {
		s.cc.Dots[d] = struct{}{}
	}
	return s
}

// ORSetJSON returns the JSON codec for ORSetState[T]. T must survive JSON:
// exported fields, or a TextMarshaler/TextUnmarshaler pair.
func ORSetJSON[T comparable]() artel.Codec[ORSetState[T]] {
	return artel.JSONCodec(ORSetState[T].Wire, ORSetStateFromWire[T])
}
