package simtest

import (
	"math/rand/v2"
	"slices"
)

// FaultKind names one network anomaly. Anomalies compose: a scenario carries
// any mix of fault windows.
type FaultKind string

const (
	FaultDrop      FaultKind = "drop"
	FaultDelay     FaultKind = "delay"
	FaultDup       FaultKind = "dup"
	FaultPartition FaultKind = "partition"
	// FaultAckLost delivers the message but reports an error to the sender.
	// Legal under the delivery contract — every subject must survive it.
	FaultAckLost FaultKind = "acklost"
	// FaultAckLie reports success WITHOUT delivering — a transport that
	// violates the delivery contract. Negative-space experiments only: it
	// must never appear in generated profiles.
	FaultAckLie FaultKind = "acklie"
)

func (k FaultKind) IsValid() bool {
	switch k {
	case FaultDrop, FaultDelay, FaultDup, FaultPartition, FaultAckLost, FaultAckLie:
		return true
	default:
		return false
	}
}

// FaultEntry is one anomaly window inside the active phase.
type FaultEntry struct {
	At, Until Dur
	Kind      FaultKind
	P         float64 // per-message probability, where the kind uses one
	MinD      Dur     // FaultDelay: delivery delay bounds
	MaxD      Dur
	Group     []int // FaultPartition: one side of the split
}

var faultPriority = map[FaultKind]int{
	FaultPartition: 0,
	FaultDrop:      1,
	FaultAckLie:    2,
	FaultDelay:     3,
	FaultDup:       4,
	FaultAckLost:   5,
}

type delivery struct {
	delay     Dur
	delivered bool
	acked     bool
}

type activeFault struct {
	FaultEntry
	group map[string]struct{} // only for Partition faults
}

type faultPolicy struct {
	rng    *rand.Rand
	faults []activeFault
}

func newFaultPolicy(seed uint64, faults []FaultEntry) faultPolicy {
	f := faultPolicy{
		rng:    rand.New(rand.NewPCG(seed, 1)),
		faults: make([]activeFault, 0, len(faults)),
	}

	for _, fault := range faults {
		af := activeFault{fault, nil}
		if fault.Kind == FaultPartition {
			af.group = make(map[string]struct{})
			for _, n := range fault.Group {
				af.group[nodeID(n)] = struct{}{}
			}
		}
		f.faults = append(f.faults, af)
	}

	slices.SortStableFunc(f.faults, func(a, b activeFault) int {
		return faultPriority[a.Kind] - faultPriority[b.Kind]
	})

	return f
}

func (f faultPolicy) fate(from, to string, now Dur) []delivery {
	faults := f.getActiveFaults(now)

	copies := 1
	var faultDelay activeFault
	hasDelay := false
	ack := true

	for _, fault := range faults {
		switch fault.Kind {
		case FaultPartition:
			_, ok1 := fault.group[to]
			_, ok2 := fault.group[from]
			if ok1 != ok2 {
				return []delivery{{delivered: false, acked: false}}
			}
		case FaultDrop:
			if f.rng.Float64() < fault.P {
				return []delivery{{delivered: false, acked: false}}
			}
		case FaultAckLie:
			if f.rng.Float64() < fault.P {
				return []delivery{{delivered: false, acked: true}}
			}
		case FaultDelay:
			faultDelay = fault
			hasDelay = true
		case FaultDup:
			if f.rng.Float64() < fault.P {
				copies = 2
			}
		case FaultAckLost:
			if f.rng.Float64() < fault.P {
				ack = false
			}
		}
	}

	out := make([]delivery, 0, copies)
	for range copies {
		d := delivery{
			delivered: true,
			acked:     ack,
		}
		if hasDelay {
			d.delay = faultDelay.MinD + Dur(f.rng.IntN(int(faultDelay.MaxD-faultDelay.MinD+1)))
		}
		out = append(out, d)
	}

	return out
}

func (f faultPolicy) getActiveFaults(now Dur) []activeFault {
	//TODO: consider more efficient approach, on reasonable amount of faults current approach may be okay
	out := make([]activeFault, 0, len(f.faults))
	for _, fault := range f.faults {
		if now >= fault.At && now < fault.Until {
			out = append(out, fault)
		}
	}
	return out
}
