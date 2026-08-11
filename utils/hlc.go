package utils

// HLC is a hybrid logical clock, which combines physical time with a logical counter to provide a total ordering of events in a distributed system.
type HLC struct {
	now  func() int64
	l, c int64
}

func NewHLC(now func() int64) *HLC {
	return &HLC{now: now}
}

func (h *HLC) Now() HLCTimestamp {
	newL := h.now()
	if newL > h.l {
		h.l, h.c = newL, 0
	} else {
		h.c++
	}
	return HLCTimestamp{L: h.l, C: h.c}
}

func (h *HLC) Update(remote HLCTimestamp) HLCTimestamp {
	newL := max(h.l, h.now(), remote.L)
	if newL == h.l && newL == remote.L {
		h.c = max(h.c, remote.C) + 1
	} else if newL == h.l {
		h.c++
	} else if newL == remote.L {
		h.c = remote.C + 1
	} else {
		h.c = 0
	}
	h.l = newL
	return HLCTimestamp{L: h.l, C: h.c}
}

type HLCTimestamp struct {
	L, C int64
}

func (t HLCTimestamp) Less(other HLCTimestamp) bool {
	return t.L < other.L || (t.L == other.L && t.C < other.C)
}

type HLCDot struct {
	Replica string
	HLCTimestamp
}

func (d HLCDot) Less(other HLCDot) bool {
	return d.HLCTimestamp.Less(other.HLCTimestamp) || (d.HLCTimestamp == other.HLCTimestamp && d.Replica < other.Replica)
}
