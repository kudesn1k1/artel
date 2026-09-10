package artel

import (
	"cmp"
	"slices"
)

// ReplicaCount is one replica's entry in a counter's wire form.
type ReplicaCount struct {
	Replica string `json:"replica"`
	N       uint64 `json:"n"`
}

func sortedCounts(m map[string]uint64) []ReplicaCount {
	var out []ReplicaCount
	for r, n := range m {
		out = append(out, ReplicaCount{Replica: r, N: n})
	}
	slices.SortFunc(out, func(a, b ReplicaCount) int { return cmp.Compare(a.Replica, b.Replica) })
	return out
}

func countsToMap(counts []ReplicaCount) map[string]uint64 {
	m := make(map[string]uint64, len(counts))
	for _, c := range counts {
		m[c.Replica] = c.N
	}
	return m
}
