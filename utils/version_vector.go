package utils

import (
	"fmt"
	"sort"
	"strings"
)

type VersionVector map[string]uint64

func (v VersionVector) leq(o VersionVector) bool {
	for k, n := range v {
		if n > o[k] {
			return false
		}
	}
	return true
}

func (v VersionVector) Dominates(o VersionVector) bool {
	return !v.leq(o) && o.leq(v)
}

func (v VersionVector) Equal(o VersionVector) bool {
	return v.leq(o) && o.leq(v)
}

func (v VersionVector) Concurrent(o VersionVector) bool {
	return !v.leq(o) && !o.leq(v)
}

func (v VersionVector) Increment(id string) {
	v[id]++
}

func (v VersionVector) Merge(o VersionVector) {
	for k, n := range o {
		v[k] = max(v[k], n)
	}
}

func (v VersionVector) Key() string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var builder strings.Builder

	for _, k := range keys {
		builder.WriteString(fmt.Sprintf("%s:%d", k, v[k]))
	}

	return builder.String()
}

type VersionDot struct {
	Replica string
	Counter uint64
}
