package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// dashboard draws a live view of one replica in the terminal.
//
// The whole mechanism is two ANSI sequences and no dependencies: move the cursor
// up over the block printed last time, then clear from there down. Scrollback
// above the block is untouched, and there is no full-screen clear to flicker.
//
// Anything else writing to the same terminal (a log line) scrolls the block; the
// next frame just redraws it lower down. Logs go to stderr for that reason —
// redirect it if it bothers you. --plain appends frames instead of redrawing,
// for terminals that do not speak ANSI.
type dashboard struct {
	out       io.Writer
	plain     bool
	lastLines int
}

type frame struct {
	nodeID    string
	replicaID string
	gossip    string
	api       string
	value     uint64
	breakdown map[string]uint64 // replica id → its contribution
	peers     map[string]peerHealth
}

const (
	cursorUp   = "\x1b[%dA"
	clearBelow = "\x1b[0J"
	bold       = "\x1b[1m"
	dim        = "\x1b[2m"
	green      = "\x1b[32m"
	red        = "\x1b[31m"
	reset      = "\x1b[0m"

	barWidth = 28
)

func (d *dashboard) render(f frame) {
	body := d.body(f)

	if !d.plain && d.lastLines > 0 {
		fmt.Fprintf(d.out, cursorUp, d.lastLines)
		io.WriteString(d.out, clearBelow)
	}
	io.WriteString(d.out, body)
	d.lastLines = strings.Count(body, "\n")
}

func (d *dashboard) body(f frame) string {
	var b strings.Builder

	fmt.Fprintf(&b, "%scrdtlab%s  node %s%s%s  replica %s%s%s\n",
		d.c(bold), d.c(reset), d.c(bold), f.nodeID, d.c(reset), d.c(bold), f.replicaID, d.c(reset))
	fmt.Fprintf(&b, "%sgossip %s   api %s%s\n\n", d.c(dim), f.gossip, f.api, d.c(reset))

	fmt.Fprintf(&b, "  value  %s%d%s\n\n", d.c(bold), f.value, d.c(reset))

	// One row per replica id ever seen, including this node's earlier
	// incarnations: after a restart the old row stays and a new one appears next
	// to it. That is the whole point of an incarnation id, made visible.
	ids := make([]string, 0, len(f.breakdown))
	var maxV uint64
	for id, v := range f.breakdown {
		ids = append(ids, id)
		if v > maxV {
			maxV = v
		}
	}
	sort.Strings(ids)

	width := 0
	for _, id := range ids {
		if len(id) > width {
			width = len(id)
		}
	}
	for _, id := range ids {
		marker := "  "
		if id == f.replicaID {
			marker = "▸ "
		}
		fmt.Fprintf(&b, "%s%-*s  %6d  %s%s%s\n",
			marker, width, id, f.breakdown[id], d.c(dim), bar(f.breakdown[id], maxV), d.c(reset))
	}
	if len(ids) == 0 {
		fmt.Fprintf(&b, "%s  (no state yet)%s\n", d.c(dim), d.c(reset))
	}

	b.WriteString("\n  peers  ")
	peerIDs := make([]string, 0, len(f.peers))
	for id := range f.peers {
		peerIDs = append(peerIDs, id)
	}
	sort.Strings(peerIDs)
	if len(peerIDs) == 0 {
		b.WriteString(d.c(dim) + "none" + d.c(reset))
	}
	for i, id := range peerIDs {
		if i > 0 {
			b.WriteString("   ")
		}
		h := f.peers[id]
		switch {
		case h.attempts == 0:
			fmt.Fprintf(&b, "%s%s ?%s", d.c(dim), id, d.c(reset))
		case h.ok:
			fmt.Fprintf(&b, "%s%s up%s", d.c(green), id, d.c(reset))
		default:
			fmt.Fprintf(&b, "%s%s down%s%s (%s)%s",
				d.c(red), id, d.c(reset), d.c(dim), since(h.lastOK), d.c(reset))
		}
	}
	b.WriteString("\n")

	return b.String()
}

// c returns an escape sequence, or nothing in plain mode.
func (d *dashboard) c(seq string) string {
	if d.plain {
		return ""
	}
	return seq
}

func bar(v, max uint64) string {
	if max == 0 {
		return ""
	}
	n := int(float64(v) / float64(max) * barWidth)
	if n == 0 && v > 0 {
		n = 1
	}
	return strings.Repeat("█", n)
}

func since(t time.Time) string {
	if t.IsZero() {
		return "never reached"
	}
	return fmt.Sprintf("last seen %s ago", time.Since(t).Truncate(100*time.Millisecond))
}
