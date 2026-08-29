package simtest

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// traceFormat versions the JSONL layout: bump it when Event or the header
// changes shape.
const traceFormat = 1

type traceHeader struct {
	Format   int    `json:"format"`
	Seed     uint64 `json:"seed"`
	Nodes    int    `json:"nodes"`
	Scenario string `json:"scenario_sha256"`
}

// WriteJSONL writes the trace as JSON Lines: a header naming the scenario
// (format version, seed, node count, sha256 of the scenario's canonical
// JSON), then one event per line. The bytes are a pure function of the
// trace and the scenario, so two runs can be compared as streams.
func (t Trace) WriteJSONL(w io.Writer, s Scenario) error {
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)
	header := traceHeader{Format: traceFormat, Seed: s.Seed, Nodes: s.Nodes, Scenario: scenarioDigest(s)}
	if err := enc.Encode(header); err != nil {
		return err
	}
	for _, e := range t.Events {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// scenarioDigest is sha256 over the scenario's JSON with nil slices written
// as empty: a literal and a generated scenario that mean the same thing
// digest the same.
func scenarioDigest(s Scenario) string {
	if s.Topology == nil {
		s.Topology = [][2]int{}
	}
	if s.Ops == nil {
		s.Ops = []OpEntry{}
	}
	if s.Faults == nil {
		s.Faults = []FaultEntry{}
	}
	for i := range s.Faults {
		if s.Faults[i].Group == nil {
			s.Faults[i].Group = []int{}
		}
	}
	b, err := json.Marshal(s)
	if err != nil {
		panic("simtest: scenario is not serializable: " + err.Error())
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// laneGutter is the spacing between swim lanes in String.
const laneGutter = 2

// String renders the trace as swim lanes: one column per node, one line per
// event, virtual time on the left. Meant for eyes; the JSONL is the format.
func (t Trace) String() string {
	var ids []string
	lane := map[string]int{}
	lines := make([]string, len(t.Events))
	width := 0
	for i, e := range t.Events {
		if _, ok := lane[e.Node]; !ok {
			lane[e.Node] = len(ids)
			ids = append(ids, e.Node)
		}
		head, tail := cell(e)
		lines[i] = head + tail
		width = max(width, len(head), len(e.Node))
	}
	width += laneGutter

	// T never decreases along the trace, so the last event has the widest T.
	tw := 1
	if n := len(t.Events); n > 0 {
		tw = len(fmt.Sprint(t.Events[n-1].T))
	}
	tw += laneGutter

	var b strings.Builder
	fmt.Fprintf(&b, "%-*s", tw, "t")
	for _, id := range ids {
		fmt.Fprintf(&b, "%-*s", width, id)
	}
	b.WriteString("\n")
	for i, e := range t.Events {
		fmt.Fprintf(&b, "%-*d", tw, e.T)
		b.WriteString(strings.Repeat(" ", lane[e.Node]*width))
		b.WriteString(lines[i])
		b.WriteString("\n")
	}
	return b.String()
}

// cell renders one event. head is the part lanes are sized for; tail is the
// error text, which may overflow its lane (there is one cell per line).
func cell(e Event) (head, tail string) {
	msg := e.MsgKind
	if e.Size > 0 {
		msg = fmt.Sprintf("%s(%dB)", msg, e.Size)
	}
	switch e.Kind {
	case EventSend, EventDrop, EventDup:
		head = fmt.Sprintf("%s %s →%s", e.Kind, msg, e.Peer)
	case EventDeliver:
		head = fmt.Sprintf("%s %s ←%s", e.Kind, msg, e.Peer)
	case EventSendResult:
		outcome := "ok"
		if e.Err != "" {
			outcome = "err"
		}
		head = fmt.Sprintf("%s %s →%s %s", e.Kind, msg, e.Peer, outcome)
	case EventOp:
		head = fmt.Sprintf("%s %s", e.Kind, e.Op)
	default:
		head = string(e.Kind)
	}
	if e.Err != "" {
		tail = ": " + e.Err
	}
	return head, tail
}
