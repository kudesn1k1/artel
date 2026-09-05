package simtest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/kudesn1k1/artel"
)

// goldenScenario: two nodes, one op, one drop window — every event kind but
// dup shows up, and the whole trace is short enough to spell out by hand.
func goldenScenario() Scenario {
	return Scenario{
		Seed: 7, Nodes: 2, Topology: FullMesh(2), Interval: 10, Horizon: 12, Settle: 0,
		Ops:    []OpEntry{{At: 5, Node: 1, Op: "noop"}},
		Faults: []FaultEntry{{At: 10, Until: 20, Kind: FaultDrop, P: 1}},
	}
}

// The JSONL format is a replay artifact: the header names the scenario, then
// one event per line in canonical field order; every row about a message
// (deliver, drop, dup, sendresult) names its send row in "sent". Pinned
// byte-for-byte.
func TestWriteJSONLGolden(t *testing.T) {
	s := goldenScenario()
	res := Run(s, &pingSubject{})

	var buf bytes.Buffer
	if err := res.Trace.WriteJSONL(&buf, s); err != nil {
		t.Fatalf("WriteJSONL: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")

	// Header: the scenario digest is sha256 over the scenario's JSON, with
	// nil slices written as empty so a literal and a generated scenario that
	// mean the same thing hash the same.
	var header struct {
		Format   int    `json:"format"`
		Seed     uint64 `json:"seed"`
		Nodes    int    `json:"nodes"`
		Scenario string `json:"scenario_sha256"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
		t.Fatalf("header is not JSON: %q: %v", lines[0], err)
	}
	canonJSON, _ := json.Marshal(s)
	sum := sha256.Sum256(canonJSON)
	if header.Format != 1 || header.Seed != 7 || header.Nodes != 2 || header.Scenario != hex.EncodeToString(sum[:]) {
		t.Fatalf("header %+v, want format=1 seed=7 nodes=2 sha256=%s", header, hex.EncodeToString(sum[:]))
	}

	want := []string{
		`{"t":0,"seq":0,"kind":"tick","node":"n0"}`,
		`{"t":0,"seq":1,"kind":"send","node":"n0","peer":"n1","msg_kind":"push"}`,
		`{"t":0,"seq":2,"kind":"tick","node":"n1"}`,
		`{"t":0,"seq":3,"kind":"send","node":"n1","peer":"n0","msg_kind":"push"}`,
		`{"t":1,"seq":4,"kind":"deliver","node":"n1","peer":"n0","msg_kind":"push","sent":1}`,
		`{"t":1,"seq":5,"kind":"deliver","node":"n0","peer":"n1","msg_kind":"push","sent":3}`,
		`{"t":1,"seq":6,"kind":"sendresult","node":"n0","peer":"n1","msg_kind":"push","sent":1}`,
		`{"t":1,"seq":7,"kind":"sendresult","node":"n1","peer":"n0","msg_kind":"push","sent":3}`,
		`{"t":5,"seq":8,"kind":"op","node":"n1","op":"noop"}`,
		`{"t":10,"seq":9,"kind":"tick","node":"n0"}`,
		`{"t":10,"seq":10,"kind":"send","node":"n0","peer":"n1","msg_kind":"push"}`,
		`{"t":10,"seq":11,"kind":"drop","node":"n0","peer":"n1","msg_kind":"push","sent":10}`,
		`{"t":10,"seq":12,"kind":"tick","node":"n1"}`,
		`{"t":10,"seq":13,"kind":"send","node":"n1","peer":"n0","msg_kind":"push"}`,
		`{"t":10,"seq":14,"kind":"drop","node":"n1","peer":"n0","msg_kind":"push","sent":13}`,
		`{"t":11,"seq":15,"kind":"sendresult","node":"n0","peer":"n1","msg_kind":"push","sent":10,"err":"failed to deliver: dropped"}`,
		`{"t":11,"seq":16,"kind":"sendresult","node":"n1","peer":"n0","msg_kind":"push","sent":13,"err":"failed to deliver: dropped"}`,
		`{"t":12,"seq":17,"kind":"observe","node":"n0"}`,
		`{"t":12,"seq":18,"kind":"observe","node":"n1"}`,
	}
	got := lines[1:]
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			t.Fatalf("line %d:\n got  %s\n want %s", i+1, got[i], want[i])
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%d event lines, want %d", len(got), len(want))
	}
}

func TestWriteJSONLCarriesSizeAndKeepsErrorsReadable(t *testing.T) {
	s := Scenario{Seed: 1, Nodes: 2, Topology: FullMesh(2), Interval: 10, Horizon: 5,
		Faults: []FaultEntry{{At: 0, Until: 10, Kind: FaultAckLost, P: 1}}}
	res := Run(s, &pingSubject{sized: true, kind: artel.KindPull})

	var buf bytes.Buffer
	if err := res.Trace.WriteJSONL(&buf, s); err != nil {
		t.Fatalf("WriteJSONL: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, `"kind":"send","node":"n0","peer":"n1","msg_kind":"pull","size":1}`) {
		t.Fatalf("size and kind are not on the send line:\n%s", out)
	}
	if strings.Contains(out, `\u0026`) || strings.Contains(out, `\u003c`) {
		t.Fatalf("HTML escaping leaked into the trace:\n%s", out)
	}
	if !strings.Contains(out, `"err":"failed to ack: lost"`) {
		t.Fatalf("the ack-lost error is not on the sendresult line:\n%s", out)
	}
}

// requireLinked checks the send link of every row about a message: Sent names
// an earlier send row of the same message, seen from the right side (a deliver
// is the receiver's row, the verdicts and the outcome are the sender's), and
// every send gets exactly one outcome — dup is two copies, one sendresult.
func requireLinked(t *testing.T, events []Event) {
	t.Helper()
	outcomes := map[uint64]int{}
	for _, e := range events {
		switch e.Kind {
		case EventDeliver, EventDrop, EventDup, EventSendResult:
		default:
			continue
		}
		if e.Sent >= e.Seq {
			t.Fatalf("row %d (%s) names send row %d, which is not before it", e.Seq, e.Kind, e.Sent)
		}
		from, to := e.Node, e.Peer
		if e.Kind == EventDeliver {
			from, to = e.Peer, e.Node
		}
		s := events[e.Sent]
		if s.Kind != EventSend || s.Node != from || s.Peer != to || s.MsgKind != e.MsgKind || s.Size != e.Size {
			t.Fatalf("row %d (%s %s→%s %s/%dB) names row %d, which is not its send: %+v", e.Seq, e.Kind, from, to, e.MsgKind, e.Size, e.Sent, s)
		}
		if e.Kind == EventSendResult {
			outcomes[e.Sent]++
		}
	}
	for _, e := range events {
		if e.Kind == EventSend && outcomes[e.Seq] != 1 {
			t.Fatalf("send row %d got %d outcomes, want exactly one", e.Seq, outcomes[e.Seq])
		}
	}
}

// The link is what lets a reader pair rows without guessing the network's
// order: jitter reorders, dup doubles, drop and delivery interleave. Checked
// under the full anomaly mix with sized pings, so a wrong pairing also shows
// as a size mismatch.
func TestTraceLinksEveryVerdictToItsSend(t *testing.T) {
	for seed := uint64(1); seed <= 5; seed++ {
		res := Run(mixScenario(seed), &pingSubject{sized: true})
		if len(ofKind(res.Trace.Events, EventDeliver)) == 0 || len(ofKind(res.Trace.Events, EventDrop)) == 0 {
			t.Fatalf("seed %d: the mix produced no deliveries or no drops, the link is untested", seed)
		}
		requireLinked(t, res.Trace.Events)
	}
}

// fatalTB captures Fatalf instead of ending the test, so a test can assert
// that RequireDeterministic fails when it should.
type fatalTB struct {
	testing.TB
	msg string
}

type fatalCalled struct{}

func (f *fatalTB) Helper() {}

func (f *fatalTB) Fatalf(format string, args ...any) {
	f.msg = fmt.Sprintf(format, args...)
	panic(fatalCalled{})
}

// captureFatal runs f against a fatalTB and returns the Fatalf message, or ""
// when f returned without failing.
func captureFatal(t *testing.T, f func(tb testing.TB)) (msg string) {
	t.Helper()
	tb := &fatalTB{TB: t}
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(fatalCalled); !ok {
				panic(r)
			}
			msg = tb.msg
		}
	}()
	f(tb)
	return ""
}

// flakySubject changes behaviour from one Run to the next: the first node it
// ever creates sends pings as pull, every later one as push.
type flakySubject struct{ made int }

func (f *flakySubject) NewNode(id string, _ int, peers []string) Node {
	f.made++
	kind := artel.KindPush
	if f.made == 1 {
		kind = artel.KindPull
	}
	return &pingNode{core: &pingCore{self: id, peers: peers, kind: kind}}
}

// driftingSubject keeps the trace identical but observes a different state on
// every Run.
type driftingSubject struct{ runs int }

type driftingNode struct {
	pingNode
	stamp int
}

func (n *driftingNode) Observe() Observation {
	s := "stamp:" + strings.Repeat("x", n.stamp)
	return Observation{Node: n.core.self, State: []byte(s), Value: s}
}

func (d *driftingSubject) NewNode(id string, _ int, peers []string) Node {
	if id == "n0" {
		d.runs++
	}
	return &driftingNode{pingNode: pingNode{core: &pingCore{self: id, peers: peers}}, stamp: d.runs}
}

func TestRequireDeterministic(t *testing.T) {
	RequireDeterministic(t, mixScenario(7), &pingSubject{})

	t.Run("trace drift", func(t *testing.T) {
		msg := captureFatal(t, func(tb testing.TB) {
			RequireDeterministic(tb, goldenScenario(), &flakySubject{})
		})
		if msg == "" {
			t.Fatal("a subject whose trace differs between runs passed RequireDeterministic")
		}
		if !strings.Contains(msg, "line") {
			t.Fatalf("the failure does not point at the diverging line: %s", msg)
		}
	})

	t.Run("final state drift", func(t *testing.T) {
		msg := captureFatal(t, func(tb testing.TB) {
			RequireDeterministic(tb, goldenScenario(), &driftingSubject{})
		})
		if msg == "" {
			t.Fatal("a subject whose final state differs between runs passed RequireDeterministic")
		}
		if !strings.Contains(msg, "n0") {
			t.Fatalf("the failure does not name the drifting node: %s", msg)
		}
	})
}

// String is for eyes, not tools: the lane layout is a taste call, so only the
// facts are pinned — every node has a lane, every kind is rendered, every row
// about a message carries its send's #tag, and the text is a pure function
// of the trace.
func TestStringRendersEveryLaneAndKind(t *testing.T) {
	s := Scenario{Seed: 3, Nodes: 3, Topology: FullMesh(3), Interval: 10, Horizon: 25,
		Ops: []OpEntry{{At: 5, Node: 2, Op: "inc:5"}},
		Faults: []FaultEntry{
			{At: 0, Until: 5, Kind: FaultDup, P: 1},
			{At: 10, Until: 20, Kind: FaultDrop, P: 1},
		}}
	res := Run(s, &pingSubject{})
	out := res.Trace.String()

	// Row 1 is n0's first send (to n1); the dup verdict, both copies and the
	// outcome are about it.
	for _, want := range []string{"n0", "n1", "n2", "tick", "send", "dup", "deliver", "sendresult", "drop", "op", "inc:5", "observe", "failed to deliver: dropped",
		"send push →n1 #1", "dup push →n1 #1", "deliver push ←n0 #1", "sendresult push →n1 #1 ok"} {
		if !strings.Contains(out, want) {
			t.Fatalf("String() lacks %q:\n%s", want, out)
		}
	}
	if again := Run(s, &pingSubject{}).Trace.String(); again != out {
		t.Fatal("String() differs between two runs of one scenario")
	}
	if len(strings.Split(strings.TrimSpace(out), "\n")) < len(res.Trace.Events) {
		t.Fatalf("String() has fewer lines than events:\n%s", out)
	}
}
