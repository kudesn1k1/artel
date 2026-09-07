package simtest

import (
	"fmt"
	"strconv"

	"github.com/kudesn1k1/artel"
)

// peerOutbox is refCore's per-peer send state. inFlight is not bottom exactly
// while one push to this peer awaits its outcome. A join-semilattice has no
// inverse, so a failed push is rejoined into pending and inFlight is cleared
// on every outcome — a path that forgets either stalls the link forever.
type peerOutbox[S artel.DeltaState[S]] struct {
	pending  S
	inFlight S
}

func (p *peerOutbox[S]) pushFailed() {
	p.pending = p.pending.Join(p.inFlight)
	p.inFlight = *new(S)
}

func (p *peerOutbox[S]) pushDone() {
	p.inFlight = *new(S)
}

// refCore is the engine's protocol as a sans-IO core: direct mesh, push only,
// one push in flight per peer, retain on failure. It is the known-good
// subject the calibration gates measure mutants against. Unexported on
// purpose: a test fixture, not API.
//
// One push in flight per peer is deliberate (in the engine it keeps a slow
// peer from draining the send pool) and makes each link FIFO: the next push
// leaves only after the previous outcome, so the network alone cannot reorder
// what a peer merges. A received state merges into the local replica and
// never re-enters the buffer — no relay, so every node must be a peer of
// every other.
type refCore[S artel.State[S], PS artel.StatePtr[S], R artel.DeltaReplica[S]] struct {
	id     string
	local  R
	peers  []string
	outbox map[string]*peerOutbox[S]
}

var _ artel.Core = (*refCore[artel.GCounterState, *artel.GCounterState, *artel.GCounter])(nil)

func newRefCore[S artel.State[S], PS artel.StatePtr[S], R artel.DeltaReplica[S]](id string, local R, peers []string) *refCore[S, PS, R] {
	outbox := make(map[string]*peerOutbox[S], len(peers))
	for _, peer := range peers {
		outbox[peer] = &peerOutbox[S]{}
	}

	return &refCore[S, PS, R]{
		id:     id,
		local:  local,
		peers:  peers,
		outbox: outbox,
	}
}

func (r *refCore[S, PS, R]) Tick() []artel.Envelope {
	fresh := r.local.FlushDelta()
	envs := make([]artel.Envelope, 0, len(r.peers))

	peers := r.peers
	for _, peer := range peers {
		outbox := r.outbox[peer]
		outbox.pending = outbox.pending.Join(fresh)
		if outbox.pending.IsBottom() || !outbox.inFlight.IsBottom() {
			continue
		}

		payload, err := outbox.pending.MarshalBinary()
		if err != nil {
			panic(fmt.Errorf("simtest: failed to marshal payload: %w", err))
		}

		outbox.inFlight = outbox.pending
		outbox.pending = *new(S)
		envs = append(envs, artel.Envelope{
			To: peer,
			Msg: artel.Message{
				From:    r.id,
				Kind:    artel.KindPush,
				Payload: payload,
			},
		})
	}

	return envs
}

func (r *refCore[S, PS, R]) Deliver(msg artel.Message) []artel.Envelope {
	if msg.Kind != artel.KindPush {
		panic("simtest: push-only core received a non-push message")
	}

	state, err := r.decode(msg.Payload)
	if err != nil {
		panic(fmt.Errorf("simtest: failed to decode payload: %w", err))
	}
	r.local.Merge(state)
	return nil
}
func (r *refCore[S, PS, R]) SendResult(to string, kind artel.Kind, err error) {
	if kind != artel.KindPush {
		panic("simtest: push-only core received a non-push message result")
	}

	outbox, ok := r.outbox[to]
	if !ok {
		panic(fmt.Sprintf("simtest: unknown peer: %s", to))
	}

	if err != nil {
		outbox.pushFailed()
	} else {
		outbox.pushDone()
	}
}

func (r *refCore[S, PS, R]) decode(b []byte) (S, error) {
	var s S
	if err := PS(&s).UnmarshalBinary(b); err != nil {
		var bottom S
		return bottom, err
	}
	return s, nil
}

// counterReplica adapts a counter type to the subject's vocabulary: "inc:N"
// and "dec:N" ops, a decimal value, canonical state bytes.
type counterReplica interface {
	apply(n int) error
	value() string
	snapshot() ([]byte, error)
}

type gCounterReplica struct{ *artel.GCounter }

func (g gCounterReplica) apply(n int) error {
	if n < 0 {
		return fmt.Errorf("simtest: GCounter cannot decrement: %d", n)
	}
	g.IncrementBy(uint64(n))
	return nil
}

func (g gCounterReplica) value() string { return strconv.FormatUint(g.Value(), 10) }

func (g gCounterReplica) snapshot() ([]byte, error) { return g.State().MarshalBinary() }

type pnCounterReplica struct{ *artel.PNCounter }

func (p pnCounterReplica) apply(n int) error {
	if n < 0 {
		p.DecrementBy(uint64(-n))
	} else {
		p.IncrementBy(uint64(n))
	}
	return nil
}

func (p pnCounterReplica) value() string { return strconv.FormatInt(p.Value(), 10) }

func (p pnCounterReplica) snapshot() ([]byte, error) { return p.State().MarshalBinary() }

// counterNode drives one counter replica through a core. The node id is the
// protocol id; the replica id carries the incarnation, so a restarted node
// never reuses its key.
type counterNode struct {
	id   string
	core artel.Core
	rep  counterReplica
}

var _ Node = (*counterNode)(nil)

func (n *counterNode) Core() artel.Core { return n.core }

func (n *counterNode) Apply(op string) error {
	x, err := parseCounterOp(op)
	if err != nil {
		return err
	}
	return n.rep.apply(x)
}

func (n *counterNode) Observe() Observation {
	state, err := n.rep.snapshot()
	if err != nil {
		panic(err)
	}
	return Observation{Node: n.id, State: state, Value: n.rep.value()}
}

func replicaID(id string, incarnation int) string { return fmt.Sprintf("%s#%d", id, incarnation) }

type gCounterSubject struct{}

var _ Subject = gCounterSubject{}

func (gCounterSubject) NewNode(id string, incarnation int, peers []string) Node {
	rep := artel.NewGCounter(replicaID(id, incarnation))
	return &counterNode{id: id, core: newRefCore(id, rep, peers), rep: gCounterReplica{rep}}
}

type pnCounterSubject struct{}

var _ Subject = pnCounterSubject{}

func (pnCounterSubject) NewNode(id string, incarnation int, peers []string) Node {
	rep := artel.NewPNCounter(replicaID(id, incarnation))
	return &counterNode{id: id, core: newRefCore(id, rep, peers), rep: pnCounterReplica{rep}}
}
