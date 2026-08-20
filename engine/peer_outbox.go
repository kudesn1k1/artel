package engine

import (
	"github.com/kudesn1k1/artel/crdt"
	"sync"
)

// peerOutbox represents a state of other peer we are communicating with. pending is a join-semilattice state waiting to be sent. It is cleared on send when pushInFlight is set to true and returned after in case of success or failure.
// needsPull does not need such optimistic behavior so it is cleared on success
type peerOutbox[S crdt.DeltaState[S]] struct {
	mu           sync.Mutex
	pending      S
	needsPull    bool
	pushInFlight bool
	pullInFlight bool
}

func (p *peerOutbox[S]) takePush(fresh S) (S, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	push := p.pending.Join(fresh)
	bottom := *new(S)

	if push.IsBottom() {
		return bottom, false
	}

	if p.pushInFlight {
		p.pending = push
		return bottom, false
	}

	p.pushInFlight = true
	p.pending = bottom
	return push, true
}

func (p *peerOutbox[S]) pushFailed(snapshot S) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = p.pending.Join(snapshot)
	p.pushInFlight = false
}

func (p *peerOutbox[S]) pushDone() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pushInFlight = false
}

func (p *peerOutbox[S]) takePull() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.needsPull || p.pullInFlight {
		return false
	}
	p.pullInFlight = true
	return true
}

func (p *peerOutbox[S]) pullFailed() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pullInFlight = false
}

func (p *peerOutbox[S]) pullDone() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pullInFlight = false
	p.needsPull = false
}

func (p *peerOutbox[S]) markNeedsPull() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.needsPull = true
}
