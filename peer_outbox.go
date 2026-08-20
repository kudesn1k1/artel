package artel

import (
	"sync"
)

// peerOutbox is the per-peer send state; pending is the join-semilattice
// delta waiting to be shipped to that peer.
//
// A join-semilattice has no inverse: once takePush removes a snapshot from the
// buffer there is nothing to "subtract back", so every failure path must
// return what it took. Push is take-and-return-on-failure (pushFailed rejoins
// the snapshot); pull is a boolean cleared only on success. pushInFlight must
// be cleared on EVERY outcome — a path that forgets it blocks that peer forever.
type peerOutbox[S DeltaState[S]] struct {
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
