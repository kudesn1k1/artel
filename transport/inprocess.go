package transport

import (
	"context"
	"fmt"
	"sync"

	"github.com/kudesn1k1/artel"
)

// InProcessRegistry is a shared in-memory switchboard: every InProcess transport in one
// test registers its handler here, and Send routes through it. Delivery is
// SYNCHRONOUS — Send runs the peer's handler on the caller's goroutine and
// returns only after it finishes. That is what makes convergence tests
// deterministic: no wall-clock timing, no background goroutines to wait on.
type InProcessRegistry struct {
	mu       sync.Mutex
	handlers map[string]artel.Handler
}

func NewInProcessRegistry() *InProcessRegistry {
	return &InProcessRegistry{handlers: make(map[string]artel.Handler)}
}

func (r *InProcessRegistry) register(id string, h artel.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[id] = h
}

func (r *InProcessRegistry) deliver(ctx context.Context, to string, m artel.Message) error {
	r.mu.Lock()
	h := r.handlers[to]
	r.mu.Unlock() // release BEFORE calling h: a handler may Send again (Pull → Push),
	//               and re-entering deliver must not deadlock on this lock.
	if h == nil {
		return fmt.Errorf("transport: no peer registered as %q", to)
	}
	return h(ctx, m)
}

// InProcess is a Transport backed by a shared InProcessRegistry. Use one InProcessRegistry per
// test and one InProcess per node.
type InProcess struct {
	id    string
	peers []string
	reg   *InProcessRegistry
}

var _ artel.Transport = (*InProcess)(nil)

func NewInProcess(id string, peers []string, reg *InProcessRegistry) *InProcess {
	return &InProcess{id: id, peers: peers, reg: reg}
}

func (i *InProcess) ID() string {
	return i.id
}

func (t *InProcess) Send(ctx context.Context, peerID string, m artel.Message) error {
	return t.reg.deliver(ctx, peerID, m)
}

func (t *InProcess) Peers() []string { return t.peers }

func (t *InProcess) Serve(h artel.Handler) error {
	t.reg.register(t.id, h)
	return nil
}

func (t *InProcess) Close() error { return nil }
