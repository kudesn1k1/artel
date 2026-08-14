package transport

import (
	"fmt"
	"sync"
)

// Registry is a shared in-memory switchboard: every InProcess transport in one
// test registers its handler here, and Send routes through it. Delivery is
// SYNCHRONOUS — Send runs the peer's handler on the caller's goroutine and
// returns only after it finishes. That is what makes convergence tests
// deterministic: no wall-clock timing, no background goroutines to wait on.
type Registry struct {
	mu       sync.Mutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) register(id string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[id] = h
}

func (r *Registry) deliver(to string, m Message) error {
	r.mu.Lock()
	h := r.handlers[to]
	r.mu.Unlock() // release BEFORE calling h: a handler may Send again (Pull → Push),
	//               and re-entering deliver must not deadlock on this lock.
	if h == nil {
		return fmt.Errorf("transport: no peer registered as %q", to)
	}
	return h(m)
}

// InProcess is a Transport backed by a shared Registry. Use one Registry per
// test and one InProcess per node.
type InProcess struct {
	id    string
	peers []string
	reg   *Registry
}

var _ Transport = (*InProcess)(nil)

func NewInProcess(id string, peers []string, reg *Registry) *InProcess {
	return &InProcess{id: id, peers: peers, reg: reg}
}

func (i *InProcess) ID() string {
	return i.id
}

func (t *InProcess) Send(peerID string, m Message) error { return t.reg.deliver(peerID, m) }

func (t *InProcess) Peers() []string { return t.peers }

func (t *InProcess) Serve(h Handler) error {
	t.reg.register(t.id, h)
	return nil
}

func (t *InProcess) Close() error { return nil }
