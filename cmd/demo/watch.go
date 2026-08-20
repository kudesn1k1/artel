package main

import (
	"context"
	"sync"
	"time"

	"crdtlab/transport"
)

// peerHealth is everything the dashboard says about one peer.
type peerHealth struct {
	attempts int
	ok       bool
	lastOK   time.Time
	lastErr  string
}

// watchedTransport is a Transport decorator that records how every send went.
//
// The engine knows nothing about it: peer reachability is a demo concern, and
// the Transport interface is exactly the seam that lets the demo observe the
// wire without the engine growing an API for it.
type watchedTransport struct {
	transport.Transport

	mu    sync.Mutex
	peers map[string]*peerHealth
}

func newWatchedTransport(inner transport.Transport) *watchedTransport {
	w := &watchedTransport{Transport: inner, peers: make(map[string]*peerHealth)}
	for _, id := range inner.Peers() {
		w.peers[id] = &peerHealth{}
	}
	return w
}

func (w *watchedTransport) Send(ctx context.Context, peerID string, m transport.Message) error {
	err := w.Transport.Send(ctx, peerID, m)

	w.mu.Lock()
	defer w.mu.Unlock()
	h, known := w.peers[peerID]
	if !known {
		h = &peerHealth{}
		w.peers[peerID] = h
	}
	h.attempts++
	h.ok = err == nil
	if err != nil {
		h.lastErr = err.Error()
	} else {
		h.lastErr = ""
		h.lastOK = time.Now()
	}
	return err
}

// health copies the table out, so the renderer never holds the lock while it draws.
func (w *watchedTransport) health() map[string]peerHealth {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]peerHealth, len(w.peers))
	for id, h := range w.peers {
		out[id] = *h
	}
	return out
}
