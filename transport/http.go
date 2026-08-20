package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"time"
)

// HTTP is a Transport over the real network: each node runs a tiny HTTP server
// and POSTs gossip messages to its peers' /gossip endpoints. The Message
// envelope is JSON (its Payload []byte rides as base64) — readable in the demo
// logs, and swappable for a compact codec later without the engine noticing.
type HTTP struct {
	id     string
	addr   string            // listen address, e.g. ":8001"
	peers  map[string]string // peer id → base URL, e.g. "http://127.0.0.1:8002"
	client *http.Client
	server *http.Server
	log    *slog.Logger
}

var _ Transport = (*HTTP)(nil)

func NewHTTP(id, addr string, peers map[string]string) *HTTP {
	return &HTTP{
		id:     id,
		addr:   addr,
		peers:  peers,
		client: &http.Client{Timeout: 5 * time.Second},
		log:    slog.Default(),
	}
}

func (h *HTTP) ID() string {
	return h.id
}

func (t *HTTP) Send(ctx context.Context, peerID string, m Message) error {
	base, ok := t.peers[peerID]
	if !ok {
		return fmt.Errorf("transport: unknown peer %q", peerID)
	}
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/gossip", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return err // peer is down / unreachable — the engine decides whether to care
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("transport: peer %q returned %s", peerID, resp.Status)
	}
	return nil
}

func (t *HTTP) Peers() []string {
	ids := make([]string, 0, len(t.peers))
	for id := range t.peers {
		ids = append(ids, id)
	}
	sort.Strings(ids) // stable, deterministic order
	return ids
}

func (t *HTTP) Serve(h Handler) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/gossip", func(w http.ResponseWriter, r *http.Request) {
		var m Message
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := h(r.Context(), m); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	// Bind synchronously so Serve returns only once we are actually listening —
	// callers may Send to us immediately after Serve returns.
	ln, err := net.Listen("tcp", t.addr)
	if err != nil {
		return err
	}
	t.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := t.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.log.Error("gossip server stopped", "node", t.id, "err", err)
		}
	}()

	return nil
}

func (t *HTTP) Close() error {
	if t.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return t.server.Shutdown(ctx)
}
