// Command demo runs one replica of a crdtlab cluster: a delta-state G-Counter, an
// anti-entropy engine gossiping over HTTP, a small control API, and a live view of
// the replica's state in the terminal.
//
// Three nodes in three terminals:
//
//	go run ./cmd/demo --id A --gossip 127.0.0.1:8001 --api 127.0.0.1:9001 --peer B=http://127.0.0.1:8002 --peer C=http://127.0.0.1:8003 --inc-every 1s
//	go run ./cmd/demo --id B --gossip 127.0.0.1:8002 --api 127.0.0.1:9002 --peer A=http://127.0.0.1:8001 --peer C=http://127.0.0.1:8003 --inc-every 1s
//	go run ./cmd/demo --id C --gossip 127.0.0.1:8003 --api 127.0.0.1:9003 --peer A=http://127.0.0.1:8001 --peer B=http://127.0.0.1:8002 --inc-every 1s
//
// Then watch the three counters track each other. Ctrl-C one of them and the
// survivors report it down while they keep converging; start it again and it
// catches up by pulling full state, appearing in everyone's breakdown under a new
// incarnation id.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"crdtlab/engine"
	"crdtlab/transport"
	"crdtlab/types/delta/counter"
)

// peerFlag collects repeated --peer ID=URL flags into the transport's address book.
type peerFlag map[string]string

func (p peerFlag) String() string {
	parts := make([]string, 0, len(p))
	for id, url := range p {
		parts = append(parts, id+"="+url)
	}
	return strings.Join(parts, ",")
}

func (p peerFlag) Set(v string) error {
	id, url, ok := strings.Cut(v, "=")
	if !ok || id == "" || url == "" {
		return fmt.Errorf("want ID=URL, got %q", v)
	}
	p[id] = url
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

func run() error {
	peers := peerFlag{}
	var (
		nodeID   = flag.String("id", "", "node id: the stable address-book name (required)")
		gossip   = flag.String("gossip", "127.0.0.1:8001", "listen address for gossip")
		apiAddr  = flag.String("api", "127.0.0.1:9001", "listen address for the control API")
		interval = flag.Duration("interval", 500*time.Millisecond, "gossip round interval")
		incEvery = flag.Duration("inc-every", 0, "increment this replica on a timer (0 = only via the API)")
		refresh  = flag.Duration("refresh", 100*time.Millisecond, "dashboard refresh interval")
		plain    = flag.Bool("plain", false, "append frames instead of redrawing in place, and drop colour")
		verbose  = flag.Bool("verbose", false, "log engine activity to stderr")
	)
	flag.Var(peers, "peer", "peer as ID=URL; repeat once per peer")
	flag.Parse()

	if *nodeID == "" {
		return errors.New("--id is required")
	}

	// Before anything constructs a logger of its own: the engine and the transport
	// both capture slog.Default() when they are built.
	level := slog.LevelError
	if *verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})).
		With("node", *nodeID))

	replicaID, err := incarnation(*nodeID)
	if err != nil {
		return err
	}

	replica := counter.NewGCounter(replicaID)
	wire := newWatchedTransport(transport.NewHTTP(*nodeID, *gossip, peers))
	eng := engine.NewEngine(replica, wire)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := eng.Start(ctx, *interval); err != nil {
		return fmt.Errorf("starting the engine: %w", err)
	}
	// The signal context is already cancelled by the time this defer runs, so the
	// drain gets its own deadline.
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := eng.Stop(stopCtx); err != nil {
			slog.Warn("engine did not stop cleanly", "err", err)
		}
	}()

	api := &http.Server{
		Addr:              *apiAddr,
		Handler:           apiHandler(replica, *nodeID, replicaID),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := api.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("control api stopped", "err", err)
		}
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = api.Shutdown(ctx)
	}()

	dash := &dashboard{out: os.Stdout, plain: *plain}
	frames := time.NewTicker(*refresh)
	defer frames.Stop()

	// A nil channel blocks forever, which is exactly the "auto-increment is off"
	// case: the select branch simply never fires.
	var autoInc <-chan time.Time
	if *incEvery > 0 {
		t := time.NewTicker(*incEvery)
		defer t.Stop()
		autoInc = t.C
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Println()
			return nil
		case <-autoInc:
			replica.Increment()
		case <-frames.C:
			dash.render(frame{
				nodeID:    *nodeID,
				replicaID: replicaID,
				gossip:    *gossip,
				api:       *apiAddr,
				value:     replica.Value(),
				breakdown: breakdown(replica),
				peers:     wire.health(),
			})
		}
	}
}

// incarnation gives this process a replica id nothing has used before.
//
// The node id is the address-book name and is stable across restarts; the replica
// id is the key this process writes into the lattice, and it must be new on every
// boot. Reusing it would let a peer's remembered value shadow this process's own
// updates: the join takes the maximum per key, so an increment made before the
// catch-up lands would be silently eaten rather than merged.
func incarnation(nodeID string) (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generating an incarnation id: %w", err)
	}
	return fmt.Sprintf("%s#%x", nodeID, b), nil
}

// breakdown reads the per-replica contributions out of the state. GCounterState
// marshals as exactly that map, so the demo can show it without the type having
// to expose its internals.
func breakdown(replica *counter.GCounter) map[string]uint64 {
	raw, err := replica.State().MarshalBinary()
	if err != nil {
		return nil
	}
	var m map[string]uint64
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

func apiHandler(replica *counter.GCounter, nodeID, replicaID string) http.Handler {
	status := func(w http.ResponseWriter) {
		raw, err := replica.State().MarshalBinary()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{
			"node":    nodeID,
			"replica": replicaID,
			"value":   replica.Value(),
			"state":   json.RawMessage(raw),
		})
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /inc", func(w http.ResponseWriter, r *http.Request) {
		by := uint64(1)
		if raw := r.URL.Query().Get("by"); raw != "" {
			n, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				http.Error(w, "by must be a non-negative integer", http.StatusBadRequest)
				return
			}
			by = n
		}
		replica.IncrementBy(by)
		status(w)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) { status(w) })
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
