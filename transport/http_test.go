package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kudesn1k1/artel"
)

// Contract tests for the real-network transport. Everything the engine relies on
// goes through this file: the envelope survives the wire byte for byte, a
// handler's error travels back to the sender as the in-band ACK, and an
// unreachable peer is reported rather than swallowed.

// freeAddr reserves a loopback port and hands it back, so parallel runs and busy
// machines do not collide on a hardcoded one.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// recorder is a served HTTP node that records what it receives and answers with
// whatever handlerErr is set to. If a test calls block(), the handler also parks
// until it is released or until its own request context dies.
type recorder struct {
	tr *HTTP

	mu         sync.Mutex
	got        []artel.Message
	handlerErr error
	gate       chan struct{}

	sawCtxDone atomic.Bool
}

func serveRecorder(t *testing.T, id, addr string, peers map[string]string) *recorder {
	t.Helper()
	r := &recorder{tr: NewHTTP(id, addr, peers)}
	if err := r.tr.Serve(func(ctx context.Context, m artel.Message) error {
		r.mu.Lock()
		r.got = append(r.got, m)
		err, gate := r.handlerErr, r.gate
		r.mu.Unlock()

		if gate != nil {
			select {
			case <-gate:
			case <-ctx.Done():
				r.sawCtxDone.Store(true)
				return ctx.Err()
			}
		}
		return err
	}); err != nil {
		t.Fatalf("serve %s: %v", id, err)
	}
	t.Cleanup(func() { _ = r.tr.Close() })
	return r
}

func (r *recorder) received() []artel.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]artel.Message(nil), r.got...)
}

func (r *recorder) failWith(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlerErr = err
}

// block makes every subsequent request park in the handler. The gate is released
// on cleanup so a failing test cannot leave the server's shutdown waiting.
func (r *recorder) block(t *testing.T) {
	t.Helper()
	gate := make(chan struct{})
	r.mu.Lock()
	r.gate = gate
	r.mu.Unlock()
	t.Cleanup(func() { close(gate) })
}

func TestHTTPDeliversTheEnvelopeIntact(t *testing.T) {
	addrB := freeAddr(t)
	b := serveRecorder(t, "B", addrB, nil)

	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + addrB})
	t.Cleanup(func() { _ = a.Close() })

	// Deliberately not valid UTF-8: the payload is opaque bytes and must survive
	// the JSON envelope (it rides as base64) unchanged.
	payload := []byte{0x00, 0xff, 0xfe, 'o', 'k'}
	if err := a.Send(context.Background(), "B", artel.Message{From: "A", Kind: artel.KindPush, Payload: payload}); err != nil {
		t.Fatalf("send: %v", err)
	}

	got := b.received()
	if len(got) != 1 {
		t.Fatalf("B received %d messages, want 1", len(got))
	}
	if got[0].From != "A" || got[0].Kind != artel.KindPush {
		t.Fatalf("envelope mangled: From=%q Kind=%v", got[0].From, got[0].Kind)
	}
	if !bytes.Equal(got[0].Payload, payload) {
		t.Fatalf("payload mangled: got %v, want %v", got[0].Payload, payload)
	}
}

func TestHTTPCarriesAPullWithNoPayload(t *testing.T) {
	addrB := freeAddr(t)
	b := serveRecorder(t, "B", addrB, nil)

	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + addrB})
	t.Cleanup(func() { _ = a.Close() })

	if err := a.Send(context.Background(), "B", artel.Message{From: "A", Kind: artel.KindPull}); err != nil {
		t.Fatalf("send: %v", err)
	}

	got := b.received()
	if len(got) != 1 || got[0].Kind != artel.KindPull {
		t.Fatalf("B did not receive a Pull: %+v", got)
	}
	if len(got[0].Payload) != 0 {
		t.Fatalf("a Pull arrived carrying %d payload bytes", len(got[0].Payload))
	}
}

// The engine treats a successful Send as the acknowledgement that the peer
// processed the message. A handler that fails must therefore surface as an error
// on the sender's side, not be swallowed by a 200.
func TestHTTPHandlerErrorReachesTheSender(t *testing.T) {
	addrB := freeAddr(t)
	b := serveRecorder(t, "B", addrB, nil)
	b.failWith(io.ErrUnexpectedEOF)

	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + addrB})
	t.Cleanup(func() { _ = a.Close() })

	if err := a.Send(context.Background(), "B", artel.Message{From: "A", Kind: artel.KindPush, Payload: []byte("{}")}); err == nil {
		t.Fatal("Send reported success although the peer's handler failed")
	}
}

// A failed handler must produce one clean response. Today /gossip calls
// http.Error and then falls through and appends a success envelope to the same
// body, so the peer answers 500 with `...\n{"status":"ok"}` in it.
func TestHTTPErrorResponseIsWellFormed(t *testing.T) {
	addr := freeAddr(t)
	node := serveRecorder(t, "B", addr, nil)
	node.failWith(io.ErrUnexpectedEOF)

	body, _ := json.Marshal(artel.Message{From: "probe", Kind: artel.KindPush, Payload: []byte("{}")})
	resp, err := http.Post("http://"+addr+"/gossip", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status %s, want 500", resp.Status)
	}
	if strings.Contains(string(raw), `"status":"ok"`) {
		t.Fatalf("a failed request answered 500 with a success marker in the body: %q", raw)
	}
}

func TestHTTPUnknownPeerIsReported(t *testing.T) {
	a := NewHTTP("A", freeAddr(t), map[string]string{})
	t.Cleanup(func() { _ = a.Close() })

	if err := a.Send(context.Background(), "nobody", artel.Message{From: "A", Kind: artel.KindPush}); err == nil {
		t.Fatal("Send to an unknown peer reported success")
	}
}

func TestHTTPUnreachablePeerIsReported(t *testing.T) {
	// Nothing is listening on this address: it was reserved and released.
	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + freeAddr(t)})
	t.Cleanup(func() { _ = a.Close() })

	if err := a.Send(context.Background(), "B", artel.Message{From: "A", Kind: artel.KindPush, Payload: []byte("{}")}); err == nil {
		t.Fatal("Send to a dead peer reported success")
	}
}

func TestHTTPPeersIsStableAndSorted(t *testing.T) {
	a := NewHTTP("A", freeAddr(t), map[string]string{
		"C": "http://c", "A2": "http://a2", "B": "http://b",
	})
	t.Cleanup(func() { _ = a.Close() })

	want := []string{"A2", "B", "C"}
	for range 5 { // map iteration is randomised; the accessor must not be
		got := a.Peers()
		if len(got) != len(want) {
			t.Fatalf("Peers() returned %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("Peers() returned %v, want %v", got, want)
			}
		}
	}
}

func TestHTTPCloseStopsServing(t *testing.T) {
	addrB := freeAddr(t)
	b := serveRecorder(t, "B", addrB, nil)

	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + addrB})
	t.Cleanup(func() { _ = a.Close() })

	if err := a.Send(context.Background(), "B", artel.Message{From: "A", Kind: artel.KindPush, Payload: []byte("{}")}); err != nil {
		t.Fatalf("send before close: %v", err)
	}
	if err := b.tr.Close(); err != nil {
		t.Fatalf("close B: %v", err)
	}

	// Shutdown drains in-flight connections; give the listener a moment to go.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := a.Send(context.Background(), "B", artel.Message{From: "A", Kind: artel.KindPush, Payload: []byte("{}")}); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("B kept accepting gossip after Close")
}

// http.Server.Shutdown never closes a connection that was accepted but has not
// sent a single byte yet (StateNew): it waits for a request to arrive or for
// ReadHeaderTimeout — longer than Close's graceful budget. Such connections
// occur in normal operation: under contention the HTTP client dials a spare
// connection and pools it without ever writing a request. Close must survive
// one, not surface it as a failed shutdown.
func TestHTTPCloseSurvivesASilentConnection(t *testing.T) {
	addrB := freeAddr(t)
	b := serveRecorder(t, "B", addrB, nil)

	// Plant the silent connection: dialled, accepted, never written to.
	silent, err := net.Dial("tcp", addrB)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = silent.Close() })

	// A completed request over a LATER connection proves the accept loop has
	// taken the silent one: a single loop accepts in arrival order.
	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + addrB})
	t.Cleanup(func() { _ = a.Close() })
	if err := a.Send(context.Background(), "B", artel.Message{From: "A", Kind: artel.KindPush, Payload: []byte("{}")}); err != nil {
		t.Fatalf("send: %v", err)
	}

	if err := b.tr.Close(); err != nil {
		t.Fatalf("Close with a silent connection open: %v", err)
	}
}

// --- context ------------------------------------------------------------------

// The engine cancels its context to shut down, and a worker parked in Send is
// what shutdown waits for. So an already-dead context must abort immediately and
// must not put anything on the wire.
func TestHTTPSendHonoursACancelledContext(t *testing.T) {
	addrB := freeAddr(t)
	b := serveRecorder(t, "B", addrB, nil)

	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + addrB})
	t.Cleanup(func() { _ = a.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := a.Send(ctx, "B", artel.Message{From: "A", Kind: artel.KindPush, Payload: []byte("{}")})
	if err == nil {
		t.Fatal("Send with a cancelled context reported success")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Send failed with %v, want it to wrap context.Canceled", err)
	}
	if got := b.received(); len(got) != 0 {
		t.Fatalf("B received %d messages from a cancelled Send", len(got))
	}
}

// A peer that accepts the connection and then never answers is the case a
// connect timeout cannot catch and the one that parks a worker indefinitely. The
// deadline must cut it, and the error must say so: the engine logs
// DeadlineExceeded as a real failure but keeps Canceled quiet.
func TestHTTPSendHonoursADeadline(t *testing.T) {
	addrB := freeAddr(t)
	b := serveRecorder(t, "B", addrB, nil)
	b.block(t) // B accepts the request and parks in the handler

	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + addrB})
	t.Cleanup(func() { _ = a.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := a.Send(ctx, "B", artel.Message{From: "A", Kind: artel.KindPush, Payload: []byte("{}")})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Send reported success although the peer never answered")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send failed with %v, want it to wrap context.DeadlineExceeded", err)
	}
	// The client's own 5s Timeout would also eventually fire; this pins that the
	// ctx deadline is what cut it, not the backstop.
	if elapsed > time.Second {
		t.Fatalf("Send took %v to honour a 100ms deadline", elapsed)
	}
}

// Why answering a Pull inline inside the inbound handler needs no timeout of its
// own: the handler's context dies when the puller stops waiting, so the outbound
// answer derived from it is cut too. If this ever stops holding, sendFullState
// becomes an unbounded outbound request run inside an inbound one.
func TestHTTPHandlerContextDiesWhenTheSenderGivesUp(t *testing.T) {
	addrB := freeAddr(t)
	b := serveRecorder(t, "B", addrB, nil)
	b.block(t)

	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + addrB})
	t.Cleanup(func() { _ = a.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := a.Send(ctx, "B", artel.Message{From: "A", Kind: artel.KindPull}); err == nil {
		t.Fatal("Send reported success although the peer never answered")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.sawCtxDone.Load() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the handler's context was never cancelled after the sender gave up: " +
		"an inline Pull answer would keep running with nobody waiting for it")
}
