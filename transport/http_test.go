package transport

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
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
// whatever handlerErr is set to.
type recorder struct {
	tr *HTTP

	mu         sync.Mutex
	got        []Message
	handlerErr error
}

func serveRecorder(t *testing.T, id, addr string, peers map[string]string) *recorder {
	t.Helper()
	r := &recorder{tr: NewHTTP(id, addr, peers)}
	if err := r.tr.Serve(func(m Message) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.got = append(r.got, m)
		return r.handlerErr
	}); err != nil {
		t.Fatalf("serve %s: %v", id, err)
	}
	t.Cleanup(func() { _ = r.tr.Close() })
	return r
}

func (r *recorder) received() []Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Message(nil), r.got...)
}

func (r *recorder) failWith(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlerErr = err
}

func TestHTTPDeliversTheEnvelopeIntact(t *testing.T) {
	addrB := freeAddr(t)
	b := serveRecorder(t, "B", addrB, nil)

	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + addrB})
	t.Cleanup(func() { _ = a.Close() })

	// Deliberately not valid UTF-8: the payload is opaque bytes and must survive
	// the JSON envelope (it rides as base64) unchanged.
	payload := []byte{0x00, 0xff, 0xfe, 'o', 'k'}
	if err := a.Send("B", Message{From: "A", Kind: Push, Payload: payload}); err != nil {
		t.Fatalf("send: %v", err)
	}

	got := b.received()
	if len(got) != 1 {
		t.Fatalf("B received %d messages, want 1", len(got))
	}
	if got[0].From != "A" || got[0].Kind != Push {
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

	if err := a.Send("B", Message{From: "A", Kind: Pull}); err != nil {
		t.Fatalf("send: %v", err)
	}

	got := b.received()
	if len(got) != 1 || got[0].Kind != Pull {
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

	if err := a.Send("B", Message{From: "A", Kind: Push, Payload: []byte("{}")}); err == nil {
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

	body, _ := json.Marshal(Message{From: "probe", Kind: Push, Payload: []byte("{}")})
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

	if err := a.Send("nobody", Message{From: "A", Kind: Push}); err == nil {
		t.Fatal("Send to an unknown peer reported success")
	}
}

func TestHTTPUnreachablePeerIsReported(t *testing.T) {
	// Nothing is listening on this address: it was reserved and released.
	a := NewHTTP("A", freeAddr(t), map[string]string{"B": "http://" + freeAddr(t)})
	t.Cleanup(func() { _ = a.Close() })

	if err := a.Send("B", Message{From: "A", Kind: Push, Payload: []byte("{}")}); err == nil {
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

	if err := a.Send("B", Message{From: "A", Kind: Push, Payload: []byte("{}")}); err != nil {
		t.Fatalf("send before close: %v", err)
	}
	if err := b.tr.Close(); err != nil {
		t.Fatalf("close B: %v", err)
	}

	// Shutdown drains in-flight connections; give the listener a moment to go.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := a.Send("B", Message{From: "A", Kind: Push, Payload: []byte("{}")}); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("B kept accepting gossip after Close")
}
