package artel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type pushJob[S DeltaState[S]] struct {
	peerId   string
	snapshot S
}

type pullJob = string

const workerCount = 8

// Engine runs anti-entropy gossip for one replica: every interval it ships
// the replica's fresh delta to each peer, merges what peers send, and pulls
// a full state from every peer when it starts, so a new or restarted node
// catches up. A push that fails is kept and sent again. The replica stays
// usable throughout; the engine only reads its delta and merges into it.
type Engine[S DeltaState[S], R DeltaReplica[S]] struct {
	local       R
	transport   Transport
	codec       Codec[S]
	peers       map[string]*peerOutbox[S]
	ticker      *time.Ticker
	pushJobs    chan pushJob[S]
	pullJobs    chan pullJob
	sendTimeout time.Duration
	wg          sync.WaitGroup
	log         *slog.Logger
	ctx         context.Context
	ctxCancel   context.CancelFunc
	stopped     chan struct{}
	stopOnce    sync.Once
}

// NewEngine wires a replica to a transport, with codec turning states into
// the bytes on the wire. Nothing runs until Start.
func NewEngine[S DeltaState[S], R DeltaReplica[S]](local R, transport Transport, codec Codec[S]) *Engine[S, R] {
	transportPeers := transport.Peers()
	peers := make(map[string]*peerOutbox[S], len(transportPeers))
	for _, peer := range transportPeers {
		peers[peer] = &peerOutbox[S]{}
	}

	return &Engine[S, R]{
		local:       local,
		transport:   transport,
		codec:       codec,
		peers:       peers,
		pushJobs:    make(chan pushJob[S], 100), // TODO: review send jobs count
		pullJobs:    make(chan pullJob, 100),    // TODO: review send jobs count
		log:         slog.Default(),
		sendTimeout: 2 * time.Second,
		stopped:     make(chan struct{}),
	}
}

func (e *Engine[S, R]) serve() error {
	if err := e.transport.Serve(e.consume); err != nil {
		return err
	}

	for _, peer := range e.transport.Peers() {
		e.peers[peer].markNeedsPull()
	}

	return nil
}

// Start begins serving inbound messages and gossiping on the given interval.
// The engine's lifetime is bound to ctx: cancelling it stops the gossip loop
// and the workers. Start is one-shot — to restart, build a new Engine with
// NewEngine.
func (e *Engine[S, R]) Start(ctx context.Context, interval time.Duration) error {
	if err := e.serve(); err != nil {
		return err
	}

	e.ctx, e.ctxCancel = context.WithCancel(ctx)

	e.ticker = time.NewTicker(interval)
	e.wg.Go(func() {
		for {
			select {
			case <-e.ctx.Done():
				return
			case <-e.ticker.C:
				e.round()
			}
		}
	})

	for range workerCount {
		e.wg.Go(e.sendLoop)
	}

	go func() {
		e.wg.Wait()
		e.stopOnce.Do(func() {
			close(e.stopped)
		})
	}()

	e.round()

	return nil
}

// Stop cancels the engine, closes the transport, and waits for in-flight work
// to drain. The wait — and only the wait — is bounded by ctx: on expiry Stop
// returns ctx.Err() while the drain finishes in the background (Stopped
// reports when it has). A transport close error is returned too, joined with
// the ctx error when both occur. Stop is idempotent and safe to call on an
// engine that was never started.
func (e *Engine[S, R]) Stop(ctx context.Context) error {
	if e.ticker != nil {
		e.ticker.Stop()
	}
	if e.ctx == nil {
		e.stopOnce.Do(func() {
			close(e.stopped)
		})

		return e.transport.Close()
	}
	e.ctxCancel()

	closeErr := e.transport.Close()

	//TODO: consider returning pending jobs to buffer

	select {
	case <-e.stopped:
		return closeErr
	case <-ctx.Done():
		return errors.Join(closeErr, ctx.Err())
	}
}

// Stopped returns a channel that is closed once every engine goroutine has
// exited — including after a Stop that gave up waiting.
func (e *Engine[S, R]) Stopped() <-chan struct{} {
	return e.stopped
}

func (e *Engine[S, R]) consume(ctx context.Context, m Message) error {
	if m.Kind == KindPull {
		return e.sendFullState(ctx, m.From)
	}

	state, err := e.codec.Decode(m.Payload)
	if err != nil {
		return fmt.Errorf("decoding message: %w", err)
	}

	e.local.Merge(state)
	return nil
}

func (e *Engine[S, R]) round() {
	freshDelta := e.local.FlushDelta()
	peers := e.transport.Peers()
	pushJobs := make([]pushJob[S], 0, len(peers))
	pullJobs := make([]pullJob, 0, len(peers))

	for _, p := range peers {
		if e.peers[p].takePull() {
			pullJobs = append(pullJobs, p)
		}

		if push, shouldSend := e.peers[p].takePush(freshDelta); shouldSend {
			pushJobs = append(pushJobs, pushJob[S]{
				peerId:   p,
				snapshot: push,
			})
		}
	}

	var pushesToReturn []pushJob[S]
	var pullsToReturn []pullJob
	for _, job := range pushJobs {
		select {
		case e.pushJobs <- job:
		default:
			pushesToReturn = append(pushesToReturn, job)
		}
	}
	for _, job := range pullJobs {
		select {
		case e.pullJobs <- job:
		default:
			pullsToReturn = append(pullsToReturn, job)
		}
	}

	for _, job := range pushesToReturn {
		e.peers[job.peerId].pushFailed(job.snapshot)
	}

	for _, job := range pullsToReturn {
		e.peers[job].pullFailed()
	}
}

func (e *Engine[S, R]) sendFullState(ctx context.Context, peerID string) error {
	binary, err := e.codec.Encode(e.local.State())
	if err != nil {
		return err // not wrapping the error cause call stack show the problem origin, no need to wrap here
	}

	message := Message{
		From:    e.transport.ID(),
		Kind:    KindPush,
		Payload: binary,
	}
	return e.transport.Send(ctx, peerID, message)
}

func (e *Engine[S, R]) sendLoop() {
	for {
		select {
		case <-e.ctx.Done():
			return
		case job := <-e.pushJobs:
			e.handlePush(job)
		case job := <-e.pullJobs:
			e.handlePull(job)
		}
	}
}

func (e *Engine[S, R]) handlePush(job pushJob[S]) {
	binary, err := e.codec.Encode(job.snapshot)
	if err != nil {
		e.log.Error("failed to marshal payload", "err", err)
		e.peers[job.peerId].pushFailed(job.snapshot)
		return
	}

	msg := Message{
		From:    e.transport.ID(),
		Kind:    KindPush,
		Payload: binary,
	}

	ctx, cancel := context.WithTimeout(e.ctx, e.sendTimeout)
	defer cancel()

	if err := e.transport.Send(ctx, job.peerId, msg); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.log.Error("failed to send push job", "peer", job.peerId, "err", err)
		}
		e.peers[job.peerId].pushFailed(job.snapshot)
		return
	}
	e.peers[job.peerId].pushDone()
}

func (e *Engine[S, R]) handlePull(job pullJob) {
	msg := Message{
		From: e.transport.ID(),
		Kind: KindPull,
	}

	ctx, cancel := context.WithTimeout(e.ctx, e.sendTimeout)
	defer cancel()

	if err := e.transport.Send(ctx, job, msg); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.log.Error("failed to send pull job", "peer", job, "err", err)
		}
		e.peers[job].pullFailed()
		return
	}

	e.peers[job].pullDone()
}
