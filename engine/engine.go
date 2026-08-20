package engine

import (
	"context"
	"crdtlab/crdt"
	"crdtlab/transport"
	"encoding"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type State[S any] interface {
	crdt.DeltaState[S]
	encoding.BinaryMarshaler
}

type StatePtr[S any] interface {
	*S
	encoding.BinaryUnmarshaler
}

type pushJob[S State[S]] struct {
	peerId   string
	snapshot S
}

type pullJob = string

const workerCount = 8

type Engine[S State[S], PS StatePtr[S], R crdt.DeltaReplica[S]] struct {
	local       R
	transport   transport.Transport
	peers       map[string]*peerOutbox[S]
	ticker      *time.Ticker
	pushJobs    chan pushJob[S]
	pullJobs    chan pullJob
	wg          sync.WaitGroup
	log         *slog.Logger
	ctx         context.Context
	ctxCancel   context.CancelFunc
	sendTimeout time.Duration
}

func NewEngine[S State[S], PS StatePtr[S], R crdt.DeltaReplica[S]](local R, transport transport.Transport) *Engine[S, PS, R] {
	transportPeers := transport.Peers()
	peers := make(map[string]*peerOutbox[S], len(transportPeers))
	for _, peer := range transportPeers {
		peers[peer] = &peerOutbox[S]{}
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Engine[S, PS, R]{
		local:       local,
		transport:   transport,
		peers:       peers,
		pushJobs:    make(chan pushJob[S], 100), // TODO: review send jobs count
		pullJobs:    make(chan pullJob, 100),    // TODO: review send jobs count
		log:         slog.Default(),
		ctx:         ctx,
		ctxCancel:   cancel,
		sendTimeout: 2 * time.Second,
	}
}

func (e *Engine[S, PS, R]) Serve() error {
	if err := e.transport.Serve(e.consume); err != nil {
		return err
	}

	for _, peer := range e.transport.Peers() {
		e.peers[peer].markNeedsPull()
	}

	return nil
}

func (e *Engine[S, PS, R]) Start(interval time.Duration) error {
	if err := e.Serve(); err != nil {
		return err
	}
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

	e.round()

	return nil
}

func (e *Engine[S, PS, R]) Stop() error {
	if e.ticker != nil {
		e.ticker.Stop()
	}

	e.ctxCancel()
	//TODO: consider returning pending jobs to buffer
	e.wg.Wait()

	if err := e.transport.Close(); err != nil {
		return err
	}
	return nil
}

func (e *Engine[S, PS, R]) consume(ctx context.Context, m transport.Message) error {
	if m.Kind == transport.Pull {
		return e.sendFullState(ctx, m.From)
	}

	state, err := e.decode(m.Payload)
	if err != nil {
		return fmt.Errorf("decoding message: %w", err)
	}

	e.local.Merge(state)
	return nil
}

func (e *Engine[S, PS, R]) round() {
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

func (e *Engine[S, PS, R]) sendFullState(ctx context.Context, peerID string) error {
	binary, err := e.local.State().MarshalBinary()
	if err != nil {
		return err // not wrapping the error cause call stack show the problem origin, no need to wrap here
	}

	message := transport.Message{
		From:    e.transport.ID(),
		Kind:    transport.Push,
		Payload: binary,
	}
	return e.transport.Send(ctx, peerID, message)
}

func (e *Engine[S, PS, R]) sendLoop() {
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

func (e *Engine[S, PS, R]) handlePush(job pushJob[S]) {
	binary, err := job.snapshot.MarshalBinary()
	if err != nil {
		e.log.Error("failed to marshal payload", "err", err)
		e.peers[job.peerId].pushFailed(job.snapshot)
		return
	}

	msg := transport.Message{
		From:    e.transport.ID(),
		Kind:    transport.Push,
		Payload: binary,
	}

	ctx, cancel := context.WithTimeout(e.ctx, e.sendTimeout)
	defer cancel()

	if err := e.transport.Send(ctx, job.peerId, msg); err != nil && !errors.Is(err, context.Canceled) {
		e.log.Error("failed to send push job", "peer", job.peerId, "err", err)
		e.peers[job.peerId].pushFailed(job.snapshot)
		return
	}
	e.peers[job.peerId].pushDone()
}

func (e *Engine[S, PS, R]) handlePull(job pullJob) {
	msg := transport.Message{
		From: e.transport.ID(),
		Kind: transport.Pull,
	}

	ctx, cancel := context.WithTimeout(e.ctx, e.sendTimeout)
	defer cancel()

	if err := e.transport.Send(ctx, job, msg); err != nil && !errors.Is(err, context.Canceled) {
		e.log.Error("failed to send pull job", "peer", job, "err", err)
		e.peers[job].pullFailed()
		return
	}

	e.peers[job].pullDone()
}

func (e *Engine[S, PS, R]) decode(b []byte) (S, error) {
	var s S
	if err := PS(&s).UnmarshalBinary(b); err != nil {
		var bottom S
		return bottom, err
	}
	return s, nil
}
