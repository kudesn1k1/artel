package engine

import (
	"crdtlab/crdt"
	"crdtlab/transport"
	"encoding"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type State[S any] interface {
	crdt.DeltaState[S]
	encoding.BinaryMarshaler
}

type pushJob[S State[S]] struct {
	peerId   string
	snapshot S
}

type pullJob string

const workerCount = 8

// TODO: add work with context
type Engine[S State[S], R crdt.DeltaReplica[S]] struct {
	local     R
	transport transport.Transport
	peers     map[string]*peerOutbox[S]
	decode    func([]byte) (S, error)
	done      chan struct{}
	ticker    *time.Ticker
	pushJobs  chan pushJob[S]
	pullJobs  chan pullJob
	wg        sync.WaitGroup
	log       *slog.Logger
	stopOnce  sync.Once
}

func NewEngine[S State[S], R crdt.DeltaReplica[S]](local R, transport transport.Transport, decode func([]byte) (S, error)) *Engine[S, R] {
	transportPeers := transport.Peers()
	peers := make(map[string]*peerOutbox[S], len(transportPeers))
	for _, peer := range transportPeers {
		peers[peer] = &peerOutbox[S]{}
	}

	return &Engine[S, R]{
		local:     local,
		transport: transport,
		peers:     peers,
		decode:    decode,
		done:      make(chan struct{}),
		pushJobs:  make(chan pushJob[S], 100), // TODO: review send jobs count
		pullJobs:  make(chan pullJob, 100),    // TODO: review send jobs count
		log:       slog.Default(),
	}
}

func (e *Engine[S, R]) Serve() error {
	if err := e.transport.Serve(e.consume); err != nil {
		return err
	}

	for _, peer := range e.transport.Peers() {
		e.peers[peer].markNeedsPull()
	}

	return nil
}

func (e *Engine[S, R]) Start(interval time.Duration) error {
	if err := e.Serve(); err != nil {
		return err
	}
	e.ticker = time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-e.done:
				return
			case <-e.ticker.C:
				e.round()
			}
		}
	}()

	for range workerCount {
		e.wg.Go(e.sendLoop)
	}

	e.round()

	return nil
}

func (e *Engine[S, R]) Stop() error {
	if e.ticker != nil {
		e.ticker.Stop()
	}

	e.stopOnce.Do(func() {
		close(e.done)
	})
	//TODO: consider returning pending jobs to buffer
	e.wg.Wait()

	if err := e.transport.Close(); err != nil {
		return err
	}
	return nil
}

func (e *Engine[S, R]) consume(m transport.Message) error {
	if m.Kind == transport.Pull {
		return e.sendFullState(m.From)
	}

	state, err := e.decode(m.Payload)
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
			pullJobs = append(pullJobs, pullJob(p))
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
		e.peers[string(job)].pullFailed()
	}
}

func (e *Engine[S, R]) sendFullState(peerID string) error {
	binary, err := e.local.State().MarshalBinary()
	if err != nil {
		return err // not wrapping the error cause call stack show the problem origin, no need to wrap here
	}

	message := transport.Message{
		From:    e.transport.ID(),
		Kind:    transport.Push,
		Payload: binary,
	}
	return e.transport.Send(peerID, message)
}

func (e *Engine[S, R]) sendLoop() {
	for {
		select {
		case <-e.done:
			return
		case job := <-e.pushJobs:
			binary, err := job.snapshot.MarshalBinary()
			if err != nil {
				e.log.Error("failed to marshal payload", "err", err)
				e.peers[job.peerId].pushFailed(job.snapshot)
				continue
			}

			msg := transport.Message{
				From:    e.transport.ID(),
				Kind:    transport.Push,
				Payload: binary,
			}

			if err := e.transport.Send(job.peerId, msg); err != nil {
				e.log.Error("failed to send push job", "peer", job.peerId, "err", err)
				e.peers[job.peerId].pushFailed(job.snapshot)
				continue
			}
			e.peers[job.peerId].pushDone()

		case job := <-e.pullJobs:
			msg := transport.Message{
				From: e.transport.ID(),
				Kind: transport.Pull,
			}

			peerId := string(job)

			if err := e.transport.Send(peerId, msg); err != nil {
				e.log.Error("failed to send pull job", "peer", job, "err", err)
				e.peers[peerId].pullFailed()
				continue
			}

			e.peers[peerId].pullDone()
		}
	}
}
