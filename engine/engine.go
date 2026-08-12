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

type sendJob[S State[S]] struct {
	peerId   string
	snapshot S
}

const workerCount = 4

type Engine[S State[S], R crdt.DeltaReplica[S]] struct {
	local     R
	transport transport.Transport
	pending   map[string]S // per-peer delta buffer
	decode    func([]byte) (S, error)
	mutex     sync.Mutex
	done      chan struct{}
	ticker    *time.Ticker
	sendJobs  chan sendJob[S]
	wg        sync.WaitGroup
	log       *slog.Logger
	stopOnce  sync.Once
}

func NewEngine[S State[S], R crdt.DeltaReplica[S]](local R, transport transport.Transport, decode func([]byte) (S, error)) *Engine[S, R] {
	return &Engine[S, R]{
		local:     local,
		transport: transport,
		pending:   make(map[string]S),
		decode:    decode,
		done:      make(chan struct{}),
		sendJobs:  make(chan sendJob[S], 100), // TODO: review send jobs count
		log:       slog.Default(),
	}
}

func (e *Engine[S, R]) Serve() error {
	//TODO: pull fresh state from known peers
	return e.transport.Serve(e.consume)
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
		e.wg.Go(func() {
			for {
				select {
				case <-e.done:
					return
				case job, ok := <-e.sendJobs:
					if !ok {
						return
					}

					binary, err := job.snapshot.MarshalBinary()
					if err != nil {
						e.log.Error("failed to marshal payload", "err", err)
						continue
					}

					msg := transport.Message{
						From:    e.local.ID(),
						Kind:    transport.Push,
						Payload: binary,
					}

					if err := e.transport.Send(job.peerId, msg); err != nil {
						e.log.Error("failed to send job", "peer", job.peerId, "err", err)
						e.mutex.Lock()
						e.pending[job.peerId] = e.pending[job.peerId].Join(job.snapshot)
						e.mutex.Unlock()
						continue
					}
				}
			}
		})
	}

	return nil
}

func (e *Engine[S, R]) Stop() error {
	if e.ticker != nil {
		e.ticker.Stop()
	}

	e.stopOnce.Do(func() {
		close(e.done)
	})

	if err := e.transport.Close(); err != nil {
		return err
	}
	return nil
}

func (e *Engine[S, R]) consume(m transport.Message) error {
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
	jobs := make([]sendJob[S], 0, len(peers))

	e.mutex.Lock()
	for _, p := range peers {
		sent := e.pending[p].Join(freshDelta)

		jobs = append(jobs, sendJob[S]{
			peerId:   p,
			snapshot: sent,
		})

		e.pending[p] = *new(S)
	}
	e.mutex.Unlock()

	for _, job := range jobs {
		e.sendJobs <- job
	}

}
