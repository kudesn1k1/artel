package simtest

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/kudesn1k1/artel"
	"golang.org/x/sync/errgroup"
)

// ErrDropped is what Send returns for a message the chaos transport lost.
var ErrDropped = errors.New("simtest: dropped")

// ErrAckLost is what Send returns for a message the chaos transport
// delivered but reports as failed.
var ErrAckLost = errors.New("simtest: ack lost")

// ChaosConfig sets how often the chaos transport misbehaves. Every
// probability is per send, in [0, 1]. DropP loses the message and reports an
// error. AckLieP loses the message and reports success: a transport that
// breaks the delivery contract, so keep it at zero unless that is the
// experiment. DupP delivers the message twice, each copy after its own delay.
// AckLostP delivers the message and reports an error. DelayMin and DelayMax
// bound the delay before a delivery; a send whose context ends during the
// delay fails with the context's error and delivers nothing. A lost message
// is never duplicated, and a duplicated message with a lost ack is delivered
// twice and reported once. Keep DelayMax well below the engine's send
// timeout, or every delayed push is retained as a failure.
type ChaosConfig struct {
	DropP, DupP, AckLostP, AckLieP float64
	DelayMin, DelayMax             time.Duration
}

type chaosTransport struct {
	inner artel.Transport
	cfg   ChaosConfig
	mx    sync.Mutex
	rng   *rand.Rand
}

func (c *chaosTransport) Send(ctx context.Context, peerID string, m artel.Message) error {
	copies := 1
	ackLost := false
	if c.decide(c.cfg.DropP) {
		return ErrDropped
	}

	if c.decide(c.cfg.AckLieP) {
		return nil
	}

	if c.decide(c.cfg.DupP) {
		copies++
	}

	if c.decide(c.cfg.AckLostP) {
		ackLost = true
	}

	var g errgroup.Group
	for range copies {
		g.Go(func() error {
			c.mx.Lock()
			delay := c.cfg.DelayMin + time.Duration(c.rng.IntN(int(c.cfg.DelayMax-c.cfg.DelayMin+1)))
			c.mx.Unlock()

			timer := time.NewTimer(delay)
			defer timer.Stop()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				err := c.inner.Send(ctx, peerID, m)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	if ackLost {
		return ErrAckLost
	}
	return nil
}

func (c *chaosTransport) ID() string {
	return c.inner.ID()
}

func (c *chaosTransport) Peers() []string {
	return c.inner.Peers()
}

func (c *chaosTransport) Serve(h artel.Handler) error {
	return c.inner.Serve(h)
}

func (c *chaosTransport) Close() error {
	return c.inner.Close()
}

func (c *chaosTransport) decide(probability float64) bool {
	c.mx.Lock()
	defer c.mx.Unlock()
	return c.rng.Float64() < probability
}

// Chaos wraps a Transport so that its sends suffer the anomalies in cfg:
// lost, duplicated and delayed messages, and wrong reports of the outcome.
// Every decision comes from a PRNG seeded with seed, so one seed over one
// sequence of sends makes the same decisions. The seed does not control the
// goroutine schedule: with several senders the order of their sends, and so
// the decisions each one gets, changes from run to run. Reproducibility here
// is statistical; byte-for-byte replay is what Run over a Scenario is for.
// ID, Peers, Serve and Close pass through to the inner transport. Chaos
// panics when DelayMax is below DelayMin.
func Chaos(inner artel.Transport, seed uint64, cfg ChaosConfig) artel.Transport {
	validateChaosConfig(cfg)
	rng := rand.New(rand.NewPCG(seed, 0))
	return &chaosTransport{
		inner: inner,
		cfg:   cfg,
		rng:   rng,
	}
}

func validateChaosConfig(cfg ChaosConfig) {
	if cfg.DelayMax < cfg.DelayMin {
		panic("simtest: DelayMax < DelayMin")
	}
	if cfg.DelayMin < 0 {
		panic("simtest: DelayMin < 0")
	}
}
