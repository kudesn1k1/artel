package counter

import (
	"sync"
	"testing"
)

// The delta replicas are touched from several goroutines at once in the real
// engine: the user calls Increment/Decrement, the gossip loop calls FlushDelta,
// inbound gossip calls Merge. These stress tests assert the per-replica mutex
// keeps map access safe (no "concurrent map read and map write" fatal, which the
// Go runtime reports even without the race detector) and that no update is lost.
// Run with -race when cgo is available for the full check.

func TestGCounterConcurrent(t *testing.T) {
	const writers, perWriter = 8, 2000
	a := NewGCounter("A")

	// A reader/flusher spinning against the writers, to exercise the mutex
	// between the state-mutating and the snapshot/drain paths.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = a.FlushDelta()
				_ = a.State()
				_ = a.Value()
			}
		}
	}()

	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for range perWriter {
				a.Increment()
			}
		})
	}
	wg.Wait()
	close(stop)

	if got := a.Value(); got != writers*perWriter {
		t.Fatalf("lost increments under concurrency: want %d, got %d", writers*perWriter, got)
	}
}

func TestPNCounterConcurrent(t *testing.T) {
	const workers, perWorker = 8, 2000
	a := NewPNCounter("A")

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = a.FlushDelta()
				_ = a.State()
				_ = a.Value()
			}
		}
	}()

	var wg sync.WaitGroup
	for range workers { // incrementers
		wg.Go(func() {
			for range perWorker {
				a.Increment()
			}
		})
	}
	for range workers { // decrementers, equal in number → net zero
		wg.Go(func() {
			for range perWorker {
				a.Decrement()
			}
		})
	}
	wg.Wait()
	close(stop)

	if got := a.Value(); got != 0 {
		t.Fatalf("equal inc and dec should cancel to 0, got %d", got)
	}
}
