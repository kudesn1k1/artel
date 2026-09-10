package artel

import "time"

// Bridges for the external artel_test package. Compiled only into tests.

const WorkerCount = workerCount

func (e *Engine[S, R]) Round() { e.round() }

func (e *Engine[S, R]) SetSendTimeout(d time.Duration) { e.sendTimeout = d }
