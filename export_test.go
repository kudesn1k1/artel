package artel

import "time"

// Bridges for the external artel_test package. Compiled only into tests.

const WorkerCount = workerCount

func (e *Engine[S, PS, R]) Round() { e.round() }

func (e *Engine[S, PS, R]) SetSendTimeout(d time.Duration) { e.sendTimeout = d }
