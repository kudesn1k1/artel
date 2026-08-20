package artel_test

import (
	"fmt"

	"github.com/kudesn1k1/artel"
	"github.com/kudesn1k1/artel/transport"
)

// Compiles with zero explicit type arguments: type inference is part of the
// public API, and this example breaks the build if it regresses.
func ExampleNewEngine() {
	reg := transport.NewRegistry()
	tr := transport.NewInProcess("a", nil, reg)

	engine := artel.NewEngine(artel.NewGCounter("a"), tr)
	_ = engine
}

func ExampleGCounter() {
	c := artel.NewGCounter("a")
	c.Increment()
	c.IncrementBy(2)
	fmt.Println(c.Value())
	// Output: 3
}
