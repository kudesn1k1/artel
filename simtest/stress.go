package simtest

import "math/rand/v2"

// Failure is one stress run that reported violations: the child seed that
// regenerates it, the scenario itself and the run's result. Running the
// scenario again reproduces the result exactly.
type Failure struct {
	ChildSeed uint64
	Scenario  Scenario
	Result    Result
}

// Stress runs n scenarios generated from the profile and returns the failing
// ones. The child seeds derive from master, so one master always yields the
// same children. The subject must not carry state between runs.
func Stress(master uint64, n int, p Profile, sub Subject, oracles ...Oracle) []Failure {
	rng := rand.New(rand.NewPCG(master, 0))
	var failures []Failure

	for range n {
		childSeed := rng.Uint64()
		s := GenScenario(childSeed, p)
		res := Run(s, sub, oracles...)
		if len(res.Violations) > 0 {
			failures = append(failures, Failure{
				ChildSeed: childSeed,
				Scenario:  s,
				Result:    res,
			})
		}
	}

	return failures
}
