package artel

import "encoding/json/v2"

// Codec turns states into bytes and back. The engine ships whatever Encode
// returns and never looks inside; Decode sees exactly those bytes on the
// other side. Encode must be deterministic — equal states, equal bytes —
// because convergence is judged by comparing payloads, and a codec that
// compresses or versions its output keeps that property by construction.
type Codec[S any] interface {
	Encode(S) ([]byte, error)
	Decode([]byte) (S, error)
}

// JSON returns a Codec that writes a state's wire form as JSON: readable,
// deterministic, and the default for every type in this package. wire and
// unwire convert between the state and its wire form.
func JSON[S, W any](wire func(S) W, unwire func(W) S) Codec[S] {
	return jsonCodec[S, W]{wire: wire, unwire: unwire}
}

type jsonCodec[S, W any] struct {
	wire   func(S) W
	unwire func(W) S
}

func (c jsonCodec[S, W]) Encode(s S) ([]byte, error) {
	return json.Marshal(c.wire(s), json.Deterministic(true))
}

func (c jsonCodec[S, W]) Decode(b []byte) (S, error) {
	var w W
	if err := json.Unmarshal(b, &w); err != nil {
		var bottom S
		return bottom, err
	}
	return c.unwire(w), nil
}
