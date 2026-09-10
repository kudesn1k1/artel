package artel

import "encoding/json/v2"

// Codec turns states into bytes and back. Encode must be deterministic:
// equal states, equal bytes.
type Codec[S any] interface {
	Encode(S) ([]byte, error)
	Decode([]byte) (S, error)
}

// JSONCodec returns a Codec that encodes a state's wire form as JSON. wire
// and unwire convert between the state and its wire form.
func JSONCodec[S, W any](wire func(S) W, unwire func(W) S) Codec[S] {
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
