package redi

import (
	"encoding/json"
	"strings"
)

// decodeInto decodes a codec-encoded Redis string into a typed pointer
// target. It always goes through Codec.Decode first so Java type wrappers
// (@class / ArrayList / Long) are stripped before binding — matching Get().
func decodeInto(c Codec, s string, target any) error {
	v, err := c.Decode(s)
	if err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.UseNumber()
	return dec.Decode(target)
}
