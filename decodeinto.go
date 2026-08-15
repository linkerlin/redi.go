package redi

import (
	"encoding/json"
	"strings"
)

// decodeInto decodes a codec-encoded string into a typed target using the
// JSON representation (works for any Codec whose encoding is JSON).
func decodeInto(c Codec, s string, target any) error {
	if _, ok := c.(JSONCodec); ok {
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		if err := dec.Decode(target); err != nil {
			return err
		}
		return nil
	}
	v, err := c.Decode(s)
	if err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, target)
}
