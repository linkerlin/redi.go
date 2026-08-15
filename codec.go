package redi

import (
	"encoding/json"
	"math"
	"strings"
)

// Codec encodes and decodes values stored in Redis, mirroring Redisson's
// codec layer. The default JSONCodec is wire-compatible with Redisson
// configured with JsonJacksonCodec (without type info) and with redi.py:
//
//   - Strings, bools, floats and small ints are stored as bare JSON.
//   - Ints outside the 32-bit range are wrapped as ["java.lang.Long", v]
//     (matching Redisson's Long wrapping).
//   - Decode unwraps ["java.lang.Long", v] / ["java.util.ArrayList", v]-style
//     typed wrappers and strips JsonJacksonCodec "@class" fields.
//   - Bytes that are not valid JSON decode to the raw string unchanged.
type Codec interface {
	Encode(v any) (string, error)
	Decode(s string) (any, error)
}

// JSONCodec is the default Codec. It is stateless and safe for concurrent use.
type JSONCodec struct{}

// Encode implements Codec.
func (JSONCodec) Encode(v any) (string, error) {
	if s, ok := longWrap(v); ok {
		return s, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	// Non-final compound types need Java type ids for JsonJacksonCodec's
	// default typing (verified against Redisson 4.6.1: reading a bare JSON
	// object throws "missing type id"). Mirrors what Redisson itself writes:
	// maps get "@class":"java.util.LinkedHashMap", lists are wrapped as
	// ["java.util.ArrayList",[...]] — the two shapes redi.py's decoder strips.
	var generic any
	if err := json.Unmarshal(b, &generic); err != nil {
		return string(b), nil // scalar bytes, nothing to type
	}
	out, err := json.Marshal(javaTypeWrap(generic))
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// javaTypeWrap recursively attaches Java type ids to non-final compound
// values (JsonJacksonCodec default-typing compatible).
func javaTypeWrap(v any) any {
	switch t := v.(type) {
	case map[string]any:
		if _, has := t["@class"]; !has {
			t["@class"] = "java.util.LinkedHashMap"
		}
		for k, val := range t {
			t[k] = javaTypeWrap(val)
		}
		return t
	case []any:
		wrapped := make([]any, len(t))
		for i, e := range t {
			wrapped[i] = javaTypeWrap(e)
		}
		return []any{"java.util.ArrayList", wrapped}
	}
	return v
}

// Decode implements Codec. Numbers decode to json.Number to preserve
// precision across languages.
func (JSONCodec) Decode(s string) (any, error) {
	if s == "" {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return s, nil // not JSON – return raw value (matches redi.py)
	}
	return unwrap(v), nil
}

// longWrap wraps ints outside the Java int range as ["java.lang.Long", v].
func longWrap(v any) (string, bool) {
	switch n := v.(type) {
	case int:
		if n >= math.MinInt32 && n <= math.MaxInt32 {
			return "", false
		}
		b, _ := json.Marshal([]any{"java.lang.Long", n})
		return string(b), true
	case int64:
		if n >= math.MinInt32 && n <= math.MaxInt32 {
			return "", false
		}
		b, _ := json.Marshal([]any{"java.lang.Long", n})
		return string(b), true
	case int32, uint8, uint16, float32, float64, bool, string, nil:
		return "", false
	case uint:
		if uint64(n) <= math.MaxInt32 {
			return "", false
		}
		b, _ := json.Marshal([]any{"java.lang.Long", n})
		return string(b), true
	case uint32:
		if n <= math.MaxInt32 {
			return "", false
		}
		b, _ := json.Marshal([]any{"java.lang.Long", n})
		return string(b), true
	case uint64:
		if n <= math.MaxInt32 {
			return "", false
		}
		b, _ := json.Marshal([]any{"java.lang.Long", n})
		return string(b), true
	}
	return "", false
}

var javaCollectionTypes = map[string]bool{
	"java.util.ArrayList":        true,
	"java.util.LinkedList":       true,
	"java.util.HashMap":          true,
	"java.util.LinkedHashMap":    true,
	"java.util.TreeMap":          true,
	"java.util.HashSet":          true,
	"java.util.LinkedHashSet":    true,
	"java.util.TreeSet":          true,
	"java.util.Collections$Unmo": true, // unmodifiableList etc. (Redisson 3.x)
}

// unwrap resolves Redisson typed wrappers recursively and strips "@class".
func unwrap(v any) any {
	switch t := v.(type) {
	case []any:
		if len(t) == 2 {
			if s, ok := t[0].(string); ok &&
				(javaCollectionTypes[s] || strings.HasPrefix(s, "java.lang.")) {
				return unwrap(t[1])
			}
		}
		for i := range t {
			t[i] = unwrap(t[i])
		}
		return t
	case map[string]any:
		delete(t, "@class")
		for k, val := range t {
			t[k] = unwrap(val)
		}
		return t
	}
	return v
}

// encodeKey encodes a key/field/member for wire storage (JSON, like values).
func encodeKey(c Codec, k string) string {
	s, err := c.Encode(k)
	if err != nil {
		return k
	}
	return s
}
