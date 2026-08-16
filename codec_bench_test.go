package redi_test

import (
	"testing"

	redi "github.com/linkerlin/redi.go"
)

// BenchmarkCodecEncode quantifies the scalar fast-path win from v0.2.6
// (scalars previously paid the 3-pass marshal/unmarshal/remarshal even
// though they need no Java type wrapping).
func BenchmarkCodecEncode_ScalarString(b *testing.B) {
	c := redi.JSONCodec{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Encode("hello world"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodecEncode_ScalarInt(b *testing.B) {
	c := redi.JSONCodec{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Encode(42); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodecEncode_Map(b *testing.B) {
	c := redi.JSONCodec{}
	v := map[string]any{"k": "v", "n": 1}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Encode(v); err != nil {
			b.Fatal(err)
		}
	}
}
