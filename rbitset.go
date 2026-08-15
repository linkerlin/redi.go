package redi

import (
	"context"
)

// RBitSet is a distributed bitset over a Redis bitmap. Bit indexes are the
// raw Redis GETBIT/SETBIT indexes — identical to Java Redisson's
// RedissonBitSet (verified from its toByteArray: bit i maps to
// byte[i/8] bit (7-i%8), i.e. native MSB-first Redis numbering; no
// reversal anywhere).
type RBitSet struct {
	rObject
}

func newRBitSet(c *Client, name string) *RBitSet {
	return &RBitSet{rObject{c: c, name: name}}
}

// Get returns the bit at index.
func (b *RBitSet) Get(ctx context.Context, bitIndex int64) (bool, error) {
	n, err := b.rc().GetBit(ctx, b.name, bitIndex).Result()
	return n == 1, err
}

// Set sets (or clears) the bit, returning its previous value.
func (b *RBitSet) Set(ctx context.Context, bitIndex int64, value bool) (bool, error) {
	v := 0
	if value {
		v = 1
	}
	prev, err := b.rc().SetBit(ctx, b.name, bitIndex, v).Result()
	return prev == 1, err
}

// Clear clears the bit, returning its previous value.
func (b *RBitSet) Clear(ctx context.Context, bitIndex int64) (bool, error) {
	return b.Set(ctx, bitIndex, false)
}

// Cardinality returns the number of set bits (BITCOUNT).
func (b *RBitSet) Cardinality(ctx context.Context) (int64, error) {
	return b.rc().BitCount(ctx, b.name, nil).Result()
}

// Length returns the highest set bit index + 1 (Java BitSet semantics;
// 0 when empty).
func (b *RBitSet) Length(ctx context.Context) (int64, error) {
	raw, err := b.rc().Get(ctx, b.name).Result()
	if err != nil { //nolint:errorlint // redis.Nil and real errors both mean "no length"
		return 0, nil
	}
	for bi := len(raw) - 1; bi >= 0; bi-- {
		byteVal := raw[bi]
		if byteVal == 0 {
			continue
		}
		for bit := 0; bit < 8; bit++ {
			if byteVal&(1<<uint(7-bit)) != 0 {
				return int64(bi)*8 + int64(bit) + 1, nil
			}
		}
	}
	return 0, nil
}

// ClearAll deletes the bitset.
func (b *RBitSet) ClearAll(ctx context.Context) error {
	return b.Delete(ctx)
}

// And / Or / Xor apply BITOP with other bitsets, storing into this one.
func (b *RBitSet) And(ctx context.Context, others ...string) error {
	_, err := b.rc().BitOpAnd(ctx, b.name, b.keys(others)...).Result()
	return err
}

// Or applies a bitwise OR.
func (b *RBitSet) Or(ctx context.Context, others ...string) error {
	_, err := b.rc().BitOpOr(ctx, b.name, b.keys(others)...).Result()
	return err
}

// Xor applies a bitwise XOR.
func (b *RBitSet) Xor(ctx context.Context, others ...string) error {
	_, err := b.rc().BitOpXor(ctx, b.name, b.keys(others)...).Result()
	return err
}

// Not applies a bitwise NOT against another bitset.
func (b *RBitSet) Not(ctx context.Context, other string) error {
	_, err := b.rc().BitOpNot(ctx, b.name, other).Result()
	return err
}

func (b *RBitSet) keys(others []string) []string {
	return append([]string{b.name}, others...)
}

// ToByteArray returns the raw bitmap bytes (MSB-first, matching Java).
func (b *RBitSet) ToByteArray(ctx context.Context) ([]byte, error) {
	raw, err := b.rc().Get(ctx, b.name).Result()
	if err != nil { //nolint:errorlint // redis.Nil = empty bitset
		return nil, nil
	}
	return []byte(raw), nil
}

// FromByteArray replaces the bitset with raw bytes (MSB-first, matching Java).
func (b *RBitSet) FromByteArray(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return b.Delete(ctx)
	}
	return b.rc().Set(ctx, b.name, data, 0).Err()
}
