package redi

import (
	"context"
	"errors"
	"strconv"
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

// SetRange sets every bit in the inclusive range [from, toInclusive].
func (b *RBitSet) SetRange(ctx context.Context, from, toInclusive int64) error {
	return b.setRange(ctx, from, toInclusive, true)
}

// ClearRange clears every bit in the inclusive range [from, toInclusive].
func (b *RBitSet) ClearRange(ctx context.Context, from, toInclusive int64) error {
	return b.setRange(ctx, from, toInclusive, false)
}

func (b *RBitSet) setRange(ctx context.Context, from, toInclusive int64, value bool) error {
	if from > toInclusive {
		return nil
	}
	v := 0
	if value {
		v = 1
	}
	args := make([]any, 0)
	for index := from; ; index++ {
		args = append(args, "SET", "u1", index, v)
		if index == toInclusive {
			break
		}
	}
	return b.rc().BitField(ctx, b.name, args...).Err()
}

// GetMany returns bits in the same order as indexes.
func (b *RBitSet) GetMany(ctx context.Context, indexes ...int64) ([]bool, error) {
	if len(indexes) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(indexes)*3)
	for _, index := range indexes {
		args = append(args, "GET", "u1", index)
	}
	values, err := b.rc().BitField(ctx, b.name, args...).Result()
	if err != nil {
		return nil, err
	}
	result := make([]bool, len(values))
	for i, value := range values {
		result[i] = value == 1
	}
	return result, nil
}

// SetMany sets all indexes to value in one BITFIELD command.
func (b *RBitSet) SetMany(ctx context.Context, value bool, indexes ...int64) error {
	if len(indexes) == 0 {
		return nil
	}
	v := 0
	if value {
		v = 1
	}
	args := make([]any, 0, len(indexes)*4)
	for _, index := range indexes {
		args = append(args, "SET", "u1", index, v)
	}
	return b.rc().BitField(ctx, b.name, args...).Err()
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

// Size returns the logical bit length (highest set bit index + 1).
func (b *RBitSet) Size(ctx context.Context) (int64, error) {
	return b.Length(ctx)
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

// Diff stores bits set in this bitset and absent from every other bitset.
// It returns the stored bitmap length in bytes.
func (b *RBitSet) Diff(ctx context.Context, others ...string) (int64, error) {
	return b.bitOp(ctx, "DIFF", others)
}

// AndOr stores bits set in this bitset and in at least one other bitset.
func (b *RBitSet) AndOr(ctx context.Context, others ...string) (int64, error) {
	return b.bitOp(ctx, "ANDOR", others)
}

// SetExclusive stores bits set in exactly one source bitset.
func (b *RBitSet) SetExclusive(
	ctx context.Context, others ...string,
) (int64, error) {
	return b.bitOp(ctx, "ONE", others)
}

func (b *RBitSet) bitOp(
	ctx context.Context, operation string, others []string,
) (int64, error) {
	sources := b.keys(others)
	args := make([]any, 0, 3+len(sources))
	args = append(args, "BITOP", operation, b.name)
	for _, source := range sources {
		args = append(args, source)
	}
	return b.c.rc.Do(ctx, args...).Int64()
}

// Not applies bitwise NOT in place. An optional source name preserves the
// previous Not(ctx, otherName) form.
func (b *RBitSet) Not(ctx context.Context, source ...string) error {
	if len(source) > 1 {
		return errors.New("redi: bitset Not accepts at most one source")
	}
	name := b.name
	if len(source) == 1 {
		name = source[0]
	}
	_, err := b.rc().BitOpNot(ctx, b.name, name).Result()
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

// BitFieldGet reads a signed or unsigned integer field.
func (b *RBitSet) BitFieldGet(ctx context.Context, signed bool, width int, offset int64) (int64, error) {
	encoding, err := bitFieldEncoding(signed, width)
	if err != nil {
		return 0, err
	}
	return b.bitFieldOne(ctx, "GET", encoding, offset)
}

// BitFieldSet writes a field and returns its previous value.
func (b *RBitSet) BitFieldSet(
	ctx context.Context,
	signed bool,
	width int,
	offset, value int64,
) (int64, error) {
	encoding, err := bitFieldEncoding(signed, width)
	if err != nil {
		return 0, err
	}
	return b.bitFieldOne(ctx, "SET", encoding, offset, value)
}

// BitFieldIncrBy increments a field and returns its new value (WRAP overflow).
func (b *RBitSet) BitFieldIncrBy(
	ctx context.Context,
	signed bool,
	width int,
	offset, increment int64,
) (int64, error) {
	encoding, err := bitFieldEncoding(signed, width)
	if err != nil {
		return 0, err
	}
	return b.bitFieldOne(ctx, "INCRBY", encoding, offset, increment)
}

func (b *RBitSet) bitFieldOne(ctx context.Context, args ...any) (int64, error) {
	values, err := b.rc().BitField(ctx, b.name, args...).Result()
	if err != nil {
		return 0, err
	}
	if len(values) == 0 {
		return 0, nil
	}
	return values[0], nil
}

func bitFieldEncoding(signed bool, width int) (string, error) {
	if signed {
		if width < 1 || width > 64 {
			return "", errors.New("redi: signed bit field width must be between 1 and 64")
		}
		return "i" + strconv.Itoa(width), nil
	}
	if width < 1 || width > 63 {
		return "", errors.New("redi: unsigned bit field width must be between 1 and 63")
	}
	return "u" + strconv.Itoa(width), nil
}
