package redi

import (
	"context"
	"encoding/binary"
	"log"
	"math"

	"github.com/minio/highwayhash"
	"github.com/redis/go-redis/v9"
)

// RBloomFilter is a distributed bloom filter, wire-compatible with Redisson:
// HighwayHash-128 with Redisson's fixed key over the codec-encoded element,
// bitmap stored at the raw name, config HASH at {name}:config.
type RBloomFilter struct {
	rObject

	size         int64
	hashIters    int64
	expected     int64
	falseProb    float64
	configLoaded bool
}

func newRBloomFilter(c *Client, name string) *RBloomFilter {
	return &RBloomFilter{
		rObject:   rObject{c: c, name: name},
		expected:  55_000_000,
		falseProb: 0.03,
	}
}

// Redisson misc.Hash KEY (HighwayHash seed), little-endian encoded.
var bloomHashKey = func() []byte {
	key := make([]byte, 32)
	words := []uint64{0x9e3779b97f4a7c15, 0xf39cc0605cedc834, 0x1082276bf3a27251, 0xf86c6a11d0c18e95}
	for i, w := range words {
		binary.LittleEndian.PutUint64(key[i*8:], w)
	}
	return key
}()

func (f *RBloomFilter) hash128(element any) (uint64, uint64) {
	enc, err := f.c.codec.Encode(element)
	if err != nil {
		log.Printf("redi: bloom encode: %v", err)
		return 0, 0
	}
	sum := highwayhashSum128([]byte(enc))
	return binary.LittleEndian.Uint64(sum[0:8]), binary.LittleEndian.Uint64(sum[8:16])
}

// highwayhashSum128 hashes with Redisson's fixed HighwayHash key.
func highwayhashSum128(data []byte) []byte {
	sum := highwayhash.Sum128(data, bloomHashKey)
	return sum[:]
}

// bitIndex mirrors Redisson: h starts at h1, alternately adding h2/h1.
func (f *RBloomFilter) bitIndex(h1, h2 uint64, iteration int) int64 {
	var h = h1 & 0x7FFFFFFFFFFFFFFF
	for i := 0; i < iteration; i++ {
		if i%2 == 0 {
			h = (h + h2) & 0x7FFFFFFFFFFFFFFF
		} else {
			h = (h + h1) & 0x7FFFFFFFFFFFFFFF
		}
	}
	return int64(h % uint64(f.size))
}

// optimalBits matches Java's double-to-long truncation (not ceil).
func optimalBits(n int64, p float64) int64 {
	return int64(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
}

// optimalHashIters matches Java's Math.round (half-up, not banker's).
func optimalHashIters(n, m int64) int64 {
	v := math.Floor(float64(m)/float64(n)*math.Log(2) + 0.5)
	if v < 1 {
		return 1
	}
	return int64(v)
}

var bloomInitScript = redis.NewScript(`
if redis.call('exists', KEYS[1]) == 1 then
    return 0
end
redis.call('hset', KEYS[1],
    'size', ARGV[1],
    'hashIterations', ARGV[2],
    'expectedInsertions', ARGV[3],
    'falseProbability', ARGV[4])
return 1
`)

func (f *RBloomFilter) configKey() string {
	return suffixName(f.name, "config")
}

// TryInit initializes the filter only when it has not been initialized yet.
func (f *RBloomFilter) TryInit(ctx context.Context, expectedInsertions int64, falseProbability float64) (bool, error) {
	f.expected = expectedInsertions
	f.falseProb = falseProbability
	f.size = optimalBits(expectedInsertions, falseProbability)
	f.hashIters = optimalHashIters(expectedInsertions, f.size)
	n, err := bloomInitScript.Run(ctx, f.rc(), []string{f.configKey()},
		f.size, f.hashIters, expectedInsertions,
		formatFloat(falseProbability)).Int()
	if err != nil {
		return false, err
	}
	if n == 1 {
		f.configLoaded = true
		return true, nil
	}
	return false, f.ensureConfig(ctx)
}

func (f *RBloomFilter) ensureConfig(ctx context.Context) error {
	if f.configLoaded {
		return nil
	}
	m, err := f.rc().HGetAll(ctx, f.configKey()).Result()
	if err != nil {
		return err
	}
	if len(m) > 0 {
		f.size = parseInt64(m["size"])
		f.hashIters = parseInt64(m["hashIterations"])
		f.configLoaded = true
		return nil
	}
	_, err = f.TryInit(ctx, f.expected, f.falseProb)
	return err
}

// Add adds element, returning true when it was not present before.
func (f *RBloomFilter) Add(ctx context.Context, element any) (bool, error) {
	if err := f.ensureConfig(ctx); err != nil {
		return false, err
	}
	h1, h2 := f.hash128(element)
	added := false
	for i := int64(0); i < f.hashIters; i++ {
		bit := f.bitIndex(h1, h2, int(i))
		v, err := f.rc().GetBit(ctx, f.name, bit).Result()
		if err != nil {
			return false, err
		}
		if v == 0 {
			if err := f.rc().SetBit(ctx, f.name, bit, 1).Err(); err != nil {
				return false, err
			}
			added = true
		}
	}
	return added, nil
}

// AddAll adds elements and returns how many set at least one previously clear
// bit, matching Redisson's collection add count.
func (f *RBloomFilter) AddAll(ctx context.Context, elements ...any) (int64, error) {
	var added int64
	for _, element := range elements {
		ok, err := f.Add(ctx, element)
		if err != nil {
			return 0, err
		}
		if ok {
			added++
		}
	}
	return added, nil
}

// Contains reports whether element may have been added (false = definitely not).
func (f *RBloomFilter) Contains(ctx context.Context, element any) (bool, error) {
	if err := f.ensureConfig(ctx); err != nil {
		return false, err
	}
	h1, h2 := f.hash128(element)
	for i := int64(0); i < f.hashIters; i++ {
		bit := f.bitIndex(h1, h2, int(i))
		v, err := f.rc().GetBit(ctx, f.name, bit).Result()
		if err != nil {
			return false, err
		}
		if v == 0 {
			return false, nil
		}
	}
	return true, nil
}

// ContainsAll returns how many supplied elements may be present.
func (f *RBloomFilter) ContainsAll(ctx context.Context, elements ...any) (int64, error) {
	var contained int64
	for _, element := range elements {
		ok, err := f.Contains(ctx, element)
		if err != nil {
			return 0, err
		}
		if ok {
			contained++
		}
	}
	return contained, nil
}

// Count estimates the number of distinct elements added.
func (f *RBloomFilter) Count(ctx context.Context) (int64, error) {
	if err := f.ensureConfig(ctx); err != nil {
		return 0, err
	}
	bits, err := f.rc().BitCount(ctx, f.name, nil).Result()
	if err != nil {
		return 0, err
	}
	if bits == 0 || f.size == 0 {
		return 0, nil
	}
	frac := float64(bits) / float64(f.size)
	if frac >= 1 {
		return math.MaxInt64, nil
	}
	return int64(-float64(f.size) * math.Log(1-frac) / float64(f.hashIters)), nil
}

// ExpectedInsertions returns the configured expected insertions.
func (f *RBloomFilter) ExpectedInsertions(ctx context.Context) (int64, error) {
	return f.expected, f.ensureConfig(ctx)
}

// FalseProbability returns the configured false positive probability.
func (f *RBloomFilter) FalseProbability(ctx context.Context) (float64, error) {
	return f.falseProb, f.ensureConfig(ctx)
}

// HashIterations returns the number of hash functions used.
func (f *RBloomFilter) HashIterations(ctx context.Context) (int64, error) {
	if err := f.ensureConfig(ctx); err != nil {
		return 0, err
	}
	return f.hashIters, nil
}

// Size returns the bitmap size in bits.
func (f *RBloomFilter) Size(ctx context.Context) (int64, error) {
	if err := f.ensureConfig(ctx); err != nil {
		return 0, err
	}
	return f.size, nil
}

// Delete removes the bitmap and its config.
func (f *RBloomFilter) Delete(ctx context.Context) error {
	if err := f.rc().Del(ctx, f.name).Err(); err != nil {
		return err
	}
	return f.rc().Del(ctx, f.configKey()).Err()
}
