package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RSetMultimapCacheNative / RListMultimapCacheNative add per-key TTL via
// Redis HPEXPIRE on the index HASH field plus PEXPIRE on the values
// collection (RedissonMultimapCacheNative 4.6.1). Requires Redis ≥ 7.4.
type RSetMultimapCacheNative struct{ *RSetMultimap }

type RListMultimapCacheNative struct{ *RListMultimap }

func newRSetMultimapCacheNative(c *Client, name string) *RSetMultimapCacheNative {
	return &RSetMultimapCacheNative{RSetMultimap: newRSetMultimap(c, name)}
}

func newRListMultimapCacheNative(c *Client, name string) *RListMultimapCacheNative {
	return &RListMultimapCacheNative{RListMultimap: newRListMultimap(c, name)}
}

var multimapCacheNativeExpireKeyScript = redis.NewScript(`
local res = redis.call('hpexpire', KEYS[1], ARGV[1], 'fields', 1, ARGV[2])
if res[1] == 1 then
    redis.call('pexpire', KEYS[2], ARGV[1])
    return 1
end
return 0
`)

func (m *RSetMultimapCacheNative) ExpireKey(ctx context.Context, key any, ttl time.Duration) (bool, error) {
	return expireMultimapNativeKey(ctx, &m.RMultimap, key, ttl)
}

func (m *RListMultimapCacheNative) ExpireKey(ctx context.Context, key any, ttl time.Duration) (bool, error) {
	return expireMultimapNativeKey(ctx, &m.RMultimap, key, ttl)
}

func expireMultimapNativeKey(ctx context.Context, m *RMultimap, key any, ttl time.Duration) (bool, error) {
	ek, err := m.c.codec.Encode(key)
	if err != nil {
		return false, err
	}
	collKey := m.collectionKey(m.internalID(ek))
	n, err := multimapCacheNativeExpireKeyScript.Run(ctx, m.rc(),
		[]string{m.name, collKey}, ttl.Milliseconds(), ek).Int()
	return n == 1, err
}
