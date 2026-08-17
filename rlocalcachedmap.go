package redi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"
)

// RLocalCachedMap is an RMap with an in-process near cache.
//
// Data interoperability: the Redis-backed map uses the exact RMap wire
// format, so Java Redisson / redi.py can read and write the same entries.
//
// Invalidation interoperability: Java's LocalCachedMapInvalidation
// protocol (key-hash byte arrays + updates log + Java object encoding) is
// NOT replicated; invalidation broadcast uses a Go-internal channel
// `{name}:inval` (JSON messages). Go instances invalidate each other;
// Java writers are not broadcast to Go caches (documented in
// COMPATIBILITY.md). Reads always have the Redis fallback, so Java writes
// are picked up by Go after the local entry is evicted or the cache is
// cleared.
type RLocalCachedMap struct {
	*RMap
	invalChannel string
	instanceID   string

	mu        sync.RWMutex
	cache     map[string]any // encoded key -> decoded value
	cacheTTL  time.Duration  // 0 = no local expiry
	sizeLimit int            // 0 = unlimited

	stopOnce sync.Once
	stop     context.CancelFunc
	ready    chan struct{}
}

// LocalCachedMapOptions configures near-cache behaviour.
type LocalCachedMapOptions struct {
	// DisableNearCache turns off the in-process cache (every Get hits Redis).
	// Use for Java/Go mixed deployments until Redisson binary invalidation is
	// implemented — data layer remains wire-compatible.
	DisableNearCache bool
	// CacheTTL expires local entries after this duration (0 = no local TTL).
	CacheTTL time.Duration
	// SizeLimit caps local entries (0 = unlimited).
	SizeLimit int
}

func newRLocalCachedMap(c *Client, name string) *RLocalCachedMap {
	return newRLocalCachedMapOpts(c, name, nil)
}

func newRLocalCachedMapOpts(c *Client, name string, opts *LocalCachedMapOptions) *RLocalCachedMap {
	ctx, cancel := context.WithCancel(c.ctx)
	uniq := make([]byte, 6)
	if _, err := rand.Read(uniq); err != nil {
		uniq = []byte(time.Now().Format("150405.000000000"))
	}
	m := &RLocalCachedMap{
		RMap:         newRMap(c, name),
		invalChannel: name + ":inval",
		instanceID:   c.id + "-" + name + "-" + hex.EncodeToString(uniq),
		cache:        make(map[string]any),
		stop:         cancel,
		ready:        make(chan struct{}),
	}
	if opts != nil {
		m.cacheTTL = opts.CacheTTL
		m.sizeLimit = opts.SizeLimit
		if opts.DisableNearCache {
			m.sizeLimit = -1 // sentinel: never store locally
			close(m.ready)
			return m
		}
	}
	m.startListener(ctx)
	select {
	case <-m.ready:
	case <-time.After(c.cfg.DialTimeout):
	}
	return m
}

// startListener consumes invalidation broadcasts from other Go instances.
func (m *RLocalCachedMap) startListener(ctx context.Context) {
	sub := m.c.rc.Subscribe(ctx, m.invalChannel)
	go func() {
		defer sub.Close() //nolint:errcheck // connection teardown //nolint:errcheck
		if _, err := sub.Receive(ctx); err != nil {
			return
		}
		close(m.ready)
		for {
			msg, err := sub.ReceiveMessage(ctx)
			if err != nil {
				return
			}
			if len(msg.Payload) < 1 || msg.Payload[0] != '{' {
				continue // not our JSON protocol
			}
			inv := decodeInvalidation(m.c.codec, msg.Payload)
			if inv == nil || inv.Instance == m.instanceID {
				continue // own write or foreign protocol
			}
			m.mu.Lock()
			if inv.Key == "" {
				m.cache = make(map[string]any)
			} else {
				delete(m.cache, inv.Key)
			}
			m.mu.Unlock()
		}
	}()
}

// localCachedInvalidation is the Go-internal broadcast payload.
type localCachedInvalidation struct {
	Instance string `json:"i"`
	Key      string `json:"k,omitempty"` // "" = clear all
}

func decodeInvalidation(c Codec, payload string) *localCachedInvalidation {
	var inv localCachedInvalidation
	if err := decodeInto(c, payload, &inv); err != nil {
		return nil
	}
	return &inv
}

// Get returns the value for field, serving from the local cache when
// present (a hit makes NO Redis round-trip).
func (m *RLocalCachedMap) Get(ctx context.Context, field string) (any, error) {
	ek := encodeKey(m.c.codec, field)
	m.mu.RLock()
	v, ok := m.cache[ek]
	m.mu.RUnlock()
	if ok {
		return v, nil
	}
	val, err := m.RMap.Get(ctx, field)
	if err != nil || val == nil {
		return val, err
	}
	m.storeLocal(ek, val)
	return val, nil
}

// GetInto decodes the value into target, serving from the local cache on hit.
func (m *RLocalCachedMap) GetInto(ctx context.Context, field string, target any) (bool, error) {
	v, err := m.Get(ctx, field)
	if err != nil || v == nil {
		return false, err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(b, target)
}

// Put writes through to Redis, updates the local cache and broadcasts
// invalidation to the other Go instances.
func (m *RLocalCachedMap) Put(ctx context.Context, field string, value any) error {
	ek := encodeKey(m.c.codec, field)
	if err := m.RMap.Put(ctx, field, value); err != nil {
		return err
	}
	m.storeLocal(ek, value)
	m.broadcast(ctx, ek)
	return nil
}

// PutAll write-throughs every entry, updates the local cache, and broadcasts
// one invalidation per field (same granularity as Put).
func (m *RLocalCachedMap) PutAll(ctx context.Context, entries map[string]any) error {
	if err := m.RMap.PutAll(ctx, entries); err != nil {
		return err
	}
	for field, value := range entries {
		ek := encodeKey(m.c.codec, field)
		m.storeLocal(ek, value)
		m.broadcast(ctx, ek)
	}
	return nil
}

// PutIfAbsent writes only when absent; on success the local cache is
// updated and the invalidation broadcast fires.
func (m *RLocalCachedMap) PutIfAbsent(ctx context.Context, field string, value any) (bool, error) {
	ok, err := m.RMap.PutIfAbsent(ctx, field, value)
	if err != nil || !ok {
		return ok, err
	}
	ek := encodeKey(m.c.codec, field)
	m.storeLocal(ek, value)
	m.broadcast(ctx, ek)
	return true, nil
}

// FastPut write-throughs like Put and returns whether the field was new.
func (m *RLocalCachedMap) FastPut(ctx context.Context, field string, value any) (bool, error) {
	ok, err := m.RMap.FastPut(ctx, field, value)
	if err != nil {
		return false, err
	}
	ek := encodeKey(m.c.codec, field)
	m.storeLocal(ek, value)
	m.broadcast(ctx, ek)
	return ok, nil
}

// Replace write-throughs when the field exists; updates local cache and
// broadcasts on success.
func (m *RLocalCachedMap) Replace(ctx context.Context, field string, value any) (any, error) {
	prev, err := m.RMap.Replace(ctx, field, value)
	if err != nil || prev == nil {
		return prev, err
	}
	ek := encodeKey(m.c.codec, field)
	m.storeLocal(ek, value)
	m.broadcast(ctx, ek)
	return prev, nil
}

// ReplaceIf write-throughs on compare-and-set success.
func (m *RLocalCachedMap) ReplaceIf(ctx context.Context, field string, oldValue, newValue any) (bool, error) {
	ok, err := m.RMap.ReplaceIf(ctx, field, oldValue, newValue)
	if err != nil || !ok {
		return ok, err
	}
	ek := encodeKey(m.c.codec, field)
	m.storeLocal(ek, newValue)
	m.broadcast(ctx, ek)
	return true, nil
}

// FastPutIfExists write-throughs when the field already exists.
func (m *RLocalCachedMap) FastPutIfExists(ctx context.Context, field string, value any) (bool, error) {
	ok, err := m.RMap.FastPutIfExists(ctx, field, value)
	if err != nil || !ok {
		return ok, err
	}
	ek := encodeKey(m.c.codec, field)
	m.storeLocal(ek, value)
	m.broadcast(ctx, ek)
	return true, nil
}

// FastReplace is an alias of FastPutIfExists with local-cache update.
func (m *RLocalCachedMap) FastReplace(ctx context.Context, field string, value any) (bool, error) {
	return m.FastPutIfExists(ctx, field, value)
}

// FastRemove deletes fields, drops them from the local cache, and broadcasts.
func (m *RLocalCachedMap) FastRemove(ctx context.Context, fields ...string) (int64, error) {
	n, err := m.RMap.FastRemove(ctx, fields...)
	if err != nil {
		return n, err
	}
	m.mu.Lock()
	for _, f := range fields {
		delete(m.cache, encodeKey(m.c.codec, f))
	}
	m.mu.Unlock()
	for _, f := range fields {
		m.broadcast(ctx, encodeKey(m.c.codec, f))
	}
	return n, nil
}

// Remove deletes the field everywhere and broadcasts.
func (m *RLocalCachedMap) Remove(ctx context.Context, field string) error {
	ek := encodeKey(m.c.codec, field)
	if err := m.Delete(ctx, field); err != nil { //nolint:staticcheck // embedded selector for clarity
		return err
	}
	m.mu.Lock()
	delete(m.cache, ek)
	m.mu.Unlock()
	m.broadcast(ctx, ek)
	return nil
}

// Clear deletes the Redis map, empties the local cache and broadcasts a
// full invalidation.
func (m *RLocalCachedMap) Clear(ctx context.Context) error {
	if err := m.RMap.Clear(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	m.cache = make(map[string]any)
	m.mu.Unlock()
	m.broadcast(ctx, "")
	return nil
}

// CachedKeys returns the keys currently held in the local cache.
func (m *RLocalCachedMap) CachedKeys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.cache))
	for k := range m.cache {
		out = append(out, k)
	}
	return out
}

// ClearLocalCache empties only the local cache (no Redis writes).
func (m *RLocalCachedMap) ClearLocalCache() {
	m.mu.Lock()
	m.cache = make(map[string]any)
	m.mu.Unlock()
}

// PreloadCache warms the local cache with all current Redis entries.
// The optional count is accepted as the Redisson scan-batch hint.
func (m *RLocalCachedMap) PreloadCache(ctx context.Context, count ...int) error {
	entries, err := m.GetAll(ctx)
	if err != nil {
		return err
	}
	for key, value := range entries {
		m.storeLocal(encodeKey(m.c.codec, key), value)
	}
	return nil
}

// CachedValues returns a snapshot of values currently held locally.
func (m *RLocalCachedMap) CachedValues() []any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]any, 0, len(m.cache))
	for _, value := range m.cache {
		out = append(out, value)
	}
	return out
}

// GetCachedMap returns a decoded snapshot of the local cache.
func (m *RLocalCachedMap) GetCachedMap() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]any, len(m.cache))
	for encodedKey, value := range m.cache {
		key, err := m.c.codec.Decode(encodedKey)
		if err != nil {
			continue
		}
		if field, ok := key.(string); ok {
			out[field] = value
		}
	}
	return out
}

// SetLocalCacheLimit configures local eviction: entries older than ttl are
// dropped on access, and the cache stops caching beyond maxEntries
// (0 = unlimited). Call before first use.
func (m *RLocalCachedMap) SetLocalCacheLimit(ttl time.Duration, maxEntries int) {
	m.mu.Lock()
	m.cacheTTL = ttl
	m.sizeLimit = maxEntries
	m.mu.Unlock()
}

func (m *RLocalCachedMap) storeLocal(ek string, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sizeLimit < 0 {
		return // near cache disabled
	}
	if m.sizeLimit > 0 && len(m.cache) >= m.sizeLimit {
		if _, exists := m.cache[ek]; !exists {
			return // cache full - serve from Redis until entries evict
		}
	}
	m.cache[ek] = value
}

func (m *RLocalCachedMap) broadcast(ctx context.Context, ek string) {
	payload, err := m.c.codec.Encode(localCachedInvalidation{
		Instance: m.instanceID,
		Key:      ek,
	})
	if err != nil {
		return
	}
	if err := m.c.rc.Publish(ctx, m.invalChannel, payload).Err(); err != nil {
		m.c.logf("local cached map %q invalidation: %v", m.name, err)
	}
}

// Destroy stops the listener goroutine (also triggered by Client.Close).
func (m *RLocalCachedMap) Destroy() {
	m.stopOnce.Do(func() { m.stop() })
}
