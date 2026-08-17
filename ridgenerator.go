package redi

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

// RIdGenerator is a distributed id generator: each process caches a range of
// ids (allocation size persisted at {name}:allocation, Redisson-compatible)
// to keep Redis round-trips low.
type RIdGenerator struct {
	rObject

	mu          sync.Mutex
	allocMu     sync.Mutex
	allocLoaded bool
	allocSize   int64
	nextID      int64
	maxID       int64
}

func newRIdGenerator(c *Client, name string) *RIdGenerator {
	return &RIdGenerator{
		rObject:   rObject{c: c, name: name},
		allocSize: 5000,
	}
}

var idInitScript = redis.NewScript(`
redis.call('setnx', KEYS[1], ARGV[1])
return redis.call('setnx', KEYS[2], ARGV[2])
`)

var idAllocateScript = redis.NewScript(`
local allocation_size = redis.call('get', KEYS[2])
if allocation_size == false then
    allocation_size = 5000
    redis.call('set', KEYS[2], allocation_size)
else
    allocation_size = tonumber(allocation_size)
end
local value = redis.call('get', KEYS[1])
if value == false then
    redis.call('incr', KEYS[1])
    value = 1
else
    value = tonumber(value)
end
redis.call('incrby', KEYS[1], allocation_size)
return {value, allocation_size}
`)

func (g *RIdGenerator) allocationKey() string {
	return suffixName(g.name, "allocation")
}

// Delete removes both the counter and its allocation-size companion.
func (g *RIdGenerator) Delete(ctx context.Context) error {
	return g.rc().Del(ctx, g.name, g.allocationKey()).Err()
}

// TryInit sets the initial value and allocation size once.
func (g *RIdGenerator) TryInit(ctx context.Context, value, allocationSize int64) (bool, error) {
	n, err := idInitScript.Run(ctx, g.rc(),
		[]string{g.name, g.allocationKey()}, value, allocationSize).Int()
	if err != nil {
		return false, err
	}
	g.mu.Lock()
	g.allocSize = allocationSize
	g.allocLoaded = true
	g.mu.Unlock()
	return n == 1, nil
}

func (g *RIdGenerator) loadAllocation(ctx context.Context) error {
	g.mu.Lock()
	loaded := g.allocLoaded
	g.mu.Unlock()
	if loaded {
		return nil
	}
	v, err := g.rc().Get(ctx, g.allocationKey()).Result()
	switch err {
	case nil:
		g.mu.Lock()
		g.allocSize = parseInt64(v)
		g.allocLoaded = true
		g.mu.Unlock()
	case redis.Nil:
		g.mu.Lock()
		g.allocLoaded = true
		g.mu.Unlock()
	default:
		return err
	}
	return nil
}

func (g *RIdGenerator) allocate(ctx context.Context) error {
	res, err := idAllocateScript.Run(ctx, g.rc(),
		[]string{g.name, g.allocationKey()}).Result()
	if err != nil {
		return err
	}
	pair, ok := res.([]any)
	if !ok || len(pair) != 2 {
		return ErrIDAlloc
	}
	start, ok1 := pair[0].(int64)
	size, ok2 := pair[1].(int64)
	if !ok1 || !ok2 {
		return ErrIDAlloc
	}
	g.mu.Lock()
	g.allocSize = size
	g.nextID = start
	g.maxID = start + size
	g.mu.Unlock()
	return nil
}

// NextID returns the next unique id (unique, not strictly monotonic).
func (g *RIdGenerator) NextID(ctx context.Context) (int64, error) {
	if err := g.loadAllocation(ctx); err != nil {
		return 0, err
	}
	for {
		g.mu.Lock()
		if g.nextID < g.maxID {
			id := g.nextID
			g.nextID++
			g.mu.Unlock()
			return id, nil
		}
		g.mu.Unlock()

		g.allocMu.Lock()
		g.mu.Lock()
		needAlloc := g.nextID >= g.maxID
		g.mu.Unlock()
		if needAlloc {
			if err := g.allocate(ctx); err != nil {
				g.allocMu.Unlock()
				return 0, err
			}
		}
		g.allocMu.Unlock()
	}
}

// NextIDs allocates count ids in batches.
func (g *RIdGenerator) NextIDs(ctx context.Context, count int) ([]int64, error) {
	if err := g.loadAllocation(ctx); err != nil {
		return nil, err
	}
	g.allocMu.Lock()
	defer g.allocMu.Unlock()
	ids := make([]int64, 0, count)
	for len(ids) < count {
		g.mu.Lock()
		needAlloc := g.nextID >= g.maxID
		g.mu.Unlock()
		if needAlloc {
			if err := g.allocate(ctx); err != nil {
				return nil, err
			}
		}
		g.mu.Lock()
		take := int64(count - len(ids))
		if rem := g.maxID - g.nextID; rem < take {
			take = rem
		}
		for i := int64(0); i < take; i++ {
			ids = append(ids, g.nextID+i)
		}
		g.nextID += take
		g.mu.Unlock()
	}
	return ids, nil
}
