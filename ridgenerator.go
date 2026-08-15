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
	allocLoaded bool
	allocSize   int64
	nextID      int64
	maxID       int64
}

func newRIdGenerator(c *Client, name string) *RIdGenerator {
	return &RIdGenerator{
		rObject:   rObject{c: c, name: name},
		allocSize: 1000,
	}
}

var idAllocateScript = redis.NewScript(`
local key = KEYS[1]
local allocation_size = tonumber(ARGV[1])
local current = redis.call('get', key)
if current == false then
    current = 0
else
    current = tonumber(current)
end
local next_val = current + allocation_size
redis.call('set', key, next_val)
return {current + 1, next_val}
`)

func (g *RIdGenerator) allocationKey() string {
	return suffixName(g.name, "allocation")
}

// TryInit sets the initial value and allocation size once.
func (g *RIdGenerator) TryInit(ctx context.Context, value, allocationSize int64) (bool, error) {
	ok, err := g.rc().SetNX(ctx, g.name, value, 0).Result()
	if err != nil || !ok {
		return false, err
	}
	if err := g.rc().Set(ctx, g.allocationKey(), allocationSize, 0).Err(); err != nil {
		return false, err
	}
	g.mu.Lock()
	g.allocSize = allocationSize
	g.allocLoaded = true
	g.mu.Unlock()
	return true, nil
}

func (g *RIdGenerator) loadAllocation(ctx context.Context) error {
	g.mu.Lock()
	loaded := g.allocLoaded
	g.mu.Unlock()
	if loaded {
		return nil
	}
	v, err := g.rc().Get(ctx, g.allocationKey()).Result()
	if err == nil {
		g.mu.Lock()
		g.allocSize = parseInt64(v)
		g.allocLoaded = true
		g.mu.Unlock()
	} else if err == redis.Nil {
		g.mu.Lock()
		g.allocLoaded = true
		g.mu.Unlock()
	} else {
		return err
	}
	return nil
}

func (g *RIdGenerator) allocate(ctx context.Context) error {
	g.mu.Lock()
	size := g.allocSize
	g.mu.Unlock()
	res, err := idAllocateScript.Run(ctx, g.rc(), []string{g.name}, size).Result()
	if err != nil {
		return err
	}
	pair, ok := res.([]any)
	if !ok || len(pair) != 2 {
		return ErrIDAlloc
	}
	lo, ok1 := pair[0].(int64)
	hi, ok2 := pair[1].(int64)
	if !ok1 || !ok2 {
		return ErrIDAlloc
	}
	g.mu.Lock()
	g.nextID = lo
	g.maxID = hi
	g.mu.Unlock()
	return nil
}

// NextID returns the next unique id (unique, not strictly monotonic).
func (g *RIdGenerator) NextID(ctx context.Context) (int64, error) {
	if err := g.loadAllocation(ctx); err != nil {
		return 0, err
	}
	g.mu.Lock()
	needAlloc := g.nextID >= g.maxID
	g.mu.Unlock()
	if needAlloc {
		if err := g.allocate(ctx); err != nil {
			return 0, err
		}
	}
	g.mu.Lock()
	id := g.nextID
	g.nextID++
	g.mu.Unlock()
	return id, nil
}

// NextIDs allocates count ids in batches.
func (g *RIdGenerator) NextIDs(ctx context.Context, count int) ([]int64, error) {
	if err := g.loadAllocation(ctx); err != nil {
		return nil, err
	}
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
