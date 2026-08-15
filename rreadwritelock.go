package redi

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RReadWriteLock is a distributed read-write lock using a single Redis HASH
// (wire-compatible with Redisson's RedissonReadWriteLock layout):
//
//	HASH {name}
//	  mode                    = "read" | "write" | "read-write"
//	  {clientID}              = read-lock hold count
//	  {clientID}:write        = write-lock hold count
//
// All acquire/release paths are atomic Lua. Channel redisson_rwlock:{name}
// wakes waiters on full release (message 0 after write release, 1 after read
// release, matching Redisson's LockPubSub messages).
//
// Unlike RLock there is no watchdog: leases expire after the TTL (30s default).
type RReadWriteLock struct {
	read  *RReadWriteSubLock
	write *RReadWriteSubLock
}

func newRReadWriteLock(c *Client, name string) *RReadWriteLock {
	rw := &RReadWriteLock{}
	rw.read = &RReadWriteSubLock{
		rObject:  rObject{c: c, name: name},
		channel:  prefixName("redisson_rwlock", name),
		isWrite:  false,
		rw:       rw,
		renewers: make(map[string]context.CancelFunc),
	}
	rw.write = &RReadWriteSubLock{
		rObject:  rObject{c: c, name: name},
		channel:  prefixName("redisson_rwlock", name),
		isWrite:  true,
		rw:       rw,
		renewers: make(map[string]context.CancelFunc),
	}
	return rw
}

// ReadLock returns the read (shared) lock view.
func (l *RReadWriteLock) ReadLock() *RReadWriteSubLock { return l.read }

// WriteLock returns the write (exclusive) lock view.
func (l *RReadWriteLock) WriteLock() *RReadWriteSubLock { return l.write }

// RReadWriteSubLock is either the read or the write half of an RReadWriteLock.
type RReadWriteSubLock struct {
	rObject
	channel string
	isWrite bool
	rw      *RReadWriteLock

	mu       sync.Mutex
	renewers map[string]context.CancelFunc // clientID -> watchdog cancel
}

func (s *RReadWriteSubLock) entries(clientID string) (primary, paired string) {
	readName := clientID
	writeName := clientID + ":write"
	if s.isWrite {
		return writeName, readName
	}
	return readName, writeName
}

func (s *RReadWriteSubLock) lease(ttl time.Duration) time.Duration {
	if ttl > 0 {
		return ttl
	}
	return s.c.cfg.LockWatchdogTimeout
}

// rwReadAcquireScript follows Redisson semantics, including the verified
// same-thread write→read downgrade (mode stays "write").
var rwReadAcquireScript = redis.NewScript(`
if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
    redis.call('hincrby', KEYS[1], ARGV[1], 1)
    redis.call('pexpire', KEYS[1], ARGV[3])
    return nil
end
if redis.call('exists', KEYS[1]) == 0 then
    redis.call('hincrby', KEYS[1], ARGV[1], 1)
    redis.call('hset', KEYS[1], 'mode', 'read')
    redis.call('pexpire', KEYS[1], ARGV[3])
    return nil
end
local mode = redis.call('hget', KEYS[1], 'mode')
if mode == 'read' or (mode == 'read-write' and redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
    redis.call('hset', KEYS[1], 'mode', 'read-write')
    redis.call('hincrby', KEYS[1], ARGV[1], 1)
    redis.call('pexpire', KEYS[1], ARGV[3])
    return nil
end
if mode == 'write' and redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    redis.call('hincrby', KEYS[1], ARGV[1], 1)
    redis.call('pexpire', KEYS[1], ARGV[3])
    return nil
end
return redis.call('pttl', KEYS[1])
`)

var rwWriteAcquireScript = redis.NewScript(`
local mode = redis.call('hget', KEYS[1], 'mode')
if mode == false then
    redis.call('hset', KEYS[1], 'mode', 'write')
    redis.call('hset', KEYS[1], ARGV[1], 1)
    redis.call('pexpire', KEYS[1], ARGV[3])
    return nil
end
if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
    redis.call('hincrby', KEYS[1], ARGV[1], 1)
    local cur = redis.call('pttl', KEYS[1])
    if cur > tonumber(ARGV[3]) then
        redis.call('pexpire', KEYS[1], cur)
    else
        redis.call('pexpire', KEYS[1], ARGV[3])
    end
    return nil
end
return redis.call('pttl', KEYS[1])
`)

// rwReadUnlockScript removes one read hold, repairing the mode field.
// ARGV[1]=readName ARGV[2]=writeName ARGV[3]=lease ARGV[4]=read unlock msg.
var rwReadUnlockScript = redis.NewScript(`
if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then
    return -1
end
local counter = redis.call('hincrby', KEYS[1], ARGV[1], -1)
if counter > 0 then
    redis.call('pexpire', KEYS[1], ARGV[3])
    return counter
end
redis.call('hdel', KEYS[1], ARGV[1])
local keys = redis.call('hkeys', KEYS[1])
local anyReaders = false
for _, k in ipairs(keys) do
    if k ~= 'mode' and k ~= ARGV[2] then
        anyReaders = true
        break
    end
end
if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    if anyReaders then
        redis.call('hset', KEYS[1], 'mode', 'read-write')
    else
        redis.call('hset', KEYS[1], 'mode', 'write')
    end
    return 0
end
if anyReaders then
    redis.call('hset', KEYS[1], 'mode', 'read')
else
    redis.call('del', KEYS[1])
    redis.call('publish', KEYS[2], ARGV[4])
end
return 0
`)

// rwWriteUnlockScript removes one write hold, repairing the mode field.
var rwWriteUnlockScript = redis.NewScript(`
if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then
    return -1
end
local counter = redis.call('hincrby', KEYS[1], ARGV[1], -1)
if counter > 0 then
    redis.call('pexpire', KEYS[1], ARGV[3])
    return counter
end
redis.call('hdel', KEYS[1], ARGV[1])
if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    redis.call('hset', KEYS[1], 'mode', 'read')
    return 0
end
local keys = redis.call('hkeys', KEYS[1])
local any = false
for _, k in ipairs(keys) do
    if k ~= 'mode' then
        any = true
        break
    end
end
if any then
    redis.call('hset', KEYS[1], 'mode', 'read')
else
    redis.call('del', KEYS[1])
    redis.call('publish', KEYS[2], ARGV[4])
end
return 0
`)

// TryLock attempts one acquisition. With ttl <= 0 the watchdog renews the
// lease (see Lock).
func (s *RReadWriteSubLock) TryLock(ctx context.Context, clientID string, ttl time.Duration) (bool, error) {
	primary, paired := s.entries(clientID)
	script := rwReadAcquireScript
	if s.isWrite {
		script = rwWriteAcquireScript
	}
	_, err := script.Run(ctx, s.rc(), []string{s.name},
		primary, paired, s.lease(ttl).Milliseconds()).Result()
	if err == redis.Nil {
		if ttl <= 0 {
			s.startRenewer(clientID)
		}
		return true, nil // Lua nil = acquired
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

// Lock blocks until acquired, waking on the rwlock channel.
func (s *RReadWriteSubLock) Lock(ctx context.Context, clientID string, ttl time.Duration) error {
	ok, err := s.TryLock(ctx, clientID, ttl)
	if err != nil || ok {
		return err
	}
	sub := s.subscribe(ctx, s.channel)
	defer sub.Close()
	wake := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-time.After(time.Second):
		}
		ok, err := s.TryLock(ctx, clientID, ttl)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
}

// Unlock releases one hold. Returns ErrLockNotHeld when not the owner.
// The watchdog is stopped only after the ownership check succeeds and the
// hold count reaches zero.
func (s *RReadWriteSubLock) Unlock(ctx context.Context, clientID string) error {
	primary, paired := s.entries(clientID)
	script := rwReadUnlockScript
	msg := "1" // READ_UNLOCK_MESSAGE
	if s.isWrite {
		script = rwWriteUnlockScript
		msg = unlockMsg
	}
	n, err := script.Run(ctx, s.rc(), []string{s.name, s.channel},
		primary, paired, s.lease(0).Milliseconds(), msg).Int()
	if err != nil {
		return err
	}
	if n < 0 {
		return ErrLockNotHeld
	}
	if n == 0 {
		s.stopRenewer(clientID)
	}
	return nil
}

// IsLocked reports whether this half is held (write half checks the mode).
func (s *RReadWriteSubLock) IsLocked(ctx context.Context) (bool, error) {
	if s.isWrite {
		mode, err := s.rc().HGet(ctx, s.name, "mode").Result()
		if err == redis.Nil {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return mode == "write" || mode == "read-write", nil
	}
	return s.Exists(ctx)
}

// IsHeldBy reports whether clientID holds this half.
func (s *RReadWriteSubLock) IsHeldBy(ctx context.Context, clientID string) (bool, error) {
	primary, _ := s.entries(clientID)
	return s.rc().HExists(ctx, s.name, primary).Result()
}

// ForceUnlock deletes the lock key and wakes all waiters.
func (s *RReadWriteSubLock) ForceUnlock(ctx context.Context) (bool, error) {
	n, err := s.rc().Del(ctx, s.name).Result()
	s.stopAllRenewers()
	return n > 0, err
}

// startRenewer launches (once per clientID on this half) the watchdog that
// renews the lease at LockWatchdogTimeout/3 until cancelled, the hold is
// fully released, or ownership is lost.
func (s *RReadWriteSubLock) startRenewer(clientID string) {
	s.mu.Lock()
	if _, ok := s.renewers[clientID]; ok {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(s.c.ctx)
	s.renewers[clientID] = cancel
	s.mu.Unlock()

	primary, _ := s.entries(clientID)
	lease := s.c.cfg.LockWatchdogTimeout
	interval := lease / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.renewers, clientID)
			s.mu.Unlock()
		}()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := lockRenewScript.Run(ctx, s.rc(), []string{s.name},
					primary, lease.Milliseconds()).Int()
				if err != nil {
					s.c.logf("rwlock %q renew: %v", s.name, err)
					continue
				}
				if n == 0 {
					return // ownership lost
				}
			}
		}
	}()
}

func (s *RReadWriteSubLock) stopRenewer(clientID string) {
	s.mu.Lock()
	cancel := s.renewers[clientID]
	delete(s.renewers, clientID)
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *RReadWriteSubLock) stopAllRenewers() {
	s.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(s.renewers))
	for _, cancel := range s.renewers {
		cancels = append(cancels, cancel)
	}
	s.renewers = make(map[string]context.CancelFunc)
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
