package redi

func (o *rObject) synchronizerName(key any, suffix string) string {
	encoded, err := o.c.codec.Encode(key)
	if err != nil {
		panic(err)
	}
	return suffixName(o.name, redissonHash128Base64(encoded)+":"+suffix)
}

// GetLock returns the Redisson lock associated with a map key.
func (m *RMap) GetLock(key any) *RLock {
	return newRLock(m.c, m.synchronizerName(key, "lock"))
}

// GetFairLock returns the Redisson fair lock associated with a map key.
func (m *RMap) GetFairLock(key any) *RFairLock {
	return newRFairLock(m.c, m.synchronizerName(key, "fairlock"))
}

// GetReadWriteLock returns the Redisson read-write lock for a map key.
func (m *RMap) GetReadWriteLock(key any) *RReadWriteLock {
	return newRReadWriteLock(m.c, m.synchronizerName(key, "rw_lock"))
}

// GetSemaphore returns the Redisson semaphore associated with a map key.
func (m *RMap) GetSemaphore(key any) *RSemaphore {
	return newRSemaphore(m.c, m.synchronizerName(key, "semaphore"))
}

// GetCountDownLatch returns the Redisson latch associated with a map key.
func (m *RMap) GetCountDownLatch(key any) *RCountDownLatch {
	return newRCountDownLatch(m.c, m.synchronizerName(key, "countdownlatch"))
}

// GetPermitExpirableSemaphore returns the Redisson expirable semaphore for a map key.
func (m *RMap) GetPermitExpirableSemaphore(key any) *RPermitExpirableSemaphore {
	return newRPermitExpirableSemaphore(
		m.c, m.synchronizerName(key, "permitexpirablesemaphore"),
	)
}

// GetLock returns the Redisson lock associated with a set value.
func (s *RSet) GetLock(value any) *RLock {
	return newRLock(s.c, s.synchronizerName(value, "lock"))
}

// GetFairLock returns the Redisson fair lock associated with a set value.
func (s *RSet) GetFairLock(value any) *RFairLock {
	return newRFairLock(s.c, s.synchronizerName(value, "fairlock"))
}

// GetReadWriteLock returns the Redisson read-write lock for a set value.
func (s *RSet) GetReadWriteLock(value any) *RReadWriteLock {
	return newRReadWriteLock(s.c, s.synchronizerName(value, "rw_lock"))
}

// GetSemaphore returns the Redisson semaphore associated with a set value.
func (s *RSet) GetSemaphore(value any) *RSemaphore {
	return newRSemaphore(s.c, s.synchronizerName(value, "semaphore"))
}

// GetCountDownLatch returns the Redisson latch associated with a set value.
func (s *RSet) GetCountDownLatch(value any) *RCountDownLatch {
	return newRCountDownLatch(s.c, s.synchronizerName(value, "countdownlatch"))
}

// GetPermitExpirableSemaphore returns the Redisson expirable semaphore for a set value.
func (s *RSet) GetPermitExpirableSemaphore(value any) *RPermitExpirableSemaphore {
	return newRPermitExpirableSemaphore(
		s.c, s.synchronizerName(value, "permitexpirablesemaphore"),
	)
}

// GetLock returns the Redisson lock associated with a multimap key.
func (m *RMultimap) GetLock(key any) *RLock {
	return newRLock(m.c, m.synchronizerName(key, "lock"))
}

// GetFairLock returns the Redisson fair lock associated with a multimap key.
func (m *RMultimap) GetFairLock(key any) *RFairLock {
	return newRFairLock(m.c, m.synchronizerName(key, "fairlock"))
}

// GetReadWriteLock returns the Redisson read-write lock for a multimap key.
func (m *RMultimap) GetReadWriteLock(key any) *RReadWriteLock {
	return newRReadWriteLock(m.c, m.synchronizerName(key, "rw_lock"))
}

// GetSemaphore returns the Redisson semaphore associated with a multimap key.
func (m *RMultimap) GetSemaphore(key any) *RSemaphore {
	return newRSemaphore(m.c, m.synchronizerName(key, "semaphore"))
}

// GetLock returns the Redisson lock associated with a multimap-cache key.
func (m *rMultimapCache) GetLock(key any) *RLock {
	return newRLock(m.c, m.synchronizerName(key, "lock"))
}

// GetFairLock returns the Redisson fair lock for a multimap-cache key.
func (m *rMultimapCache) GetFairLock(key any) *RFairLock {
	return newRFairLock(m.c, m.synchronizerName(key, "fairlock"))
}

// GetReadWriteLock returns the Redisson read-write lock for a cache key.
func (m *rMultimapCache) GetReadWriteLock(key any) *RReadWriteLock {
	return newRReadWriteLock(m.c, m.synchronizerName(key, "rw_lock"))
}

// GetSemaphore returns the Redisson semaphore for a multimap-cache key.
func (m *rMultimapCache) GetSemaphore(key any) *RSemaphore {
	return newRSemaphore(m.c, m.synchronizerName(key, "semaphore"))
}
