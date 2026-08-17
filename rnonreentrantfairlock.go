package redi

// RNonReentrantFairLock is Redisson's FIFO lock variant that rejects a second
// acquisition by its current holder with ErrLockReentrant.
type RNonReentrantFairLock struct {
	*RFairLock
}

func newRNonReentrantFairLock(c *Client, name string) *RNonReentrantFairLock {
	lock := newRFairLock(c, name)
	lock.reentrant = false
	return &RNonReentrantFairLock{RFairLock: lock}
}
