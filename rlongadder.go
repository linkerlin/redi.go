package redi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// adder core implements the Redisson BaseAdder coordination protocol,
// source-verified against Redisson 4.6.1:
//
//	channel {name}:adder-topic     messages "1:<reqId>" = SUM, "0:<reqId>" = CLEAR
//	                            (StringCodec — plain text, no codec wrapping)
//	string {name}:{reqId}:counter  flush target: every instance INCRBYs its
//	                            full local total here; the requester GETDELs it
//	string {name}:{reqId}:semaphore release barrier: publish returns the
//	                            subscriber count n; the requester acquires n
//	                            permits (each instance releases one after
//	                            flushing), then deletes the key
//
// The requester is itself a subscriber, so its own listener performs its own
// flush — exactly like Java. Cross-language sums work because Java instances
// respond to Go's topic messages (and vice versa) with identical semantics.
type rAdderCore struct {
	rObject
	topic string

	localI   atomic.Int64
	localF   float64
	fMu      sync.Mutex // guards localF (no atomic float)
	isDouble bool

	cancel context.CancelFunc
	sub    *redis.PubSub
	wg     sync.WaitGroup
}

func (a *rAdderCore) start() {
	ctx, cancel := context.WithCancel(a.c.ctx)
	a.cancel = cancel
	sub := a.c.rc.Subscribe(ctx, a.topic)
	a.sub = sub
	ready := make(chan struct{})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		// (no sub.Close here: PubSub.Receive does NOT wake on ctx cancel -
		// only Close() unblocks it, and destroy() owns the Close.)
		// The subscribe ack is consumed HERE; ready fires only after the
		// subscription is live, so the first real message cannot be
		// swallowed by a competing readiness receive.
		if _, err := sub.Receive(ctx); err != nil {
			close(ready)
			return
		}
		close(ready)
		for {
			msg, err := sub.ReceiveMessage(ctx)
			if err != nil {
				return
			}
			a.handle(msg.Payload)
		}
	}()
	select {
	case <-ready:
	case <-time.After(a.c.cfg.DialTimeout):
	}
}

func (a *rAdderCore) counterName(id string) string { return suffixName(a.name, id+":counter") }
func (a *rAdderCore) semaphoreName(id string) string {
	return suffixName(a.name, id+":semaphore")
}

// handle responds to another instance's SUM/CLEAR request.
func (a *rAdderCore) handle(msg string) {
	kind, id, ok := strings.Cut(msg, ":")
	if !ok {
		return
	}
	switch kind {
	case "1": // SUM: flush local total into the requester's counter cell.
		if a.isDouble {
			a.fMu.Lock()
			v := a.localF
			a.fMu.Unlock()
			if err := a.rc().IncrByFloat(a.c.ctx, a.counterName(id), v).Err(); err != nil {
				a.c.logf("adder %q flush: %v", a.name, err)
			}
		} else {
			if err := a.rc().IncrBy(a.c.ctx, a.counterName(id), a.localI.Load()).Err(); err != nil {
				a.c.logf("adder %q flush: %v", a.name, err)
			}
		}
	case "0": // CLEAR: drop the local buffer.
		if a.isDouble {
			a.fMu.Lock()
			a.localF = 0
			a.fMu.Unlock()
		} else {
			a.localI.Store(0)
		}
	default:
		return
	}
	// Release the barrier permit (release = INCRBY on the semaphore key).
	if _, err := semReleaseScript.Run(a.c.ctx, a.rc(),
		[]string{a.semaphoreName(id), prefixName("redisson_sc", a.semaphoreName(id))}, 1).Result(); err != nil {
		a.c.logf("adder %q barrier release: %v", a.name, err)
	}
}

// request runs the publish → barrier → collect cycle. collect reads and
// deletes the requester's counter cell.
func (a *rAdderCore) request(ctx context.Context, kind string) (int64, float64, bool, error) {
	uniq := make([]byte, 16)
	if _, err := rand.Read(uniq); err != nil {
		return 0, 0, false, err
	}
	id := hex.EncodeToString(uniq)

	n, err := a.rc().Publish(ctx, a.topic, kind+":"+id).Result()
	if err != nil {
		return 0, 0, false, err
	}
	if n == 0 {
		// No live subscribers (this adder's listener included) — the local
		// buffer is the whole state.
		if a.isDouble {
			a.fMu.Lock()
			defer a.fMu.Unlock()
			return 0, a.localF, true, nil
		}
		return a.localI.Load(), 0, true, nil
	}

	// Wait for all n instances to release the barrier (short poll; sums are
	// a coordination point, not a hot path).
	deadline := time.Now().Add(60 * time.Second) // Java sumAsync default timeout
	sem := a.semaphoreName(id)
	for {
		ok, err := semAcquireScript.Run(ctx, a.rc(), []string{sem}, n).Int()
		if err == nil && ok == 1 {
			break
		}
		if ctx.Err() != nil {
			return 0, 0, false, ctx.Err()
		}
		if time.Now().After(deadline) {
			return 0, 0, false, fmt.Errorf("redi: adder %q sum barrier timed out waiting for %d instances", a.name, n)
		}
		time.Sleep(10 * time.Millisecond)
	}

	raw, err := a.rc().GetDel(ctx, a.counterName(id)).Result()
	if err == redis.Nil {
		err = nil
		raw = "0"
	}
	if err != nil {
		return 0, 0, false, err
	}
	_ = a.rc().Del(ctx, sem).Err()

	if a.isDouble {
		f, perr := strconv.ParseFloat(raw, 64)
		if perr != nil {
			return 0, 0, false, perr
		}
		return 0, f, true, nil
	}
	i, perr := strconv.ParseInt(raw, 10, 64)
	if perr != nil {
		return 0, 0, false, perr
	}
	return i, 0, true, nil
}

func (a *rAdderCore) destroy() {
	if a.cancel != nil {
		a.cancel()
	}
	if a.sub != nil {
		_ = a.sub.Close() // unblocks the listener's Receive (ctx won't)
	}
	a.wg.Wait()
}

// RLongAdder is a high-contention distributed counter: writes accumulate in
// a local atomic (zero network), Sum() coordinates a flush across all live
// instances (Go and Java alike) and returns the grand total. Wire-compatible
// with Redisson's RedissonLongAdder.
type RLongAdder struct{ rAdderCore }

func newRLongAdder(c *Client, name string) *RLongAdder {
	a := &RLongAdder{rAdderCore{rObject: rObject{c: c, name: name}, topic: suffixName(name, "adder-topic")}}
	a.start()
	return a
}

// Add adds x to the local buffer (no network round-trip).
func (a *RLongAdder) Add(x int64) { a.localI.Add(x) }

// Increment adds 1.
func (a *RLongAdder) Increment() { a.localI.Add(1) }

// Decrement subtracts 1.
func (a *RLongAdder) Decrement() { a.localI.Add(-1) }

// Sum flushes every live instance's buffer and returns the total
// (non-destructive, matching Java).
func (a *RLongAdder) Sum(ctx context.Context) (int64, error) {
	v, _, _, err := a.request(ctx, "1")
	return v, err
}

// Reset clears every live instance's local buffer.
func (a *RLongAdder) Reset(ctx context.Context) error {
	_, _, _, err := a.request(ctx, "0")
	return err
}

// Destroy unsubscribes this instance (also triggered by Client.Close).
func (a *RLongAdder) Destroy() { a.destroy() }

// RDoubleAdder is the double variant of RLongAdder.
type RDoubleAdder struct{ rAdderCore }

func newRDoubleAdder(c *Client, name string) *RDoubleAdder {
	a := &RDoubleAdder{rAdderCore{
		rObject: rObject{c: c, name: name}, topic: suffixName(name, "adder-topic"), isDouble: true,
	}}
	a.start()
	return a
}

// Add adds x to the local buffer.
func (a *RDoubleAdder) Add(x float64) {
	a.fMu.Lock()
	a.localF += x
	a.fMu.Unlock()
}

// Increment adds 1.
func (a *RDoubleAdder) Increment() { a.Add(1) }

// Decrement subtracts 1.
func (a *RDoubleAdder) Decrement() { a.Add(-1) }

// Sum flushes every live instance's buffer and returns the total.
func (a *RDoubleAdder) Sum(ctx context.Context) (float64, error) {
	_, v, _, err := a.request(ctx, "1")
	return v, err
}

// Reset clears every live instance's local buffer.
func (a *RDoubleAdder) Reset(ctx context.Context) error {
	_, _, _, err := a.request(ctx, "0")
	return err
}

// Destroy unsubscribes this instance.
func (a *RDoubleAdder) Destroy() { a.destroy() }
