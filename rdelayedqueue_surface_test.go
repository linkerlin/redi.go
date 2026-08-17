package redi_test

import (
	"testing"
	"time"
)

func TestRDelayedQueuePendingSurface(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "dq-surface")
	q := client.GetDelayedQueue(name)
	defer q.Delete(testCtx) //nolint:errcheck

	for _, value := range []any{"duplicate", "duplicate", "other"} {
		if err := q.Offer(testCtx, value, time.Hour); err != nil {
			t.Fatal("Offer:", err)
		}
	}

	contains, err := q.Contains(testCtx, "duplicate")
	if err != nil || !contains {
		t.Fatalf("Contains(duplicate) = %v, %v; want true, nil", contains, err)
	}
	contains, err = q.Contains(testCtx, "missing")
	if err != nil || contains {
		t.Fatalf("Contains(missing) = %v, %v; want false, nil", contains, err)
	}

	values, err := q.ReadAll(testCtx)
	if err != nil || len(values) != 3 ||
		values[0] != "duplicate" || values[1] != "duplicate" || values[2] != "other" {
		t.Fatalf("ReadAll = %#v, %v", values, err)
	}

	removed, err := q.Remove(testCtx, "duplicate")
	if err != nil || !removed {
		t.Fatalf("Remove(duplicate) = %v, %v; want true, nil", removed, err)
	}
	rc := rawClient(t)
	listSize, err := rc.LLen(testCtx, "redisson_delay_queue:{"+name+"}").Result()
	if err != nil || listSize != 2 {
		t.Fatalf("internal list size = %d, %v; want 2, nil", listSize, err)
	}
	zsetSize, err := rc.ZCard(testCtx, "redisson_delay_queue_timeout:{"+name+"}").Result()
	if err != nil || zsetSize != 2 {
		t.Fatalf("timeout zset size = %d, %v; want 2, nil", zsetSize, err)
	}

	ready := client.GetQueue(name)
	if err := ready.Offer(testCtx, "ready"); err != nil {
		t.Fatal("ready Offer:", err)
	}
	if err := q.Clear(testCtx); err != nil {
		t.Fatal("Clear:", err)
	}
	if value, err := ready.Peek(testCtx); err != nil || value != "ready" {
		t.Fatalf("target queue after Clear = %v, %v; want ready, nil", value, err)
	}
	internalKeys := []string{
		"redisson_delay_queue:{" + name + "}",
		"redisson_delay_queue_timeout:{" + name + "}",
	}
	if n, err := rc.Exists(testCtx, internalKeys...).Result(); err != nil || n != 0 {
		t.Fatalf("internal keys after Clear = %d, %v; want 0, nil", n, err)
	}
}
