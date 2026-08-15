package redi_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	redi "github.com/linkerlin/redi.go"
)

func TestRQueue_OfferPollPeek(t *testing.T) {
	if !redisAvailable(t) {
		return
	}
	cfg := redi.DefaultConfig()
	client, err := redi.NewClient(cfg)
	if err != nil {
		t.Fatal("NewClient:", err)
	}
	defer client.Close()

	ctx := context.Background()
	q := client.GetRQueue(uniqueKey(t, "queue"))
	defer q.Clear(ctx) //nolint:errcheck

	if err := q.Offer(ctx, "first", "second"); err != nil {
		t.Fatal("Offer:", err)
	}

	head, err := q.Peek(ctx)
	if err != nil {
		t.Fatal("Peek:", err)
	}
	if head != "first" {
		t.Errorf("Peek = %q, want %q", head, "first")
	}

	val, err := q.Poll(ctx)
	if err != nil {
		t.Fatal("Poll:", err)
	}
	if val != "first" {
		t.Errorf("Poll = %q, want %q", val, "first")
	}

	sz, _ := q.Size(ctx)
	if sz != 1 {
		t.Errorf("Size after poll = %d, want 1", sz)
	}

	// Empty after second poll
	_, _ = q.Poll(ctx)
	_, err = q.Poll(ctx)
	if !isNilErr(err) {
		t.Errorf("Poll on empty: err = %v, want redis.Nil", err)
	}
}

func TestRQueue_EmptyPeek(t *testing.T) {
	if !redisAvailable(t) {
		return
	}
	cfg := redi.DefaultConfig()
	client, _ := redi.NewClient(cfg)
	defer client.Close()

	ctx := context.Background()
	q := client.GetRQueue(uniqueKey(t, "empty"))
	defer q.Clear(ctx) //nolint:errcheck

	_, err := q.Peek(ctx)
	if !isNilErr(err) {
		t.Errorf("Peek on empty queue: err = %v, want redis.Nil", err)
	}

	// Ensure size is 0
	sz, _ := q.Size(ctx)
	if sz != 0 {
		t.Errorf("Size of empty queue = %d, want 0", sz)
	}
}

// Compile-time import guard to keep redis.Nil accessible from this file.
var _ = redis.Nil
