package redi_test

import (
	"context"
	"testing"

	redi "github.com/linkerlin/redi.go"
)

func TestRList_AddGetSize(t *testing.T) {
	if !redisAvailable(t) {
		return
	}
	cfg := redi.DefaultConfig()
	client, err := redi.NewClient(cfg)
	if err != nil {
		t.Fatal("NewClient:", err)
	}
	defer client.Close()

	l := client.GetRList(uniqueKey(t, "list"))
	ctx := context.Background()
	defer l.Clear(ctx) //nolint:errcheck

	if err := l.Add(ctx, "a", "b", "c"); err != nil {
		t.Fatal("Add:", err)
	}

	sz, err := l.Size(ctx)
	if err != nil {
		t.Fatal("Size:", err)
	}
	if sz != 3 {
		t.Errorf("Size = %d, want 3", sz)
	}

	val, err := l.Get(ctx, 1)
	if err != nil {
		t.Fatal("Get:", err)
	}
	if val != "b" {
		t.Errorf("Get(1) = %q, want %q", val, "b")
	}

	all, err := l.Range(ctx, 0, -1)
	if err != nil {
		t.Fatal("Range:", err)
	}
	if len(all) != 3 {
		t.Errorf("Range len = %d, want 3", len(all))
	}
}

func TestRList_Remove(t *testing.T) {
	if !redisAvailable(t) {
		return
	}
	cfg := redi.DefaultConfig()
	client, _ := redi.NewClient(cfg)
	defer client.Close()

	l := client.GetRList(uniqueKey(t, "rm"))
	ctx := context.Background()
	defer l.Clear(ctx) //nolint:errcheck

	_ = l.Add(ctx, "x", "x", "y")
	_ = l.Remove(ctx, 2, "x")

	sz, _ := l.Size(ctx)
	if sz != 1 {
		t.Errorf("after remove size = %d, want 1", sz)
	}
}
