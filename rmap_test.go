package redi_test

import (
	"context"
	"testing"

	redi "github.com/linkerlin/redi.go"
)

func TestRMap_PutGetDelete(t *testing.T) {
	if !redisAvailable(t) {
		return
	}
	cfg := redi.DefaultConfig()
	client, err := redi.NewClient(cfg)
	if err != nil {
		t.Fatal("NewClient:", err)
	}
	defer client.Close()

	m := client.GetRMap(uniqueKey(t, "map"))
	ctx := context.Background()
	defer m.Clear(ctx) //nolint:errcheck

	if err := m.Put(ctx, "hello", "world"); err != nil {
		t.Fatal("Put:", err)
	}

	val, err := m.Get(ctx, "hello")
	if err != nil {
		t.Fatal("Get:", err)
	}
	if val != "world" {
		t.Errorf("Get = %q, want %q", val, "world")
	}

	ok, err := m.ContainsKey(ctx, "hello")
	if err != nil {
		t.Fatal("ContainsKey:", err)
	}
	if !ok {
		t.Error("ContainsKey(hello) = false, want true")
	}

	if err := m.Delete(ctx, "hello"); err != nil {
		t.Fatal("Delete:", err)
	}

	_, err = m.Get(ctx, "hello")
	if !isNilErr(err) {
		t.Errorf("Get after delete: err = %v, want redis.Nil", err)
	}
}

func TestRMap_Size(t *testing.T) {
	if !redisAvailable(t) {
		return
	}
	cfg := redi.DefaultConfig()
	client, _ := redi.NewClient(cfg)
	defer client.Close()

	m := client.GetRMap(uniqueKey(t, "size"))
	ctx := context.Background()
	defer m.Clear(ctx) //nolint:errcheck

	_ = m.Put(ctx, "a", "1")
	_ = m.Put(ctx, "b", "2")

	sz, err := m.Size(ctx)
	if err != nil {
		t.Fatal("Size:", err)
	}
	if sz != 2 {
		t.Errorf("Size = %d, want 2", sz)
	}
}
