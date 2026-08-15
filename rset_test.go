package redi_test

import (
	"context"
	"testing"

	redi "github.com/linkerlin/redi.go"
)

func TestRSet_AddContainsRemove(t *testing.T) {
	if !redisAvailable(t) {
		return
	}
	cfg := redi.DefaultConfig()
	client, err := redi.NewClient(cfg)
	if err != nil {
		t.Fatal("NewClient:", err)
	}
	defer client.Close()

	s := client.GetRSet(uniqueKey(t, "set"))
	ctx := context.Background()
	defer s.Clear(ctx) //nolint:errcheck

	if err := s.Add(ctx, "a", "b", "c"); err != nil {
		t.Fatal("Add:", err)
	}

	ok, err := s.Contains(ctx, "b")
	if err != nil {
		t.Fatal("Contains:", err)
	}
	if !ok {
		t.Error("Contains(b) = false, want true")
	}

	sz, err := s.Size(ctx)
	if err != nil {
		t.Fatal("Size:", err)
	}
	if sz != 3 {
		t.Errorf("Size = %d, want 3", sz)
	}

	if err := s.Remove(ctx, "b"); err != nil {
		t.Fatal("Remove:", err)
	}

	ok, _ = s.Contains(ctx, "b")
	if ok {
		t.Error("Contains(b) after remove = true, want false")
	}
}
