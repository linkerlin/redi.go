package redi_test

import (
	"testing"
	"time"
)

func TestRScoredSortedSet_SetOperations(t *testing.T) {
	client := newTestClient(t)
	base := uniqueKey(t, "scored-set-ops")
	name := "{" + base + "}:source"
	otherName := "{" + base + "}:other"
	intersectionDest := "{" + base + "}:intersection"
	unionDest := "{" + base + "}:union"
	diffDest := "{" + base + "}:diff"
	defer func() {
		_, _ = client.GetKeys().Delete(
			testCtx, name, otherName, intersectionDest, unionDest, diffDest,
		)
	}()

	set := client.GetScoredSortedSet(name)
	other := client.GetScoredSortedSet(otherName)
	for member, score := range map[string]float64{"a": 1, "b": 2} {
		if _, err := set.Add(testCtx, member, score); err != nil {
			t.Fatal("Add source:", err)
		}
	}
	for member, score := range map[string]float64{"b": 3, "c": 4} {
		if _, err := other.Add(testCtx, member, score); err != nil {
			t.Fatal("Add other:", err)
		}
	}

	intersection, err := set.Intersection(testCtx, otherName)
	if err != nil || len(intersection) != 1 || intersection[0] != "b" {
		t.Fatalf("Intersection = %v, %v; want [b], nil", intersection, err)
	}
	union, err := set.Union(testCtx, otherName)
	if err != nil || len(union) != 3 {
		t.Fatalf("Union = %v, %v; want 3 members, nil", union, err)
	}
	diff, err := set.Diff(testCtx, otherName)
	if err != nil || len(diff) != 1 || diff[0] != "a" {
		t.Fatalf("Diff = %v, %v; want [a], nil", diff, err)
	}

	if n, err := set.IntersectionStore(testCtx, intersectionDest, otherName); err != nil || n != 1 {
		t.Fatalf("IntersectionStore = %d, %v; want 1, nil", n, err)
	}
	if score, err := client.GetScoredSortedSet(intersectionDest).Score(testCtx, "b"); err != nil || score != 5 {
		t.Fatalf("intersection score = %v, %v; want 5, nil", score, err)
	}
	if n, err := set.UnionStore(testCtx, unionDest, otherName); err != nil || n != 3 {
		t.Fatalf("UnionStore = %d, %v; want 3, nil", n, err)
	}
	if score, err := client.GetScoredSortedSet(unionDest).Score(testCtx, "b"); err != nil || score != 5 {
		t.Fatalf("union score = %v, %v; want 5, nil", score, err)
	}
	if n, err := set.DiffStore(testCtx, diffDest, otherName); err != nil || n != 1 {
		t.Fatalf("DiffStore = %d, %v; want 1, nil", n, err)
	}
}

func TestRBlockingQueue_MultiQueueAndMove(t *testing.T) {
	client := newTestClient(t)
	base := uniqueKey(t, "blocking-queue-ops")
	sourceName := "{" + base + "}:source"
	otherName := "{" + base + "}:other"
	destName := "{" + base + "}:dest"
	defer client.GetKeys().Delete(testCtx, sourceName, otherName, destName) //nolint:errcheck

	source := client.GetBlockingQueue(sourceName)
	other := client.GetBlockingQueue(otherName)
	dest := client.GetQueue(destName)

	if err := other.Offer(testCtx, "other-value"); err != nil {
		t.Fatal("Offer other:", err)
	}
	value, err := source.PollFromAny(testCtx, time.Second, otherName)
	if err != nil || value != "other-value" {
		t.Fatalf("PollFromAny = %v, %v; want other-value, nil", value, err)
	}

	if err := source.Offer(testCtx, "first", "last"); err != nil {
		t.Fatal("Offer source:", err)
	}
	value, err = source.PollLastAndOfferFirstTo(testCtx, destName, time.Second)
	if err != nil || value != "last" {
		t.Fatalf("PollLastAndOfferFirstTo = %v, %v; want last, nil", value, err)
	}
	value, err = dest.Poll(testCtx)
	if err != nil || value != "last" {
		t.Fatalf("destination Poll = %v, %v; want last, nil", value, err)
	}
}
