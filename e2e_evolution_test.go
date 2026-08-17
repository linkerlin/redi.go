package redi_test

import (
	"testing"
	"time"
)

func TestE2E_KeysObjectBatch_Coverage(t *testing.T) {
	client := newTestClient(t)
	keys := client.GetKeys()

	a := uniqueKey(t, "keys-a")
	b := uniqueKey(t, "keys-b")
	t.Cleanup(func() { interopCleanup(t, a, b) })

	bucket := client.GetBucket(a)
	if err := bucket.Set(testCtx, "v"); err != nil {
		t.Fatal(err)
	}
	if n, err := keys.Count(testCtx); err != nil || n < 1 {
		t.Fatalf("Count = %d, %v", n, err)
	}
	if typ, err := keys.Type(testCtx, a); err != nil || typ == "" {
		t.Fatalf("Type = %q, %v", typ, err)
	}
	if _, err := keys.RandomKey(testCtx); err != nil {
		t.Fatal(err)
	}
	if ok, err := keys.Copy(testCtx, a, b, true); err != nil {
		t.Log("Copy:", err)
	} else if ok {
		t.Cleanup(func() { interopCleanup(t, b) })
	}

	obj := client.GetBucket(uniqueKey(t, "obj-tools"))
	t.Cleanup(func() { interopCleanup(t, obj.Name()) })
	if err := obj.Set(testCtx, "x"); err != nil {
		t.Fatal(err)
	}
	if ok, err := obj.Touch(testCtx); err != nil || !ok {
		t.Fatalf("Touch = %v, %v", ok, err)
	}
	if ok, err := obj.ExpireAt(testCtx, time.Now().Add(time.Minute)); err != nil || !ok {
		t.Fatalf("ExpireAt = %v, %v", ok, err)
	}
	if ok, err := obj.ClearExpire(testCtx); err != nil || !ok {
		t.Fatalf("ClearExpire = %v, %v", ok, err)
	}
	if err := obj.Unlink(testCtx); err != nil {
		t.Fatal(err)
	}

	batch := client.NewBatch()
	_ = batch.GetList(uniqueKey(t, "batch-l")).Add(testCtx, "1")
	_ = batch.GetSet(uniqueKey(t, "batch-s")).Add(testCtx, "1")
	_ = batch.GetDeque(uniqueKey(t, "batch-d")).AddFirst(testCtx, "1")
	_, _ = batch.GetAtomicDouble(uniqueKey(t, "batch-ad")).AddAndGet(testCtx, 1.0)
	_, _ = batch.GetAtomicLong(uniqueKey(t, "batch-al")).AddAndGet(testCtx, 1)
	if err := batch.Execute(testCtx); err != nil {
		t.Fatal(err)
	}
	batch2 := client.NewBatch()
	_ = batch2.GetMap(uniqueKey(t, "batch-m")).Put(testCtx, "k", "v")
	batch2.Discard()
}

func TestE2E_QueueStreamTopic_Gaps(t *testing.T) {
	client := newTestClient(t)

	q := client.GetQueue(uniqueKey(t, "q-gaps"))
	t.Cleanup(func() { interopCleanup(t, q.Name()) })
	if err := q.Offer(testCtx, "a", "b"); err != nil {
		t.Fatal(err)
	}
	if all, err := q.ReadAll(testCtx); err != nil || len(all) < 2 {
		t.Fatalf("ReadAll = %v, %v", all, err)
	}

	dq := client.GetDeque(uniqueKey(t, "dq-gaps"))
	t.Cleanup(func() { interopCleanup(t, dq.Name()) })
	if err := dq.AddLast(testCtx, "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := dq.RemoveFirst(testCtx); err != nil {
		t.Fatal(err)
	}

	sss := client.GetScoredSortedSet(uniqueKey(t, "sss-gaps"))
	t.Cleanup(func() { interopCleanup(t, sss.Name()) })
	_, _ = sss.Add(testCtx, "m1", 1)
	_, _ = sss.Add(testCtx, "m2", 2)
	if err := sss.Remove(testCtx, "m1"); err != nil {
		t.Fatal(err)
	}

	st := client.GetStream(uniqueKey(t, "st-gaps"))
	t.Cleanup(func() { interopCleanup(t, st.Name()) })
	if _, err := st.Add(testCtx, map[string]any{"f": "v"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Trim(testCtx, 10, true); err != nil {
		t.Fatal(err)
	}

	topic := client.GetTopic(uniqueKey(t, "topic-gaps"))
	if names := topic.GetChannelNames(); len(names) != 1 {
		t.Fatalf("GetChannelNames = %v", names)
	}
	if _, err := topic.CountSubscribers(testCtx); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_ClientOpsAndMultiLockAlias(t *testing.T) {
	client := newTestClient(t)
	if id := client.ID(); id == "" {
		t.Fatal("ID empty")
	}
	if len(client.Config().Addrs) == 0 {
		t.Fatal("Config.Addrs empty")
	}
	nodes := client.GetRedisNodes()
	if nodes == nil {
		t.Fatal("GetRedisNodes nil")
	}
	if err := nodes.Ping(testCtx); err != nil {
		t.Fatal(err)
	}
	if nodes.String() == "" {
		t.Fatal("RedisNodes.String empty")
	}

	a := uniqueKey(t, "ml-a")
	b := uniqueKey(t, "ml-b")
	t.Cleanup(func() { interopCleanup(t, a, b) })
	ml := client.GetMultiLock(client.GetLock(a), client.GetLock(b))
	holder := client.HolderID("1")
	ok, err := ml.TryLock(testCtx, holder, time.Second)
	if err != nil || !ok {
		t.Fatalf("GetMultiLock TryLock = %v, %v", ok, err)
	}
	if err := ml.Unlock(testCtx, holder); err != nil {
		t.Fatal(err)
	}
}
