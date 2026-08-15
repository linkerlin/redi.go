package redi_test

import (
	"testing"
	"time"
)

func TestRReadWriteLock_ReadShared(t *testing.T) {
	client := newTestClient(t)
	rw := client.GetReadWriteLock(uniqueKey(t, "rw"))
	r1, r2 := rw.ReadLock(), rw.ReadLock()

	if err := r1.Lock(testCtx, "reader-1", 30*time.Second); err != nil {
		t.Fatal("read Lock 1:", err)
	}
	ok, err := r2.TryLock(testCtx, "reader-2", 30*time.Second)
	if err != nil {
		t.Fatal("read TryLock 2:", err)
	}
	if !ok {
		t.Fatal("second reader could not share the read lock")
	}
	_ = r1.Unlock(testCtx, "reader-1")
	_ = r2.Unlock(testCtx, "reader-2")
}

func TestRReadWriteLock_WriteExcludesRead(t *testing.T) {
	client := newTestClient(t)
	rw := client.GetReadWriteLock(uniqueKey(t, "rw2"))
	w, r := rw.WriteLock(), rw.ReadLock()

	if err := w.Lock(testCtx, "writer", 30*time.Second); err != nil {
		t.Fatal("write Lock:", err)
	}

	ok, err := r.TryLock(testCtx, "reader", 30*time.Second)
	if err != nil {
		t.Fatal("read TryLock:", err)
	}
	if ok {
		t.Fatal("read lock acquired while write lock held")
	}

	// Same client downgrades write→read (verified Redisson semantics).
	ok, err = r.TryLock(testCtx, "writer", 30*time.Second)
	if err != nil {
		t.Fatal("downgrade TryLock:", err)
	}
	if !ok {
		t.Fatal("write holder could not downgrade to read")
	}

	// Another reader is still blocked while the downgrade holds.
	ok, err = r.TryLock(testCtx, "other-reader", 30*time.Second)
	if err != nil {
		t.Fatal("other reader TryLock:", err)
	}
	if ok {
		t.Fatal("other reader acquired while (downgraded) writer holds read")
	}

	_ = r.Unlock(testCtx, "writer")
	_ = w.Unlock(testCtx, "writer")
}
