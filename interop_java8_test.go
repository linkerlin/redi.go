package redi_test

import (
	"testing"
	"time"
)

func TestJavaInterop_RDeque(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-deque")
	t.Cleanup(func() { interopCleanup(t, name) })
	d := client.GetDeque(name)

	if _, err := javaSend("deque_add_first " + name + ` "ja"`); err != nil {
		t.Fatal(err)
	}
	if err := d.AddLast(testCtx, "gb"); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("deque_size " + name); err != nil || !numEq(reply["size"], 2) {
		t.Fatalf("java size = %v, %v", reply, err)
	}
	v, err := d.RemoveFirst(testCtx)
	if err != nil || v != "ja" {
		t.Fatalf("Go RemoveFirst = %v, %v; want ja", v, err)
	}
	if reply, err := javaSend("deque_remove_last " + name); err != nil || reply["value"] != "gb" {
		t.Fatalf("java removeLast = %v, %v; want gb", reply, err)
	}
}

func TestJavaInterop_RBlockingDeque(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-bdq")
	t.Cleanup(func() { interopCleanup(t, name) })
	d := client.GetBlockingDeque(name)

	if err := d.AddLast(testCtx, "from-go"); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("bdq_take_first " + name); err != nil || reply["value"] != "from-go" {
		t.Fatalf("java takeFirst = %v, %v; want from-go", reply, err)
	}
	if _, err := javaSend("bdq_put_last " + name + ` "from-java"`); err != nil {
		t.Fatal(err)
	}
	v, err := d.PollFirstWithTimeout(testCtx, 2*time.Second)
	if err != nil || v != "from-java" {
		t.Fatalf("Go PollFirst = %v, %v; want from-java", v, err)
	}
}

func TestJavaInterop_RPatternTopic(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	prefix := uniqueKey(t, "jio-pt")
	pattern := prefix + ":*"
	channel := prefix + ":ch"
	pt := client.GetPatternTopic(pattern)
	topic := client.GetTopic(channel)

	got := make(chan any, 1)
	id, err := pt.Subscribe(func(_ string, msg any) { got <- msg })
	if err != nil {
		t.Fatal(err)
	}
	defer pt.Unsubscribe(id)

	mustJava(t, "ptopic_listen", pattern)
	time.Sleep(300 * time.Millisecond)

	if _, err := topic.Publish(testCtx, "from-go"); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got:
		if v != "from-go" {
			t.Fatalf("Go got %v", v)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Go pattern listener not notified")
	}
	if reply, err := javaSend("ptopic_collect"); err != nil || reply["value"] != "from-go" {
		t.Fatalf("java collect = %v, %v", reply, err)
	}

	mustJava(t, "ptopic_listen", pattern)
	time.Sleep(200 * time.Millisecond)
	got2 := make(chan any, 1)
	id2, err := pt.Subscribe(func(_ string, msg any) { got2 <- msg })
	if err != nil {
		t.Fatal(err)
	}
	defer pt.Unsubscribe(id2)
	time.Sleep(200 * time.Millisecond)
	if _, err := javaSend(`topic_publish ` + channel + ` "from-java"`); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got2:
		if v != "from-java" {
			t.Fatalf("Go got java msg %v", v)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Go did not receive java pattern publish")
	}
}

func TestJavaInterop_RBlockingQueue(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-bq")
	t.Cleanup(func() { interopCleanup(t, name) })
	q := client.GetBlockingQueue(name)

	if _, err := javaSend("bq_offer " + name + ` "ja"`); err != nil {
		t.Fatal(err)
	}
	if err := q.Offer(testCtx, "gb"); err != nil {
		t.Fatal(err)
	}
	v, err := q.Poll(testCtx)
	if err != nil || v != "ja" {
		t.Fatalf("Go Poll = %v, %v; want ja", v, err)
	}
	if reply, err := javaSend("bq_poll " + name); err != nil || reply["value"] != "gb" {
		t.Fatalf("java poll = %v, %v; want gb", reply, err)
	}
}

func TestJavaInterop_RBatch(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-batch")
	t.Cleanup(func() { interopCleanup(t, name) })

	mustJava(t, "batch_map_put", name, `"jk"`, `"jv"`)
	if v, err := client.GetMap(name).Get(testCtx, "jk"); err != nil || v != "jv" {
		t.Fatalf("Go read after java batch = %v, %v", v, err)
	}

	b := client.NewBatch()
	if err := b.GetMap(name).Put(testCtx, "gk", "gv"); err != nil {
		t.Fatal(err)
	}
	if err := b.Execute(testCtx); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("map_get " + name + ` "gk"`); err != nil || reply["value"] != "gv" {
		t.Fatalf("java read after Go batch = %v, %v", reply, err)
	}
}

func TestJavaInterop_RMaps(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-maps")
	t.Cleanup(func() { interopCleanup(t, name) })

	// Redisson 4.6.1 has no getMaps(); verify Go Set HASH is readable as RMap.
	if err := client.GetMaps().Set(testCtx, map[string]map[string]any{
		name: {"k": "from-go", "keep": "yes"},
	}); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("map_get " + name + ` "k"`); err != nil || reply["value"] != "from-go" {
		t.Fatalf("java read after Go maps.Set = %v, %v", reply, err)
	}
	if err := client.GetMaps().Set(testCtx, map[string]map[string]any{
		name: {"k": "replaced"},
	}); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("map_get " + name + ` "k"`); err != nil || reply["value"] != "replaced" {
		t.Fatalf("java read after replace = %v, %v", reply, err)
	}
	if reply, err := javaSend("map_get " + name + ` "keep"`); err != nil || reply["value"] != nil {
		t.Fatalf("stale field after maps.Set = %v, %v; want nil", reply, err)
	}
}
