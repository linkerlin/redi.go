package redi_test

import (
	"encoding/base64"
	"math"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

func TestJavaInterop_RTopic(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-topic")
	topic := client.GetTopic(name)

	got := make(chan any, 1)
	id, err := topic.Subscribe(func(msg any) { got <- msg })
	if err != nil {
		t.Fatal(err)
	}
	defer topic.Unsubscribe(id)

	mustJava(t, "topic_listen", name)
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
		t.Fatal("Go listener not notified")
	}
	if reply, err := javaSend("topic_collect"); err != nil || reply["value"] != "from-go" {
		t.Fatalf("java collect = %v, %v", reply, err)
	}

	mustJava(t, "topic_listen", name)
	time.Sleep(200 * time.Millisecond)
	got2 := make(chan any, 1)
	id2, err := topic.Subscribe(func(msg any) { got2 <- msg })
	if err != nil {
		t.Fatal(err)
	}
	defer topic.Unsubscribe(id2)
	time.Sleep(200 * time.Millisecond)
	if _, err := javaSend(`topic_publish ` + name + ` "from-java"`); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got2:
		if v != "from-java" {
			t.Fatalf("Go got java msg %v", v)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Go did not receive java publish")
	}
}

func TestJavaInterop_RScript(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	s := client.GetScript()
	lua := "return 42"
	b64 := base64.StdEncoding.EncodeToString([]byte(lua))

	reply, err := javaSend("script_eval " + b64)
	if err != nil {
		t.Fatal(err)
	}
	if !numEq(reply["value"], 42) {
		t.Fatalf("java eval = %v; want 42", reply["value"])
	}

	v, err := s.Eval(testCtx, lua, redi.ScriptReturnInteger, nil)
	if err != nil || !numEq(v, 42) {
		t.Fatalf("Go Eval = %#v, %v; want 42", v, err)
	}

	sha, err := s.ScriptLoad(testCtx, lua)
	if err != nil || sha == "" {
		t.Fatalf("Go ScriptLoad = %q, %v", sha, err)
	}
	exists, err := s.ScriptExists(testCtx, sha)
	if err != nil || len(exists) != 1 || !exists[0] {
		t.Fatalf("Go ScriptExists = %v, %v", exists, err)
	}
	loadReply, err := javaSend("script_load " + b64)
	if err != nil {
		t.Fatal(err)
	}
	if loadReply["sha"] != sha {
		t.Fatalf("java sha = %v; want %s", loadReply["sha"], sha)
	}
}

func TestJavaInterop_RBuckets(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	a := uniqueKey(t, "jio-bkt-a")
	b := uniqueKey(t, "jio-bkt-b")
	t.Cleanup(func() { interopCleanup(t, a, b) })
	buckets := client.GetBuckets()

	if _, err := javaSend("buckets_set " + a + ` "ja" ` + b + ` "jb"`); err != nil {
		t.Fatal(err)
	}
	got, err := buckets.Get(testCtx, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if got[a] != "ja" || got[b] != "jb" {
		t.Fatalf("Go Get after Java = %v", got)
	}

	c := uniqueKey(t, "jio-bkt-c")
	d := uniqueKey(t, "jio-bkt-d")
	t.Cleanup(func() { interopCleanup(t, c, d) })
	if err := buckets.Set(testCtx, map[string]any{c: "gc", d: "gd"}, 0); err != nil {
		t.Fatal(err)
	}
	reply, err := javaSend("buckets_get " + c + " " + d)
	if err != nil {
		t.Fatal(err)
	}
	vals, _ := reply["values"].(map[string]any)
	if vals[c] != "gc" || vals[d] != "gd" {
		t.Fatalf("java get = %v", reply)
	}
}

func TestJavaInterop_RKeys(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-keys")
	t.Cleanup(func() { interopCleanup(t, name) })
	if err := client.GetBucket(name).Set(testCtx, "v"); err != nil {
		t.Fatal(err)
	}

	reply, err := javaSend("keys_type " + name)
	if err != nil {
		t.Fatal(err)
	}
	typ, _ := reply["type"].(string)
	if typ == "" {
		t.Fatalf("java type empty: %v", reply)
	}
	keys := client.GetKeys()
	goTyp, err := keys.Type(testCtx, name)
	if err != nil || goTyp == "" {
		t.Fatalf("Go Type = %q, %v", goTyp, err)
	}

	if reply, err := javaSend("keys_count_exists " + name); err != nil || !numEq(reply["count"], 1) {
		t.Fatalf("java countExists = %v, %v", reply, err)
	}
	if n, err := keys.CountExists(testCtx, name); err != nil || n != 1 {
		t.Fatalf("Go CountExists = %d, %v", n, err)
	}

	if reply, err := javaSend("keys_delete " + name); err != nil || !numEq(reply["deleted"], 1) {
		t.Fatalf("java delete = %v, %v", reply, err)
	}
	if n, err := keys.CountExists(testCtx, name); err != nil || n != 0 {
		t.Fatalf("after java delete CountExists = %d, %v", n, err)
	}
}

func TestJavaInterop_RSet(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-set")
	t.Cleanup(func() { interopCleanup(t, name) })
	set := client.GetSet(name)

	if _, err := javaSend("set_add " + name + ` "jv"`); err != nil {
		t.Fatal(err)
	}
	ok, err := set.Contains(testCtx, "jv")
	if err != nil || !ok {
		t.Fatalf("Go Contains jv = %v, %v", ok, err)
	}
	if err := set.Add(testCtx, "gv"); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("set_contains " + name + ` "gv"`); err != nil || reply["contains"] != true {
		t.Fatalf("java contains gv = %v, %v", reply, err)
	}
	if reply, err := javaSend("set_size " + name); err != nil || !numEq(reply["size"], 2) {
		t.Fatalf("java size = %v, %v", reply, err)
	}
	if n, err := set.Size(testCtx); err != nil || n != 2 {
		t.Fatalf("Go Size = %d, %v", n, err)
	}
}

func TestJavaInterop_RQueue(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-queue")
	t.Cleanup(func() { interopCleanup(t, name) })
	q := client.GetQueue(name)

	if _, err := javaSend("queue_offer " + name + ` "ja"`); err != nil {
		t.Fatal(err)
	}
	if err := q.Offer(testCtx, "gb"); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("queue_size " + name); err != nil || !numEq(reply["size"], 2) {
		t.Fatalf("java size = %v, %v", reply, err)
	}
	v, err := q.Poll(testCtx)
	if err != nil || v != "ja" {
		t.Fatalf("Go Poll = %v, %v; want ja", v, err)
	}
	if reply, err := javaSend("queue_poll " + name); err != nil || reply["value"] != "gb" {
		t.Fatalf("java poll = %v, %v; want gb", reply, err)
	}
}

func TestJavaInterop_RSetMultimap(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-smm")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	m := client.GetSetMultimap(name)

	if _, err := javaSend("smm_put " + name + ` "k" "jv"`); err != nil {
		t.Fatal(err)
	}
	vals, err := m.Get(testCtx, "k")
	if err != nil || len(vals) != 1 || vals[0] != "jv" {
		t.Fatalf("Go Get = %v, %v", vals, err)
	}
	if _, err := m.Put(testCtx, "k", "gv"); err != nil {
		t.Fatal(err)
	}
	reply, err := javaSend("smm_getall " + name + ` "k"`)
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := reply["values"].([]any)
	if len(arr) < 2 {
		t.Fatalf("java getall = %v; want both", reply)
	}
}

func TestJavaInterop_RListMultimap(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-lmm")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	m := client.GetListMultimap(name)

	if _, err := javaSend("lmm_put " + name + ` "k" "a"`); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Put(testCtx, "k", "b"); err != nil {
		t.Fatal(err)
	}
	reply, err := javaSend("lmm_getall " + name + ` "k"`)
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := reply["values"].([]any)
	if len(arr) != 2 || arr[0] != "a" || arr[1] != "b" {
		t.Fatalf("java getall order = %v; want [a b]", arr)
	}
}

func TestJavaInterop_RDoubleAdder(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-dadder")
	t.Cleanup(func() { interopCleanupPattern(t, "{"+name+"}:*") })

	mustJava(t, "dadder_create", name)
	time.Sleep(500 * time.Millisecond)

	a := client.GetDoubleAdder(name)
	defer a.Destroy()

	a.Add(10.5)
	mustJava(t, "dadder_add", name, "2.25")

	if !eventual(t, 15*time.Second, func() bool {
		sum, err := a.Sum(testCtx)
		return err == nil && math.Abs(sum-12.75) < 1e-9
	}) {
		sum, _ := a.Sum(testCtx)
		t.Fatalf("Go Sum = %v, want 12.75", sum)
	}

	mustJava(t, "dadder_add", name, "0.25")
	reply, err := javaSend("dadder_sum " + name)
	if err != nil {
		t.Fatal(err)
	}
	jv, _ := reply["value"].(float64)
	if math.Abs(jv-13.0) > 1e-9 {
		t.Fatalf("java sum = %v; want 13", reply["value"])
	}
}
