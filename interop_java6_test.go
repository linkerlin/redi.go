package redi_test

import (
	"encoding/base64"
	"testing"
	"time"
)

func TestJavaInterop_RIdGenerator(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-idgen")
	alloc := "{" + name + "}:allocation"
	t.Cleanup(func() { interopCleanup(t, name, alloc) })
	g := client.GetIdGenerator(name)

	if reply, err := javaSend("idgen_init " + name + " 0 1"); err != nil || reply["ok"] != true {
		t.Fatalf("java init = %v, %v", reply, err)
	}
	if reply, err := javaSend("idgen_next " + name); err != nil || !numEq(reply["id"], 0) {
		t.Fatalf("java first id = %v, %v; want 0", reply, err)
	}
	id, err := g.NextID(testCtx)
	if err != nil || id != 1 {
		t.Fatalf("Go id = %d, %v; want 1", id, err)
	}
	if reply, err := javaSend("idgen_next " + name); err != nil || !numEq(reply["id"], 2) {
		t.Fatalf("java second id = %v, %v; want 2", reply, err)
	}
	id, err = g.NextID(testCtx)
	if err != nil || id != 3 {
		t.Fatalf("Go second id = %d, %v; want 3", id, err)
	}
}

func TestJavaInterop_RFunction(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	f := client.GetFunction()
	lib := "redigojio" + sanitizeLib(uniqueKey(t, "fn"))

	body := "redis.register_function{function_name='redigo_jio_mul', callback=function(keys, args)\n" +
		"    return tonumber(args[1]) * 3\n" +
		"end, flags={'no-writes'}}"
	b64 := base64.StdEncoding.EncodeToString([]byte(body))

	if _, err := javaSend("func_load " + lib + " " + b64); err != nil {
		t.Skipf("FUNCTION unavailable via Java: %v", err)
	}
	t.Cleanup(func() { _, _ = javaSend("func_delete " + lib) })

	v, err := f.Call(testCtx, "redigo_jio_mul", nil, 7)
	if err != nil {
		t.Skipf("FCALL unavailable: %v", err)
	}
	if !numEq(v, 21) {
		t.Fatalf("Go Call after Java LOAD = %#v, want 21", v)
	}

	body2 := "redis.register_function{function_name='redigo_jio_mul', callback=function(keys, args)\n" +
		"    return tonumber(args[1]) * 4\n" +
		"end, flags={'no-writes'}}"
	if err := f.Load(testCtx, "#!lua name="+lib+"\n"+body2); err != nil {
		t.Fatal(err)
	}
	v, err = f.Call(testCtx, "redigo_jio_mul", nil, 5)
	if err != nil || !numEq(v, 20) {
		t.Fatalf("Go Call after Go reload = %#v, %v; want 20", v, err)
	}
}

func sanitizeLib(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			out = append(out, c)
		}
	}
	return string(out)
}

func TestJavaInterop_RShardedTopic(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-stopic")
	topic := client.GetShardedTopic(name)

	got := make(chan any, 1)
	id, err := topic.Subscribe(func(msg any) { got <- msg })
	if err != nil {
		t.Fatal(err)
	}
	defer topic.Unsubscribe(id)

	mustJava(t, "stopic_listen", name)
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
	if reply, err := javaSend("stopic_collect"); err != nil || reply["value"] != "from-go" {
		t.Fatalf("java collect = %v, %v", reply, err)
	}

	mustJava(t, "stopic_listen", name)
	time.Sleep(200 * time.Millisecond)
	got2 := make(chan any, 1)
	id2, err := topic.Subscribe(func(msg any) { got2 <- msg })
	if err != nil {
		t.Fatal(err)
	}
	defer topic.Unsubscribe(id2)
	time.Sleep(200 * time.Millisecond)
	if _, err := javaSend(`stopic_publish ` + name + ` "from-java"`); err != nil {
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
