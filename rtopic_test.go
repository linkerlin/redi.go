package redi_test

import (
	"sync"
	"testing"
	"time"
)

func TestRTopic_PubSub(t *testing.T) {
	client := newTestClient(t)
	topic := client.GetTopic(uniqueKey(t, "topic"))

	var mu sync.Mutex
	got := []any{}
	id, err := topic.Subscribe(func(msg any) {
		mu.Lock()
		got = append(got, msg)
		mu.Unlock()
	})
	if err != nil {
		t.Fatal("Subscribe:", err)
	}
	defer topic.Unsubscribe(id)

	if _, err := topic.Publish(testCtx, "hello"); err != nil {
		t.Fatal("Publish:", err)
	}

	if !eventual(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 1 && got[0] == "hello"
	}) {
		t.Fatalf("listener did not receive message: %v", got)
	}
}

func TestRTopic_StructValue(t *testing.T) {
	client := newTestClient(t)
	topic := client.GetTopic(uniqueKey(t, "topic2"))

	type payload struct {
		N int    `json:"n"`
		S string `json:"s"`
	}
	var mu sync.Mutex
	var got payload
	id, err := topic.Subscribe(func(msg any) {
		mu.Lock()
		defer mu.Unlock()
		if m, ok := msg.(map[string]any); ok {
			got.S, _ = m["s"].(string)
		}
	})
	if err != nil {
		t.Fatal("Subscribe:", err)
	}
	defer topic.Unsubscribe(id)

	if _, err := topic.Publish(testCtx, payload{N: 1, S: "x"}); err != nil {
		t.Fatal("Publish:", err)
	}
	if !eventual(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got.S == "x"
	}) {
		t.Fatal("struct message not received")
	}
}

func TestRPatternTopic(t *testing.T) {
	client := newTestClient(t)
	pt := client.GetPatternTopic(uniqueKey(t, "pat") + ":*")

	var mu sync.Mutex
	channels := []string{}
	id, err := pt.Subscribe(func(channel string, msg any) {
		mu.Lock()
		channels = append(channels, channel)
		mu.Unlock()
	})
	if err != nil {
		t.Fatal("Subscribe:", err)
	}
	defer pt.Unsubscribe(id)

	suffix := uniqueKey(t, "pat") + ":news"
	if _, err := client.GetTopic(suffix).Publish(testCtx, "event"); err != nil {
		t.Fatal("Publish:", err)
	}

	if !eventual(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(channels) == 1 && channels[0] == suffix
	}) {
		t.Fatalf("pattern listener did not fire: %v", channels)
	}
}
