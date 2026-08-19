package redi

import "testing"

func TestHashTag_CompanionKeysShareSlot(t *testing.T) {
	names := []string{"orders", "user:42", "a/b", "redi:test:x"}
	for _, name := range names {
		base := redisSlot(name)
		companions := []string{
			prefixName("redisson_lock__channel", name),
			prefixName("redisson_rb", name),
			prefixName("redisson__timeout__set", name),
			prefixName("redisson_delay_queue_timeout", name),
			suffixName(name, "value"),
			suffixName(name, "permits"),
			suffixName(name, "allocation"),
			suffixName(name, "timeout"),
			suffixName(name, "config"),
		}
		for _, k := range companions {
			if got := redisSlot(k); got != base {
				t.Errorf("name=%q companion=%q slot %d != %d", name, k, got, base)
			}
		}
	}
}

func TestHashTag_KnownRedisVectors(t *testing.T) {
	cases := map[string]int{
		"somekey":       11058,
		"foo{hash_tag}": 2515,
		"bar{hash_tag}": 2515,
	}
	for k, want := range cases {
		if got := redisSlot(k); got != want {
			t.Errorf("redisSlot(%q) = %d, want %d", k, got, want)
		}
	}
	if redisSlot("{user:100}.name") != redisSlot("user:100") {
		t.Fatal("hash-tag extraction failed for {user:100}.name")
	}
}
