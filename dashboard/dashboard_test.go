package dashboard

import (
	"testing"
)

func TestParseInfo(t *testing.T) {
	raw := "# Server\r\nredis_version:8.0.0\r\nuptime_in_seconds:42\r\n\r\n# Clients\r\nconnected_clients:3\r\n"
	info := parseInfo(raw)
	if info["redis_version"] != "8.0.0" || info["uptime_in_seconds"] != "42" || info["connected_clients"] != "3" {
		t.Fatalf("parseInfo = %v", info)
	}
	if _, ok := info["# Server"]; ok {
		t.Fatal("section header leaked into info map")
	}
}

func TestBuildRowsPriority(t *testing.T) {
	rows := buildRows(map[string]string{
		"zzz_last":                  "1",
		"redis_version":             "8.0.0",
		"instantaneous_ops_per_sec": "99",
	})
	if rows[0][0] != "redis_version" {
		t.Fatalf("first row = %v, want redis_version (priority order)", rows[0][0])
	}
	if rows[1][0] != "instantaneous_ops_per_sec" {
		t.Fatalf("second row = %v", rows[1][0])
	}
}

func TestBuildLockRows(t *testing.T) {
	rows := buildLockRows([]lockSample{
		{name: "mylock", holders: "uuid-1:7 ×1, uuid-2 ×3", ttlMs: 4200},
		{name: "stuck", holders: "uuid-9:1", ttlMs: -1},
	})
	if len(rows) != 2 {
		t.Fatalf("rows = %d", len(rows))
	}
	if rows[0][1] != "uuid-1:7 ×1, uuid-2 ×3 • TTL 4200ms" {
		t.Fatalf("lock row = %q", rows[0][1])
	}
	if rows[1][1] != "uuid-9:1 • TTL no expiry" {
		t.Fatalf("no-expiry row = %q", rows[1][1])
	}

	if empty := buildLockRows(nil); len(empty) != 1 {
		t.Fatalf("empty placeholder rows = %d", len(empty))
	}
}

func TestBuildLimiterRows(t *testing.T) {
	rows := buildLimiterRows([]limiterSample{
		{name: "api", rate: "100", interval: "60000", value: "37"},
	})
	if len(rows) != 1 || rows[0][1] != "rate 100 / 60000ms • available 37" {
		t.Fatalf("limiter row = %v", rows)
	}
	if rows := buildLimiterRows(nil); len(rows) != 1 {
		t.Fatalf("empty placeholder rows = %d", len(rows))
	}
}

func TestKindFromChannelLikeScan(t *testing.T) {
	// fetchLocks' lock-shaped-hash filter: a hash whose only field is
	// "mode" yields no holders and is skipped. That logic lives inline;
	// here we lock the holders formatting rules it depends on.
	fields := map[string]string{"mode": "write", "uuid-1:1": "3"}
	holders := formatHolders(fields)
	if holders != "uuid-1:1×3" {
		t.Fatalf("holders = %q (mode must be skipped, ×N only for N>1)", holders)
	}
}
