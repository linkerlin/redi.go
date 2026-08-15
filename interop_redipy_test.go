package redi_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// interop_redipy_test.go drives the Python probe (interop/redipy_probe.py)
// to assert bidirectional wire compatibility between redi.go and redi.py.
// redi.py's formats were verified against real Java Redisson, so passing
// these tests transitively validates redi.go against Redisson.
//
// Skipped when python, the redipy package or Redis are unavailable.

var pythonBin = func() string {
	for _, name := range []string{"python", "python3", "py"} {
		if _, err := exec.LookPath(name); err == nil {
			return name
		}
	}
	return ""
}()

func probeScript(t *testing.T) string {
	t.Helper()
	probe, err := filepath.Abs(filepath.Join("interop", "redipy_probe.py"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(probe); err != nil {
		t.Skip("interop probe script not found")
	}
	return probe
}

// runProbe executes one probe command and decodes its JSON reply.
func runProbe(t *testing.T, args ...string) map[string]any {
	t.Helper()
	if pythonBin == "" {
		t.Skip("python not found")
	}
	script := probeScript(t)
	out, err := exec.Command(pythonBin, append([]string{script}, args...)...).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Skipf("probe failed (deps missing?): %v: %s", err, ee.Stderr)
		}
		t.Fatal(err)
	}
	var reply map[string]any
	if err := json.Unmarshal([]byte(out), &reply); err != nil {
		t.Fatalf("probe reply not JSON: %q (%v)", out, err)
	}
	return reply
}

// startLockHold runs the long-running lock_hold probe: it acquires, reports,
// and holds the lock until the returned release func closes stdin.
func startLockHold(t *testing.T, name, holderID string) (acquired bool, release func()) {
	t.Helper()
	if pythonBin == "" {
		t.Skip("python not found")
	}
	runProbe(t, "ping") // pre-flight: skip early when redipy/redis are missing
	script := probeScript(t)
	cmd := exec.Command(pythonBin, script, "lock_hold", name, holderID)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 4)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// First line: {"acquired": bool}.
	var line string
	select {
	case line = <-lines:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("lock_hold probe did not report")
	}
	var reply map[string]any
	if err := json.Unmarshal([]byte(line), &reply); err != nil {
		t.Fatalf("lock_hold reply not JSON: %q", line)
	}
	acq, _ := reply["acquired"].(bool)

	release = func() {
		_ = stdin.Close()
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
		}
	}
	return acq, release
}

// interopKeys lists every Redis key (main + companions) a structure uses, so
// tests can clean up after themselves.
func interopCleanup(t *testing.T, keys ...string) {
	t.Helper()
	rc := rawClient(t)
	_ = rc.Del(testCtx, keys...).Err()
}

// interopCleanupPattern deletes every key matching a glob (multimap
// collections, companion keys with dynamic ids).
func interopCleanupPattern(t *testing.T, pattern string) {
	t.Helper()
	rc := rawClient(t)
	var cursor uint64
	for {
		keys, next, err := rc.Scan(testCtx, cursor, pattern, 100).Result()
		if err != nil {
			return
		}
		if len(keys) > 0 {
			_ = rc.Del(testCtx, keys...).Err()
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

func TestInterop_RLock(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "io-lock")
	t.Cleanup(func() { interopCleanup(t, name) })

	// Python holds; Go observes.
	acquired, release := startLockHold(t, name, "pyproc:77")
	if !acquired {
		t.Fatal("python could not acquire a fresh lock")
	}
	got, err := client.GetLock(name).TryLock(testCtx, "go-client:1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Fatal("Go TryLock succeeded while python holds the lock")
	}
	release()

	// After release Go acquires; python observes and releases in-process.
	if err := client.GetLock(name).Lock(testCtx, "go-client:1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if reply := runProbe(t, "lock_acquire", name, "pyproc:88"); reply["acquired"] != false {
		t.Fatalf("python tryLock succeeded while Go holds: %v", reply)
	}
	if err := client.GetLock(name).Unlock(testCtx, "go-client:1"); err != nil {
		t.Fatal(err)
	}
	if reply := runProbe(t, "lock_acquire", name, "pyproc:88"); reply["acquired"] != true {
		t.Fatalf("python tryLock failed after Go released: %v", reply)
	}
}

func TestInterop_RMap(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "io-map")
	t.Cleanup(func() { interopCleanup(t, name) })
	m := client.GetMap(name)

	// Go writes, python reads (incl. java.lang.Long wrapper + struct value).
	if err := m.Put(testCtx, "hello", "world"); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(testCtx, "big", 5000000000); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(testCtx, "obj", map[string]any{"n": 1}); err != nil {
		t.Fatal(err)
	}
	reply := runProbe(t, "map_get", name, "hello")
	if reply["value"] != "world" {
		t.Fatalf("python read of Go string = %v", reply["value"])
	}
	reply = runProbe(t, "map_get", name, "big")
	if n, ok := reply["value"].(float64); !ok || int64(n) != 5000000000 {
		t.Fatalf("python read of Go long = %v", reply["value"])
	}
	reply = runProbe(t, "map_get", name, "obj")
	obj, ok := reply["value"].(map[string]any)
	if !ok || obj["n"] != float64(1) {
		t.Fatalf("python read of Go struct = %v", reply["value"])
	}

	// Python writes, Go reads.
	runProbe(t, "map_put", name, "pystr", `"from-python"`)
	runProbe(t, "map_put", name, "pylong", "6000000000")
	runProbe(t, "map_put", name, "pyobj", `{"k":[1,2]}`)

	v, err := m.Get(testCtx, "pystr")
	if err != nil || v != "from-python" {
		t.Fatalf("Go read of python string = %v, %v", v, err)
	}
	v, err = m.Get(testCtx, "pylong")
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := v.(json.Number); !ok || n.String() != "6000000000" {
		t.Fatalf("Go read of python long = %#v", v)
	}
	v, _ = m.Get(testCtx, "pyobj")
	mv, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("Go read of python object = %#v", v)
	}
	arr, ok := mv["k"].([]any)
	if !ok || len(arr) != 2 || arr[0].(json.Number).String() != "1" {
		t.Fatalf("Go read of python array = %#v", mv["k"])
	}
}

func TestInterop_RAtomicLong(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "io-along")
	t.Cleanup(func() { interopCleanup(t, name) })
	a := client.GetAtomicLong(name)

	if reply := runProbe(t, "along_add", name, "5"); reply["value"] != float64(5) {
		t.Fatalf("python add = %v", reply["value"])
	}
	v, err := a.Get(testCtx)
	if err != nil || v != 5 {
		t.Fatalf("Go read = %d, %v; want 5", v, err)
	}
	if _, err := a.AddAndGet(testCtx, 2); err != nil {
		t.Fatal(err)
	}
	if reply := runProbe(t, "along_get", name); reply["value"] != float64(7) {
		t.Fatalf("python read = %v; want 7", reply["value"])
	}
}

func TestInterop_RBloomFilter(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "io-bloom")
	t.Cleanup(func() { interopCleanup(t, name, "{"+name+"}:config") })
	f := client.GetBloomFilter(name)

	if reply := runProbe(t, "bloom_init", name, "1000", "0.03"); reply["ok"] != true {
		t.Fatalf("python init = %v", reply)
	}
	sz, err := f.Size(testCtx)
	if err != nil || sz != 7298 {
		t.Fatalf("Go size = %d, %v; want 7298 (same Java-truncated formula)", sz, err)
	}

	if reply := runProbe(t, "bloom_add", name, `"apple"`); reply["added"] != true {
		t.Fatalf("python add = %v", reply)
	}
	contains, err := f.Contains(testCtx, "apple")
	if err != nil || !contains {
		t.Fatalf("Go contains(python's apple) = %v, %v; want true (hash agreement)", contains, err)
	}

	if _, err := f.Add(testCtx, "banana"); err != nil {
		t.Fatal(err)
	}
	if reply := runProbe(t, "bloom_contains", name, `"banana"`); reply["contains"] != true {
		t.Fatalf("python contains(Go's banana) = %v", reply)
	}
}

func TestInterop_RRateLimiter(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "io-rate")
	t.Cleanup(func() { interopCleanup(t, name, "{"+name+"}:value", "{"+name+"}:permits") })

	if reply := runProbe(t, "rate_set", name, "2", "60000"); reply["ok"] != true {
		t.Fatalf("python rate set = %v", reply)
	}
	r := client.GetRateLimiter(name)
	// Both windows share the same permit pool: python takes 1, Go takes 1,
	// the third acquire (Go) must fail.
	if reply := runProbe(t, "rate_try", name, "1"); reply["acquired"] != true {
		t.Fatalf("python acquire = %v", reply)
	}
	ok, err := r.TryAcquire(testCtx, 1)
	if err != nil || !ok {
		t.Fatalf("Go acquire after python = %v, %v", ok, err)
	}
	ok, err = r.TryAcquire(testCtx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("Go acquire succeeded beyond python-configured rate=2")
	}
}

func TestInterop_RSemaphore(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "io-sem")
	t.Cleanup(func() { interopCleanup(t, name, name+":total") })
	s := client.GetSemaphore(name)

	if reply := runProbe(t, "sem_set", name, "1"); reply["ok"] != true {
		t.Fatalf("python sem set = %v", reply)
	}
	if reply := runProbe(t, "sem_try", name, "1"); reply["acquired"] != true {
		t.Fatalf("python sem acquire = %v", reply)
	}
	ok, err := s.TryAcquire(testCtx, 1)
	if err != nil || ok {
		t.Fatalf("Go TryAcquire with 0 permits = %v, %v; want false", ok, err)
	}
	runProbe(t, "sem_release", name, "1")
	ok, err = s.TryAcquire(testCtx, 1)
	if err != nil || !ok {
		t.Fatalf("Go TryAcquire after python release = %v, %v", ok, err)
	}
}

func TestInterop_RCountDownLatch(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "io-cdl")
	t.Cleanup(func() { interopCleanup(t, name) })

	if reply := runProbe(t, "latch_set", name, "1"); reply["ok"] != true {
		t.Fatalf("python latch set = %v", reply)
	}
	l := client.GetCountDownLatch(name)

	opened := make(chan bool, 1)
	go func() {
		open, _ := l.Await(context.Background(), 5*time.Second)
		opened <- open
	}()

	// Still closed before the countdown.
	select {
	case <-opened:
		t.Fatal("Await opened before countdown")
	case <-time.After(150 * time.Millisecond):
	}

	runProbe(t, "latch_count_down", name)
	select {
	case open := <-opened:
		if !open {
			t.Fatal("Await returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Go Await not woken by python countDown")
	}
}

func TestInterop_RDelayedQueue(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "io-dq")
	t.Cleanup(func() { interopCleanup(t, name, "redisson_delay_queue:{"+name+"}") })
	q := client.GetDelayedQueue(name)

	if err := q.Offer(testCtx, "from-go", time.Hour); err != nil {
		t.Fatal(err)
	}
	if reply := runProbe(t, "dq_delayed_size", name); reply["size"] != float64(1) {
		t.Fatalf("python sees delayed size = %v; want 1", reply["size"])
	}

	runProbe(t, "dq_offer", name, `"from-python"`, "3600000")
	n, err := q.DelayedSize(testCtx)
	if err != nil || n != 2 {
		t.Fatalf("Go DelayedSize after python offer = %d, %v; want 2", n, err)
	}
}

// TestInterop_RSetMultimap verifies the deterministic internal collection
// id (Hash.hash128toBase64) agrees byte-for-byte, so both languages land on
// the same {name}:{id} sub-collection.
func TestInterop_RSetMultimap(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "io-setmm")
	mm := client.GetSetMultimap(name)
	t.Cleanup(func() { interopCleanupPattern(t, "{"+name+"}:*") })

	// Go writes; python reads the same associations and the raw id.
	if _, err := mm.Put(testCtx, "lang", "go"); err != nil {
		t.Fatal(err)
	}
	reply := runProbe(t, "setmm_get", name, `"lang"`)
	vals, _ := reply["values"].([]any)
	if len(vals) != 1 || vals[0] != "go" {
		t.Fatalf("python read of Go multimap = %v", vals)
	}

	// python writes; Go reads.
	runProbe(t, "setmm_put", name, `"lang"`, `"python"`)
	got, err := mm.Get(testCtx, "lang")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("Go Get = %v; want both values", got)
	}
	found := map[string]bool{}
	for _, v := range got {
		found[v.(string)] = true
	}
	if !found["go"] || !found["python"] {
		t.Fatalf("Go Get values = %v", got)
	}

	// Deterministic id agreement: same {name}:{id} collection key.
	rc := rawClient(t)
	ids, err := rc.HVals(testCtx, name).Result()
	if err != nil || len(ids) != 1 {
		t.Fatalf("multimap hash ids = %v, %v", ids, err)
	}
	pyID := runProbe(t, "setmm_internal_id", name, `"lang"`)
	if ids[0] != pyID["id"] {
		t.Fatalf("internal id mismatch: Go side=%q python=%q", ids[0], pyID["id"])
	}
	// And the collection lives at {name}:{id} with both members.
	typ, _ := rc.Type(testCtx, "{"+name+"}:"+ids[0]).Result()
	if typ != "set" {
		t.Fatalf("collection type = %q, want set", typ)
	}
}

func TestInterop_RListMultimap(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "io-listmm")
	mm := client.GetListMultimap(name)
	t.Cleanup(func() { interopCleanupPattern(t, "{"+name+"}:*") })

	if _, err := mm.Put(testCtx, "k", "go-1"); err != nil {
		t.Fatal(err)
	}
	runProbe(t, "listmm_put", name, `"k"`, `"python-1"`)
	runProbe(t, "listmm_put", name, `"k"`, `"python-2"`)

	reply := runProbe(t, "listmm_get", name, `"k"`)
	vals, _ := reply["values"].([]any)
	if len(vals) != 3 {
		t.Fatalf("python read = %v; want 3 ordered values", vals)
	}

	got, err := mm.Get(testCtx, "k")
	if err != nil || len(got) != 3 {
		t.Fatalf("Go read = %v, %v", got, err)
	}
}
