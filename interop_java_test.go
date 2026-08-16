package redi_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// interop_java_test.go drives the Java probe (interop/java-probe) running
// real Redisson 4.6.1 with JsonJacksonCodec — the configuration redi.go's
// wire format targets. This is the direct (non-transitive) Go ↔ Java
// interop validation.
//
// Skipped when java/maven, the built probe or Redis are unavailable.

var (
	javaOnce     sync.Once
	javaCmd      *exec.Cmd
	javaIn       writeFlusher
	javaLines    chan string
	javaStartErr error
	javaMu       sync.Mutex // serialises send/reply pairs
)

type writeFlusher interface {
	Write([]byte) (int, error)
	Close() error
}

// javaProbe ensures the JVM REPL is built and running.
func javaProbe(t *testing.T) {
	t.Helper()
	javaOnce.Do(startJavaProbe)
	if javaStartErr != nil {
		t.Skipf("java probe unavailable: %v", javaStartErr)
	}
}

func startJavaProbe() {
	javaBin, err := exec.LookPath("java")
	if err != nil {
		javaStartErr = err
		return
	}
	mvnBin, err := exec.LookPath("mvn")
	if err != nil {
		javaStartErr = err
		return
	}
	probeDir, err := filepath.Abs(filepath.Join("interop", "java-probe"))
	if err != nil {
		javaStartErr = err
		return
	}
	if _, err := os.Stat(filepath.Join(probeDir, "pom.xml")); err != nil {
		javaStartErr = err
		return
	}

	// Compile + resolve classpath (cached by maven after the first run).
	compile := exec.Command(mvnBin, "-q", "-f", filepath.Join(probeDir, "pom.xml"), "compile",
		"dependency:build-classpath", "-Dmdep.outputFile=target/cp.txt")
	compile.Dir = probeDir
	compile.Env = javaEnv()
	if out, err := compile.CombinedOutput(); err != nil {
		javaStartErr = fmt.Errorf("mvn compile failed: %v: %s", err, string(out))
		return
	}
	cpBytes, err := os.ReadFile(filepath.Join(probeDir, "target", "cp.txt"))
	if err != nil {
		javaStartErr = err
		return
	}
	classpath := filepath.Join(probeDir, "target", "classes") +
		string(os.PathListSeparator) + strings.TrimSpace(string(cpBytes))

	cmd := exec.Command(javaBin, "-cp", classpath, "redigo.RedigoProbe")
	cmd.Dir = probeDir
	cmd.Env = javaEnv()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		javaStartErr = err
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		javaStartErr = err
		return
	}
	if err := cmd.Start(); err != nil {
		javaStartErr = err
		return
	}
	javaCmd = cmd
	javaIn = stdin
	javaLines = make(chan string, 16)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			javaLines <- scanner.Text()
		}
		close(javaLines)
	}()

	// Pre-flight ping (also verifies Redis + Redisson connectivity).
	if _, err := javaSend("ping"); err != nil {
		stopJavaProbe()
		javaStartErr = err
	}
}

func javaEnv() []string {
	env := os.Environ()
	if os.Getenv("JAVA_HOME") == "" {
		for _, candidate := range []string{
			`C:\Program Files\Java\jdk-25.0.3`,
		} {
			if _, err := os.Stat(candidate); err == nil {
				env = append(env, "JAVA_HOME="+candidate)
				break
			}
		}
	}
	return env
}

// javaSend writes one command and reads its JSON reply.
func javaSend(command string) (map[string]any, error) {
	javaMu.Lock()
	defer javaMu.Unlock()
	if _, err := javaIn.Write([]byte(command + "\n")); err != nil {
		return nil, err
	}
	select {
	case line, ok := <-javaLines:
		if !ok {
			return nil, fmt.Errorf("java probe exited")
		}
		var reply map[string]any
		if err := json.Unmarshal([]byte(line), &reply); err != nil {
			return nil, fmt.Errorf("java reply not JSON: %q", line)
		}
		if e, has := reply["error"]; has {
			return nil, fmt.Errorf("java probe: %v", e)
		}
		return reply, nil
	case <-time.After(20 * time.Second):
		return nil, fmt.Errorf("java probe timed out on %q", command)
	}
}

func stopJavaProbe() {
	if javaIn != nil {
		_, _ = javaIn.Write([]byte("exit\n"))
		_ = javaIn.Close()
	}
	if javaCmd != nil {
		done := make(chan error, 1)
		go func() { done <- javaCmd.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = javaCmd.Process.Kill()
		}
	}
}

// numEq compares a JSON number (float64 from Go decode, or any numeric
// shape) against an int64 expected value.
func numEq(v any, want int64) bool {
	switch n := v.(type) {
	case float64:
		return int64(n) == want
	case int64:
		return n == want
	case int:
		return int64(n) == want
	case json.Number:
		return n.String() == strconv.FormatInt(want, 10)
	}
	return false
}

func TestJavaProbe_Smoke(t *testing.T) {
	javaProbe(t)
	reply, err := javaSend("ping")
	if err != nil {
		t.Fatal(err)
	}
	if reply["ok"] != true {
		t.Fatalf("ping reply = %v", reply)
	}
}

func TestJavaInterop_RLock(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-lock")
	t.Cleanup(func() { interopCleanup(t, name) })

	// Java holds; Go is blocked; release; Go acquires; Java blocked.
	if reply, err := javaSend("lock_hold " + name); err != nil || reply["acquired"] != true {
		t.Fatalf("java lock_hold = %v, %v", reply, err)
	}
	got, err := client.GetLock(name).TryLock(testCtx, "go:1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Fatal("Go TryLock succeeded while Redisson holds the lock")
	}
	if _, err := javaSend("lock_release"); err != nil {
		t.Fatal(err)
	}

	if err := client.GetLock(name).Lock(testCtx, "go:1", time.Minute); err != nil {
		t.Fatal("Go Lock after java release:", err)
	}
	if reply, _ := javaSend("lock_acquire " + name); reply["acquired"] == true {
		t.Fatal("Redisson tryLock succeeded while Go holds the lock")
	}
	if err := client.GetLock(name).Unlock(testCtx, "go:1"); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("lock_acquire " + name); err != nil || reply["acquired"] != true {
		t.Fatalf("java tryLock after Go release = %v, %v", reply, err)
	}
}

func TestJavaInterop_RMap(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-map")
	t.Cleanup(func() { interopCleanup(t, name) })
	m := client.GetMap(name)

	// Java writes plain string / Long; Go reads.
	mustJava(t, "map_put", name, `"hello"`, `"world"`)
	mustJava(t, "map_put", name, `"big"`, `5000000000`)
	v, err := m.Get(testCtx, "hello")
	if err != nil || v != "world" {
		t.Fatalf("Go read of java string = %v, %v", v, err)
	}
	v, err = m.Get(testCtx, "big")
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := v.(json.Number); !ok || n.String() != "5000000000" {
		t.Fatalf("Go read of java Long = %#v", v)
	}

	// Go writes string / Long / struct; Java reads.
	if err := m.Put(testCtx, "gostr", "from-go"); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(testCtx, "golong", 6000000000); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(testCtx, "goobj", map[string]any{"n": 1}); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("map_get " + name + ` "gostr"`); err != nil || reply["value"] != "from-go" {
		t.Fatalf("java read of Go string = %v, %v", reply, err)
	}
	if reply, err := javaSend("map_get " + name + ` "golong"`); err != nil {
		t.Fatal(err)
	} else if !numEq(reply["value"], 6000000000) {
		t.Fatalf("java read of Go Long = %#v", reply["value"])
	}
	if reply, err := javaSend("map_get " + name + ` "goobj"`); err != nil {
		t.Fatal(err)
	} else {
		obj, ok := reply["value"].(map[string]any)
		if !ok || !numEq(obj["n"], 1) {
			t.Fatalf("java read of Go struct = %#v", reply["value"])
		}
	}
}

func TestJavaInterop_RAtomicLong(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-along")
	t.Cleanup(func() { interopCleanup(t, name) })
	a := client.GetAtomicLong(name)

	if reply, err := javaSend("along_add " + name + " 5"); err != nil || !numEq(reply["value"], 5) {
		t.Fatalf("java add = %v, %v", reply, err)
	}
	v, err := a.Get(testCtx)
	if err != nil || v != 5 {
		t.Fatalf("Go read = %d, %v; want 5", v, err)
	}
	if _, err := a.AddAndGet(testCtx, 2); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("along_get " + name); err != nil || !numEq(reply["value"], 7) {
		t.Fatalf("java read = %v, %v; want 7", reply, err)
	}
}

func TestJavaInterop_RBloomFilter(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-bloom")
	t.Cleanup(func() { interopCleanup(t, name, "{"+name+"}:config") })
	f := client.GetBloomFilter(name)

	// Java initializes; Go must compute the identical bit size.
	if reply, err := javaSend("bloom_init " + name + " 1000 0.03"); err != nil || reply["ok"] != true {
		t.Fatalf("java bloom init = %v, %v", reply, err)
	}
	sz, err := f.Size(testCtx)
	if err != nil || sz != 7298 {
		t.Fatalf("Go size = %d, %v; want 7298 (Java formula agreement)", sz, err)
	}

	// Java adds; Go contains must agree (HighwayHash-128 key + codec agree).
	mustJava(t, "bloom_add", name, `"apple"`)
	contains, err := f.Contains(testCtx, "apple")
	if err != nil || !contains {
		t.Fatalf("Go contains(java apple) = %v, %v", contains, err)
	}
	// Go adds; Java contains must agree.
	if _, err := f.Add(testCtx, "banana"); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("bloom_contains " + name + ` "banana"`); err != nil || reply["contains"] != true {
		t.Fatalf("java contains(Go banana) = %v, %v", reply, err)
	}
}

func TestJavaInterop_RRateLimiter(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-rate")
	t.Cleanup(func() {
		// Config HASH lives at the bare name; value/permits keys are {name}:xxx.
		interopCleanup(t, name)
		interopCleanupPattern(t, "{"+name+"}:*")
	})

	// Java configures rate=2 / 60s and takes 1; Go takes the second; both
	// sides' third acquire must fail.
	if reply, err := javaSend("rate_set " + name + " 2 60000"); err != nil || reply["ok"] != true {
		t.Fatalf("java rate set = %v, %v", reply, err)
	}
	if reply, err := javaSend("rate_try " + name + " 1"); err != nil || reply["acquired"] != true {
		t.Fatalf("java acquire 1 = %v, %v", reply, err)
	}
	r := client.GetRateLimiter(name)
	ok, err := r.TryAcquire(testCtx, 1)
	if err != nil || !ok {
		t.Fatalf("Go acquire after java = %v, %v", ok, err)
	}
	ok, _ = r.TryAcquire(testCtx, 1)
	if ok {
		t.Fatal("Go acquire succeeded beyond java-configured rate=2")
	}
	if reply, _ := javaSend("rate_try " + name + " 1"); reply["acquired"] == true {
		t.Fatal("java acquire succeeded beyond its own rate=2")
	}
}

func TestJavaInterop_RSemaphore(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-sem")
	t.Cleanup(func() { interopCleanup(t, name, name+":total") })
	s := client.GetSemaphore(name)

	if reply, err := javaSend("sem_set " + name + " 1"); err != nil || reply["ok"] != true {
		t.Fatalf("java sem set = %v, %v", reply, err)
	}
	if reply, err := javaSend("sem_try " + name + " 1"); err != nil || reply["acquired"] != true {
		t.Fatalf("java sem acquire = %v, %v", reply, err)
	}
	ok, err := s.TryAcquire(testCtx, 1)
	if err != nil || ok {
		t.Fatalf("Go TryAcquire with 0 permits = %v, %v; want false", ok, err)
	}
	if _, err := javaSend("sem_release " + name + " 1"); err != nil {
		t.Fatal(err)
	}
	ok, err = s.TryAcquire(testCtx, 1)
	if err != nil || !ok {
		t.Fatalf("Go TryAcquire after java release = %v, %v", ok, err)
	}
}

func TestJavaInterop_RCountDownLatch(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-cdl")
	t.Cleanup(func() { interopCleanup(t, name) })

	if reply, err := javaSend("latch_set " + name + " 1"); err != nil || reply["ok"] != true {
		t.Fatalf("java latch set = %v, %v", reply, err)
	}
	l := client.GetCountDownLatch(name)

	opened := make(chan bool, 1)
	go func() {
		open, _ := l.Await(context.Background(), 5*time.Second)
		opened <- open
	}()

	select {
	case <-opened:
		t.Fatal("Await opened before countdown")
	case <-time.After(150 * time.Millisecond):
	}

	mustJava(t, "latch_count_down", name)
	select {
	case open := <-opened:
		if !open {
			t.Fatal("Await returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Go Await not woken by Redisson countDown")
	}
}

func mustJava(t *testing.T, args ...string) {
	t.Helper()
	if _, err := javaSend(strings.Join(args, " ")); err != nil {
		t.Fatal(err)
	}
}
