package redi_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

var (
	_ io.Reader          = (*redi.RBinaryStreamReader)(nil)
	_ io.Writer          = (*redi.RBinaryStreamWriter)(nil)
	_ io.ReadWriteSeeker = (*redi.RBinaryStreamChannel)(nil)
	_ io.Closer          = (*redi.RBinaryStreamChannel)(nil)
)

func TestWire_RBinaryStreamRawString(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "binary")
	stream := client.GetBinaryStream(name)
	t.Cleanup(func() { interopCleanup(t, name) })

	got, err := stream.Get(testCtx)
	if err != nil || got != nil {
		t.Fatalf("Get absent = %v, %v; want nil, nil", got, err)
	}

	want := []byte{0x00, '{', '"', 0xff}
	if err := stream.Set(testCtx, want); err != nil {
		t.Fatal("Set:", err)
	}

	rc := rawClient(t)
	raw, err := rc.Get(testCtx, name).Bytes()
	if err != nil || !bytes.Equal(raw, want) {
		t.Fatalf("raw Redis value = %v, %v; want %v", raw, err, want)
	}
	typ, err := rc.Type(testCtx, name).Result()
	if err != nil || typ != "string" {
		t.Fatalf("Redis type = %q, %v; want string", typ, err)
	}

	part, err := stream.Read(testCtx, 1, 2)
	if err != nil || !bytes.Equal(part, want[1:3]) {
		t.Fatalf("Read(1, 2) = %v, %v", part, err)
	}
	rest, err := stream.Read(testCtx, 2, -1)
	if err != nil || !bytes.Equal(rest, want[2:]) {
		t.Fatalf("Read(2, -1) = %v, %v", rest, err)
	}

	if n, err := stream.Write(testCtx, 1, []byte{0xaa, 0x00}); err != nil || n != 2 {
		t.Fatalf("Write = %d, %v; want 2", n, err)
	}
	want = []byte{0x00, 0xaa, 0x00, 0xff}
	if size, err := stream.Append(testCtx, []byte{0xfe}); err != nil || size != 5 {
		t.Fatalf("Append size = %d, %v; want 5", size, err)
	}
	want = append(want, 0xfe)

	got, err = stream.Get(testCtx)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("Get = %v, %v; want %v", got, err, want)
	}
	if size, err := stream.Size(testCtx); err != nil || size != int64(len(want)) {
		t.Fatalf("Size = %d, %v; want %d", size, err, len(want))
	}
}

func TestRBinaryStream_StreamViews(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "binary-views")
	stream := client.GetBinaryStream(name)
	t.Cleanup(func() { interopCleanup(t, name) })

	output := stream.GetOutputStream(testCtx)
	if n, err := output.Write([]byte("abc")); err != nil || n != 3 {
		t.Fatalf("output Write = %d, %v", n, err)
	}
	if n, err := output.Write([]byte("def")); err != nil || n != 3 {
		t.Fatalf("output Write = %d, %v", n, err)
	}

	input := stream.GetInputStream(testCtx)
	first := make([]byte, 2)
	if n, err := io.ReadFull(input, first); err != nil || n != 2 || string(first) != "ab" {
		t.Fatalf("input first read = %q, %d, %v", first, n, err)
	}
	rest, err := io.ReadAll(input)
	if err != nil || string(rest) != "cdef" {
		t.Fatalf("input rest = %q, %v", rest, err)
	}
}

func TestRBinaryStream_ChannelSeekAndTruncate(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "binary-channel")
	stream := client.GetBinaryStream(name)
	t.Cleanup(func() { interopCleanup(t, name) })

	if err := stream.Set(testCtx, []byte("abcdef")); err != nil {
		t.Fatal("Set:", err)
	}
	channel := stream.GetChannel(testCtx)

	buf := make([]byte, 2)
	if n, err := channel.Read(buf); err != nil || n != 2 || string(buf) != "ab" {
		t.Fatalf("channel Read = %q, %d, %v", buf, n, err)
	}
	if pos := channel.Position(); pos != 2 {
		t.Fatalf("Position = %d; want 2", pos)
	}
	if pos, err := channel.Seek(-1, io.SeekCurrent); err != nil || pos != 1 {
		t.Fatalf("Seek current = %d, %v; want 1", pos, err)
	}
	if n, err := channel.Write([]byte("XY")); err != nil || n != 2 {
		t.Fatalf("channel Write = %d, %v", n, err)
	}
	if pos, err := channel.Seek(-2, io.SeekEnd); err != nil || pos != 4 {
		t.Fatalf("Seek end = %d, %v; want 4", pos, err)
	}
	tail, err := io.ReadAll(channel)
	if err != nil || string(tail) != "ef" {
		t.Fatalf("channel tail = %q, %v; want ef", tail, err)
	}

	if pos, err := channel.Seek(8, io.SeekStart); err != nil || pos != 8 {
		t.Fatalf("Seek gap = %d, %v; want 8", pos, err)
	}
	if n, err := channel.Write([]byte{'Z'}); err != nil || n != 1 {
		t.Fatalf("gap Write = %d, %v", n, err)
	}
	want := []byte{'a', 'X', 'Y', 'd', 'e', 'f', 0, 0, 'Z'}
	got, err := stream.Get(testCtx)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("gap value = %v, %v; want %v", got, err, want)
	}

	if ok, err := stream.Expire(testCtx, time.Minute); err != nil || !ok {
		t.Fatalf("Expire = %v, %v", ok, err)
	}
	if err := channel.Truncate(4); err != nil {
		t.Fatal("Truncate:", err)
	}
	got, err = stream.Get(testCtx)
	if err != nil || string(got) != "aXYd" {
		t.Fatalf("truncated value = %q, %v; want aXYd", got, err)
	}
	if ttl, err := stream.RemainTTL(testCtx); err != nil || ttl >= 0 {
		t.Fatalf("TTL after Redisson-style truncate = %v, %v; want persistent", ttl, err)
	}
	if pos := channel.Position(); pos != 9 {
		t.Fatalf("truncate changed position to %d; want 9", pos)
	}

	if err := channel.Truncate(20); err != nil {
		t.Fatal("growing Truncate:", err)
	}
	if size, err := channel.Size(); err != nil || size != 4 {
		t.Fatalf("Size after growing truncate = %d, %v; want 4", size, err)
	}
	if err := channel.Truncate(0); err != nil {
		t.Fatal("zero Truncate:", err)
	}
	if exists, err := stream.Exists(testCtx); err != nil || exists {
		t.Fatalf("Exists after zero truncate = %v, %v; want false", exists, err)
	}
	if !channel.IsOpen() {
		t.Fatal("channel should always remain open")
	}
	if err := channel.Close(); err != nil {
		t.Fatal("Close:", err)
	}
}
