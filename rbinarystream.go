package redi

import (
	"context"
	"errors"
	"io"

	"github.com/redis/go-redis/v9"
)

// RBinaryStream stores raw bytes in a Redis String. Unlike RBucket, values
// bypass the configured Codec to match Redisson's ByteArrayCodec wire format.
type RBinaryStream struct {
	rObject
}

func newRBinaryStream(c *Client, name string) *RBinaryStream {
	return &RBinaryStream{rObject{c: c, name: name}}
}

// Get returns all bytes, or (nil, nil) when the stream is absent.
func (s *RBinaryStream) Get(ctx context.Context) ([]byte, error) {
	value, err := s.rc().Get(ctx, s.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []byte(value), nil
}

// Set replaces the stream with raw bytes.
func (s *RBinaryStream) Set(ctx context.Context, data []byte) error {
	return s.rc().Set(ctx, s.name, data, 0).Err()
}

// Size returns the stream size in bytes.
func (s *RBinaryStream) Size(ctx context.Context) (int64, error) {
	return s.rc().StrLen(ctx, s.name).Result()
}

// Read returns length bytes starting at offset. A length of -1 reads through
// the end of the stream.
func (s *RBinaryStream) Read(ctx context.Context, offset, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, errors.New("redi: binary stream offset must not be negative")
	}
	if length < -1 {
		return nil, errors.New("redi: binary stream length must be -1 or non-negative")
	}
	if length == 0 {
		return []byte{}, nil
	}
	end := int64(-1)
	if length > 0 {
		end = offset + length - 1
		if end < offset {
			return nil, errors.New("redi: binary stream range overflows int64")
		}
	}
	value, err := s.rc().GetRange(ctx, s.name, offset, end).Result()
	if err != nil {
		return nil, err
	}
	return []byte(value), nil
}

// Write writes raw bytes at offset using SETRANGE and returns bytes written.
// A write beyond the current end fills the gap with zero bytes.
func (s *RBinaryStream) Write(ctx context.Context, offset int64, data []byte) (int, error) {
	if offset < 0 {
		return 0, errors.New("redi: binary stream offset must not be negative")
	}
	if _, err := s.rc().SetRange(ctx, s.name, offset, string(data)).Result(); err != nil {
		return 0, err
	}
	return len(data), nil
}

// Append appends raw bytes and returns the resulting stream size.
func (s *RBinaryStream) Append(ctx context.Context, data []byte) (int64, error) {
	return s.rc().Append(ctx, s.name, string(data)).Result()
}

// Truncate shrinks the stream to size bytes. As in Redisson 4.6.1, growing is
// a no-op, size zero deletes the key, and a non-zero shrink clears its TTL.
func (s *RBinaryStream) Truncate(ctx context.Context, size int64) error {
	if size < 0 {
		return errors.New("redi: binary stream size must not be negative")
	}
	if size == 0 {
		return s.Delete(ctx)
	}
	return binaryStreamTruncateScript.Run(ctx, s.rc(), []string{s.name}, size).Err()
}

// GetInputStream returns a sequential raw-byte reader starting at position 0.
// The returned reader is not safe for concurrent use.
func (s *RBinaryStream) GetInputStream(ctx context.Context) *RBinaryStreamReader {
	return &RBinaryStreamReader{stream: s, ctx: ctx}
}

// GetOutputStream returns an append-only raw-byte writer.
// The returned writer is not safe for concurrent use.
func (s *RBinaryStream) GetOutputStream(ctx context.Context) *RBinaryStreamWriter {
	return &RBinaryStreamWriter{stream: s, ctx: ctx}
}

// GetChannel returns a seekable raw-byte channel starting at position 0.
// The returned channel is not safe for concurrent use.
func (s *RBinaryStream) GetChannel(ctx context.Context) *RBinaryStreamChannel {
	return &RBinaryStreamChannel{stream: s, ctx: ctx}
}

// RBinaryStreamReader is the Go counterpart of Redisson's InputStream view.
type RBinaryStreamReader struct {
	stream   *RBinaryStream
	ctx      context.Context
	position int64
}

// Read reads from the current position and advances by the bytes returned.
func (r *RBinaryStreamReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	data, err := r.stream.Read(r.ctx, r.position, int64(len(p)))
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, data)
	r.position += int64(n)
	return n, nil
}

// RBinaryStreamWriter is the Go counterpart of Redisson's append-only
// OutputStream view.
type RBinaryStreamWriter struct {
	stream *RBinaryStream
	ctx    context.Context
}

// Write appends p and reports the number of bytes written.
func (w *RBinaryStreamWriter) Write(p []byte) (int, error) {
	if _, err := w.stream.Append(w.ctx, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// RBinaryStreamChannel is a seekable read/write view over an RBinaryStream.
// Close is intentionally a no-op, matching Redisson's channel.
type RBinaryStreamChannel struct {
	stream   *RBinaryStream
	ctx      context.Context
	position int64
}

// Read reads from the current position and advances by the bytes returned.
func (c *RBinaryStreamChannel) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	data, err := c.stream.Read(c.ctx, c.position, int64(len(p)))
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, data)
	c.position += int64(n)
	return n, nil
}

// Write writes at the current position and advances by len(p).
func (c *RBinaryStreamChannel) Write(p []byte) (int, error) {
	n, err := c.stream.Write(c.ctx, c.position, p)
	c.position += int64(n)
	return n, err
}

// Seek sets the channel position using standard io.Seeker semantics.
func (c *RBinaryStreamChannel) Seek(offset int64, whence int) (int64, error) {
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = c.position
	case io.SeekEnd:
		size, err := c.stream.Size(c.ctx)
		if err != nil {
			return c.position, err
		}
		base = size
	default:
		return c.position, errors.New("redi: invalid binary stream seek origin")
	}
	position := base + offset
	if (offset > 0 && position < base) || (offset < 0 && position > base) {
		return c.position, errors.New("redi: binary stream seek overflows int64")
	}
	if position < 0 {
		return c.position, errors.New("redi: binary stream position must not be negative")
	}
	c.position = position
	return position, nil
}

// Position returns the current channel position.
func (c *RBinaryStreamChannel) Position() int64 {
	return c.position
}

// Size returns the underlying stream size.
func (c *RBinaryStreamChannel) Size() (int64, error) {
	return c.stream.Size(c.ctx)
}

// Truncate shrinks the underlying stream without changing the channel
// position.
func (c *RBinaryStreamChannel) Truncate(size int64) error {
	return c.stream.Truncate(c.ctx, size)
}

// IsOpen always returns true, matching Redisson's no-op channel Close.
func (c *RBinaryStreamChannel) IsOpen() bool {
	return true
}

// Close is a no-op because the channel doesn't own a Redis connection.
func (c *RBinaryStreamChannel) Close() error {
	return nil
}

var binaryStreamTruncateScript = redis.NewScript(`
local len = redis.call('strlen', KEYS[1])
if tonumber(ARGV[1]) >= len then
    return 0
end
local limitedValue = redis.call('getrange', KEYS[1], 0, tonumber(ARGV[1])-1)
redis.call('set', KEYS[1], limitedValue)
return 1
`)
