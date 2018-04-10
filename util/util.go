package util

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"os"
	"regexp"
	"sync"
)

const defaultBufSize = 32 * 1024

var bufferPool sync.Pool

func NewBuffer() *bytes.Buffer {
	if v := bufferPool.Get(); v != nil {
		b := v.(*bytes.Buffer)
		b.Reset()
		return b
	}
	return new(bytes.Buffer)
}

func PutBuffer(b *bytes.Buffer) {
	if b != nil {
		bufferPool.Put(b)
	}
}

type BufferReadCloser struct {
	*bytes.Buffer
}

// WARN: making this a pointer "seemed" to help the race, but needs requires a
// more thorough investigation.
func (b *BufferReadCloser) Close() error {
	// WARN: FUCK THE RACE IS HERE - WTF
	PutBuffer(b.Buffer)
	b.Buffer = nil // TODO: necessary ???
	return nil
}

func readAll(r io.Reader, capacity int64) (*bytes.Buffer, error) {
	buf := NewBuffer()
	if int64(int(capacity)) == capacity {
		buf.Grow(int(capacity))
	}
	if _, err := buf.ReadFrom(r); err != nil {
		PutBuffer(buf)
		return nil, err
	}
	return buf, nil
}

func ReadAll(r io.Reader) (*BufferReadCloser, error) {
	buf, err := readAll(r, bytes.MinRead)
	if err != nil {
		return nil, err
	}
	return &BufferReadCloser{Buffer: buf}, nil
}

func ReadAllSize(r io.Reader, size int64) (*BufferReadCloser, error) {
	buf, err := readAll(r, size+bytes.MinRead)
	if err != nil {
		return nil, err
	}
	return &BufferReadCloser{Buffer: buf}, nil
}

type GzipReadCloser struct {
	rc io.ReadCloser
	gz *gzip.Reader
}

func (g *GzipReadCloser) Read(p []byte) (int, error) {
	return g.gz.Read(p)
}

func (g *GzipReadCloser) Close() error {
	if err := g.gz.Close(); err != nil {
		g.rc.Close()
		return err
	}
	return g.rc.Close()
}

func NewGzipReadCloser(rc io.ReadCloser) (*GzipReadCloser, error) {
	rr, ok := rc.(flate.Reader)
	if !ok {
		rr = bufio.NewReaderSize(rc, defaultBufSize)
	}
	gz, err := gzip.NewReader(rr)
	if err != nil {
		return nil, err
	}
	return &GzipReadCloser{rc: rc, gz: gz}, nil
}

type Reader struct {
	b   *bufio.Reader
	buf []byte
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{b: bufio.NewReaderSize(rd, defaultBufSize)}
}

func (r *Reader) Reset(rd io.Reader) {
	r.b.Reset(rd)
	r.buf = r.buf[:0]
}

// ReadLine reads a line from input, returning a slice containing the data up
// to but not including the delimiter. The bytes stop being valid at the next
// read.
func (r *Reader) ReadLine() ([]byte, error) {
	var frag []byte
	var err error
	r.buf = r.buf[:0]
	for {
		var e error
		frag, e = r.b.ReadSlice('\n')
		if e == nil { // got final fragment
			break
		}
		if e != bufio.ErrBufferFull { // unexpected error
			err = e
			break
		}
		r.buf = append(r.buf, frag...)
	}
	// do not include newline
	if len(frag) > 1 {
		r.buf = append(r.buf, frag[:len(frag)-1]...)
	}
	return r.buf, err
}

// ReadLineNoColor is like ReadLine, but removes ANSI escape sequences
// from the returned bytes.
func (r *Reader) ReadLineNoColor() ([]byte, error) {
	var frag []byte
	var err error
	r.buf = r.buf[:0]
	for {
		var e error
		frag, e = r.b.ReadSlice('\n')
		if e == nil { // got final fragment
			break
		}
		if e != bufio.ErrBufferFull { // unexpected error
			err = e
			break
		}
		r.buf = append(r.buf, frag...)
	}
	// do not include newline
	if len(frag) > 1 {
		r.buf = append(r.buf, frag[:len(frag)-1]...)
	}
	return StripColor(r.buf), err
}

var colorRe = regexp.MustCompile("\x1b\\[[0-?]*[ -/]*[@-~]")

// TODO: this is really StripANSI as we remove more than color escape sequences.
func StripColor(b []byte) []byte {
	if len(b) == 0 || bytes.IndexByte(b, '\x1b') == -1 {
		return b
	}
	return colorRe.ReplaceAllFunc(b, func(_ []byte) []byte {
		return nil
	})
}

func OpenFile(name string) (io.ReadCloser, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	var rc io.ReadCloser = f
	if hasSuffix(name, ".gz") || hasSuffix(name, ".tgz") {
		gr, err := gzip.NewReader(bufio.NewReaderSize(f, 32*1024))
		if err != nil {
			f.Close()
			return nil, err
		}
		rc = gr
	}
	return rc, nil
}

// hasSuffix tests whether the string s ends with suffix.  Same as
// strings.HasSuffix, but with a shorter name.
func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
