package util

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
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

func (b BufferReadCloser) Close() error {
	PutBuffer(b.Buffer)
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

func ReadAll(r io.Reader) (BufferReadCloser, error) {
	buf, err := readAll(r, bytes.MinRead)
	return BufferReadCloser{Buffer: buf}, err
}

func ReadAllSize(r io.Reader, size int64) (BufferReadCloser, error) {
	buf, err := readAll(r, size+bytes.MinRead)
	return BufferReadCloser{Buffer: buf}, err
}

type GzipReadCloser struct {
	rc io.ReadCloser
	*gzip.Reader
}

func (g GzipReadCloser) Read(p []byte) (int, error) {
	return g.Read(p)
}

func (g GzipReadCloser) Close() error {
	err := g.Close()
	if e := g.rc.Close(); e != nil && err == nil {
		err = e
	}
	return err
}

func NewGzipReadCloser(rc io.ReadCloser) (GzipReadCloser, error) {
	rr, ok := rc.(flate.Reader)
	if !ok {
		rr = bufio.NewReaderSize(rc, defaultBufSize)
	}
	gz, err := gzip.NewReader(rr)
	if err != nil {
		return GzipReadCloser{}, err
	}
	return GzipReadCloser{rc: rc, Reader: gz}, err
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
