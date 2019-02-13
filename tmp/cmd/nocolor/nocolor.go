package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
)

func Readlines(rd io.Reader) (int, error) {
	var lines int
	var err error
	r := newReader(rd)
	for {
		b, e := r.ReadBytes('\n')
		if len(b) != 0 {
			lines++
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	putReader(r)
	return lines, err
}

func ReadlinesNoColor(rd io.Reader) (int, error) {
	var lines int
	var err error
	r := newReader(rd)
	for {
		b, e := r.ReadBytes('\n')
		if len(b) != 0 {
			lines++
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	putReader(r)
	return lines, err
}

var colorRe = regexp.MustCompile("\x1b\\[[0-?]*[ -/]*[@-~]")

func StripColor(b []byte) []byte {
	if bytes.IndexByte(b, '\x1b') == -1 {
		return b
	}
	return colorRe.ReplaceAllFunc(b, func(_ []byte) []byte {
		return nil
	})
}

func colorless(rd io.Reader, out io.Writer) error {
	var err error
	r := newReader(rd)
	for {
		b, e := r.ReadBytes('\n')
		b = StripColor(b)
		if len(b) != 0 {
			out.Write(append(b, '\n'))
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	putReader(r)
	return err
}

type Reader struct {
	b   *bufio.Reader
	buf []byte
	out []byte
}

const defaultSize = 32 * 1024

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		b:   bufio.NewReaderSize(rd, defaultSize),
		buf: make([]byte, 0, 128),
		out: make([]byte, 0, 128),
	}
}

func (r *Reader) ReadBytes(delim byte) ([]byte, error) {
	var frag []byte
	var err error
	r.buf = r.buf[:0]
	for {
		var e error
		frag, e = r.b.ReadSlice(delim)
		if e == nil { // got final fragment
			break
		}
		if e != bufio.ErrBufferFull { // unexpected error
			err = e
			break
		}
		r.buf = append(r.buf, frag...)
	}
	r.buf = append(r.buf, frag...)
	return r.StripANSI(), err
}

// func (r *Reader) Readlines() (int, error) {
// 	var lines int
// 	var err error
// 	r := newReader(rd)
// 	for {
// 		b, e := r.ReadLine()
// 		if len(b) != 0 {
// 			lines++
// 		}
// 		if e != nil {
// 			if e != io.EOF {
// 				err = e
// 			}
// 			break
// 		}
// 	}
// 	putReader(r)
// 	return lines, err
// }

func (r *Reader) StripANSI() []byte {
	// TODO: prevent r.buf from sliding down the array and reallocating
	r.out = r.out[:0]
	for {
		start, end := findIndex(r.buf)
		if start == -1 {
			break
		}
		if start > 0 {
			r.out = append(r.out, r.buf[:start]...)
		}
		if end < len(r.buf) {
			r.buf = r.buf[end:]
		} else {
			r.buf = r.buf[len(r.buf):]
			break
		}
	}
	r.out = append(r.out, r.buf...)
	return r.out
}

func (r *Reader) ReadAll(wr io.Writer) error {
	out := bufio.NewWriterSize(wr, defaultSize)
	var buf []byte
	var err error
	for {
		buf, err = r.ReadBytes('\n')
		if err != nil {
			break
		}
		if _, err := out.Write(buf); err != nil {
			return errors.New("writing: " + err.Error())
		}
	}
	if err != io.EOF {
		return fmt.Errorf("reading: %s\n", err)
	}
	if _, err := out.Write(buf); err != nil {
		return errors.New("writing: " + err.Error())
	}
	if err := out.Flush(); err != nil {
		return errors.New("flushing: " + err.Error())
	}
	return nil
}

func findIndex(b []byte) (int, int) {
	// Pattern: \x1b\[[0-?]*[ -/]*[@-~]
	const minLen = 2 // "\\[[@-~]"

	start := bytes.IndexByte(b, '\x1b')
	if start == -1 || len(b)-start < minLen || b[start+1]-'@' > '_'-'@' {
		return -1, -1
	}

	n := start + 2 // ESC + second byte [@-_]

	// parameter bytes
	for ; n < len(b) && b[n]-'0' <= '?'-'0'; n++ {
	}
	// intermediate bytes
	for ; n < len(b) && b[n]-' ' <= '/'-' '; n++ {
	}
	// final byte
	if n < len(b) && b[n]-'@' <= '~'-'@' {
		return start, n + 1
	}
	return -1, -1
}

func xmain() {
	const name = "../../../testdata/test.json"
	b, err := ioutil.ReadFile(name)
	if err != nil {
		panic(err)
	}
	line := bytes.Split(b, []byte{'\n'})[0]

	x := colorRe.FindAllIndex(line, -1)
	fmt.Println(x)
}

func main() {
	{
		xmain()
		return
	}
	if err := colorless(os.Stdin, os.Stdout); err != nil {
		Fatal(err)
	}
}

func Fatal(err interface{}) {
	if err == nil {
		return
	}
	var s string
	if _, file, line, ok := runtime.Caller(1); ok && file != "" {
		s = fmt.Sprintf("Error (%s:%d)", filepath.Base(file), line)
	} else {
		s = "Error"
	}
	switch err.(type) {
	case error, string, fmt.Stringer:
		fmt.Fprintf(os.Stderr, "%s: %s\n", s, err)
	default:
		fmt.Fprintf(os.Stderr, "%s: %#v\n", s, err)
	}
	os.Exit(1)
}

var readerPool sync.Pool

func newReader(rd io.Reader) *Reader {
	if v := readerPool.Get(); v != nil {
		r := v.(*Reader)
		r.b.Reset(rd)
		return r
	}
	return NewReader(rd)
}

func putReader(r *Reader) {
	r.b.Reset(nil) // Remove reference
	readerPool.Put(r)
}
