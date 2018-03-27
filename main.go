package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/charlievieth/chug/walk"
)

type Entry struct {
	Log *LogEntry
	Raw []byte
}

func (e *Entry) Lager() bool { return e.Log != nil && !e.Log.Timestamp.IsZero() }

func ParseEntry(b []byte) Entry {
	if maybeJSON(b) {
		if log, err := ParseLogEntry(b); err == nil {
			return Entry{Log: log}
		}
	}
	raw := make([]byte, len(b))
	copy(raw, b)
	return Entry{Raw: raw}
}

type entryByTime []Entry

func (e entryByTime) Len() int      { return len(e) }
func (e entryByTime) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

func (e entryByTime) Less(i, j int) bool {
	return e[i].Log != nil && e[j].Log != nil &&
		e[i].Log.Timestamp.Before(e[j].Log.Timestamp)
}

var bufferPool sync.Pool

func newBuffer() *bytes.Buffer {
	if v := bufferPool.Get(); v != nil {
		b := v.(*bytes.Buffer)
		b.Reset()
		return b
	}
	return new(bytes.Buffer)
}

func putBuffer(b *bytes.Buffer) {
	bufferPool.Put(b)
}

type bufferReadCloser struct {
	*bytes.Buffer
}

func (b bufferReadCloser) Close() error {
	putBuffer(b.Buffer)
	return nil
}

type gzipReadCloser struct {
	buf *bytes.Buffer
	gz  *gzip.Reader
}

func (g gzipReadCloser) Read(p []byte) (int, error) {
	return g.gz.Read(p)
}

func (g gzipReadCloser) Close() error {
	putBuffer(g.buf)
	return g.gz.Close()
}

type nopCloser struct{ io.Reader }

func (nopCloser) Close() error { return nil }

var readerPool sync.Pool

func newReader(rd io.Reader) *Reader {
	if v := readerPool.Get(); v != nil {
		r := v.(*Reader)
		r.Reset(rd)
		return r
	}
	return &Reader{b: bufio.NewReaderSize(rd, 32*1014)}
}

func putReader(r *Reader) {
	r.b.Reset(nil) // Remove reference
	readerPool.Put(r)
}

type Reader struct {
	b   *bufio.Reader
	buf []byte
}

func (r *Reader) Reset(rd io.Reader) {
	r.b.Reset(rd)
	r.buf = r.buf[:0]
}

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

func DecodeEntriesFile(name string) ([]Entry, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return DecodeEntries(f)
}

func DecodeEntries(rd io.Reader) ([]Entry, error) {
	r := newReader(rd)
	defer putReader(r)
	var err error
	var ents []Entry
	for {
		b, e := r.ReadLine()
		if len(b) != 0 {
			ents = append(ents, ParseEntry(b))
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	return ents, err
}

func DecodeValidEntries(rd io.Reader) ([]Entry, error) {
	r := newReader(rd)
	defer putReader(r)
	var err error
	var ents []Entry
	invalid := 0
	for invalid < 100 {
		b, e := r.ReadLine()
		if len(b) != 0 {
			if ent := ParseEntry(b); ent.Lager() {
				ent.Raw = nil
				ents = append(ents, ent)
			} else {
				invalid++
			}
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	if len(ents) == 0 || invalid >= 100 {
		return nil, nil // WARN WARN WARN
	}
	return ents, err
}

func DecodeEntriesFast(rd io.Reader) ([]Entry, error) {
	lines := make(chan []byte, 8)
	out := make(chan Entry, 8)
	wg := new(sync.WaitGroup)
	for i := 0; i < runtime.NumCPU()-1; i++ {
		wg.Add(1)
		go func(in chan []byte, out chan Entry, wg *sync.WaitGroup) {
			defer wg.Done()
			for b := range in {
				out <- ParseEntry(b)
			}
		}(lines, out, wg)
	}

	go func() {
		r := newReader(rd)
		defer putReader(r)
		var err error
		var buf []byte
		for err == nil {
			buf, err = r.ReadLine()
			// trim trailing newline
			if len(buf) >= 1 && buf[len(buf)-1] == '\n' {
				buf = buf[:len(buf)-1]
			}
			if len(buf) != 0 {
				// TODO: use a buffer pool
				b := make([]byte, len(buf))
				copy(b, buf)
				lines <- b
			}
		}
		if err != io.EOF {
			Fatal(err) // WARN
		}
		close(lines)
		wg.Wait()
		close(out)
	}()

	var ents []Entry
	for e := range out {
		ents = append(ents, e)
	}

	return ents, nil
}

func openFile(name string) (io.ReadCloser, error) {
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

type Walker struct {
	ents []Entry
	mu   sync.Mutex

	// WARN NEW
	reqs chan WalkRequest
	wg   sync.WaitGroup
}

type WalkRequest struct {
	path string
	typ  os.FileMode
	rc   io.ReadCloser
}

func (w *Walker) Worker() {
Loop:
	for r := range w.reqs {
		if r.typ.IsDir() {
			continue Loop
		}
		if !hasSuffix(r.path, ".log") {
			ok, err := filepath.Match("*.log.*.gz", filepath.Base(r.path))
			if err != nil {
				fmt.Fprintf(os.Stderr, "error (match - %s): %s\n", r.path, err)
				continue Loop
			}
			if !ok {
				continue Loop
			}
		}
		var xrc io.ReadCloser
		switch {
		case r.rc != nil:
			defer r.rc.Close()
			if hasSuffix(r.path, ".gz") {
				gz, err := gzip.NewReader(r.rc)
				if err != nil {
					fmt.Fprintf(os.Stderr, "error (rc - %s): %s\n", r.path, err)
					continue Loop
				}
				xrc = gz
			} else {
				xrc = r.rc
			}
		case r.typ.IsRegular():
			var err error
			xrc, err = openFile(r.path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error (file - %s): %s\n", r.path, err)
				continue Loop
			}
		default:
			fmt.Fprintf(os.Stderr, "WTF (path - %s): %s - %#v\n", r.path, r.typ, r.rc)
			continue Loop
		}
		defer xrc.Close()
		ents, err := DecodeValidEntries(xrc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error (decode - %s): %s\n", r.path, err)
			continue Loop
		}
		w.mu.Lock()
		w.ents = append(w.ents, ents...)
		w.mu.Unlock()
		continue Loop
	}
}

func (w *Walker) Walk(path string, typ os.FileMode, rc io.ReadCloser) error {
	w.reqs <- WalkRequest{path, typ, rc}
	return nil
}

func (w *Walker) XWalk(path string, typ os.FileMode, rc io.ReadCloser) error {
	if typ.IsDir() {
		return nil
	}
	if !hasSuffix(path, ".log") {
		ok, err := filepath.Match("*.log.*.gz", filepath.Base(path))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error (match - %s): %s\n", path, err)
			return nil
		}
		if !ok {
			return nil
		}
	}
	var xrc io.ReadCloser
	switch {
	case rc != nil:
		defer rc.Close()
		if hasSuffix(path, ".gz") {
			gz, err := gzip.NewReader(rc)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error (rc - %s): %s\n", path, err)
				return nil
			}
			xrc = gz
		} else {
			xrc = rc
		}
	case typ.IsRegular():
		var err error
		xrc, err = openFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error (file - %s): %s\n", path, err)
			return nil
		}
	default:
		fmt.Fprintf(os.Stderr, "WTF (path - %s): %s - %#v\n", path, typ, rc)
		return nil
	}
	defer xrc.Close()
	ents, err := DecodeValidEntries(xrc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error (decode - %s): %s\n", path, err)
		return nil
	}
	w.mu.Lock()
	w.ents = append(w.ents, ents...)
	w.mu.Unlock()
	return nil
}

func (w *Walker) EncodeJSON(filename string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, e := range w.ents {
		if e.Log == nil {
			continue
		}
		if err := enc.Encode(e.Log); err != nil {
			return err
		}
	}
	return nil
}

func ParseJSON(filename string) ([]LogEntry, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var logs []LogEntry
	for {
		var e LogEntry
		if e := dec.Decode(&e); e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
		logs = append(logs, e)
	}
	return logs, err
}

func PrintMemstats() {
	type MemStats struct {
		Alloc        uint64
		TotalAlloc   uint64
		Sys          uint64
		Lookups      uint64
		Mallocs      uint64
		Frees        uint64
		HeapAlloc    uint64
		HeapSys      uint64
		HeapIdle     uint64
		HeapInuse    uint64
		HeapReleased uint64
		HeapObjects  uint64
		StackInuse   uint64
		StackSys     uint64
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	x := MemStats{
		Alloc:        m.Alloc,
		TotalAlloc:   m.TotalAlloc,
		Sys:          m.Sys,
		Lookups:      m.Lookups,
		Mallocs:      m.Mallocs,
		Frees:        m.Frees,
		HeapAlloc:    m.HeapAlloc,
		HeapSys:      m.HeapSys,
		HeapIdle:     m.HeapIdle,
		HeapInuse:    m.HeapInuse,
		HeapReleased: m.HeapReleased,
		HeapObjects:  m.HeapObjects,
		StackInuse:   m.StackInuse,
		StackSys:     m.StackSys,
	}
	PrintJSON(x)
}

func main() {
	if len(os.Args) != 2 {
		Fatal("USAGE: PATH")
	}
	out := bufio.NewWriterSize(os.Stdout, 32*1024)
	p := NewPrinter(out)

	{
		start := time.Now()
		t := start
		x := Walker{
			reqs: make(chan WalkRequest, 4),
		}
		for i := 0; i < 4; i++ {
			x.wg.Add(1)
			go x.Worker()
		}

		fmt.Fprintln(os.Stderr, "walk start")
		if err := walk.Walk(os.Args[1], x.Walk); err != nil {
			Fatal(err)
		}
		close(x.reqs) // WARN NEW
		x.wg.Wait()   // WARN NEW
		d := time.Since(t)
		fmt.Fprintln(os.Stderr, "walk done:", d, d/time.Duration(len(x.ents)))

		fmt.Fprintln(os.Stderr, "sort start")
		t = time.Now()
		sort.Sort(entryByTime(x.ents))
		d = time.Since(t)
		fmt.Fprintln(os.Stderr, "sort done:", d, d/time.Duration(len(x.ents)))

		{
			fmt.Fprintln(os.Stderr, "json start")
			if err := x.EncodeJSON("out.json"); err != nil {
				Fatal(err)
			}
			d = time.Since(start)
			fmt.Fprintln(os.Stderr, "json done:", d, d/time.Duration(len(x.ents)))
			return
		}

		fmt.Fprintln(os.Stderr, "print start")
		t = time.Now()
		for _, e := range x.ents {
			if e.Log == nil {
				continue // shouldn't happen
			}
			if err := p.EncodePretty(e.Log); err != nil {
				Fatal(err)
			}
		}
		if err := out.Flush(); err != nil {
			Fatal(err)
		}
		d = time.Since(t)
		total := time.Since(start)
		fmt.Fprintln(os.Stderr, "print done:", d, d/time.Duration(len(x.ents)))
		fmt.Fprintln(os.Stderr, "total:", total, total/time.Duration(len(x.ents)))
		return
	}
}

type Config struct {
	Sort           bool
	AllLogs        bool
	FollowSymlinks bool
}

func (c *Config) Walk(root string) error {
	return nil
}

func PrintJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "    ")
	return enc.Encode(v)
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

// helpers

func maybeJSON(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\r', '\n':
			// skip space
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}

// hasSuffix tests whether the string s ends with suffix.  Same as
// strings.HasSuffix, but with a shorter name.
func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
