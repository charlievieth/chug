package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/charlievieth/chug/util"
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

func DecodeEntriesFile(name string) ([]Entry, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return DecodeEntries(f)
}

func DecodeEntries(rd io.Reader) ([]Entry, error) {
	r := util.NewReader(rd)
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
	r := util.NewReader(rd)
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

// TODO: move to util - share with walk
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
	defer w.wg.Done()
	for r := range w.reqs {
		if err := w.Parse(r.path, r.typ, r.rc); err != nil {
			fmt.Fprintf(os.Stderr, "error (%+v): %s\n", r, err)
		}
	}
}

func (w *Walker) Walk(path string, typ os.FileMode, rc io.ReadCloser) error {
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
	w.reqs <- WalkRequest{path, typ, rc}
	return nil
}

// TODO: this is a mess - fix
func (w *Walker) Parse(path string, typ os.FileMode, rc io.ReadCloser) error {
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
	if len(ents) != 0 {
		w.mu.Lock()
		w.ents = append(w.ents, ents...)
		w.mu.Unlock()
	}
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

// TODO:
//  - strip ANSI escape sequences
//  - allow source colors to be set via the environment using a JSON map[string]string.
//  - add support for streaming stdint
//  - add support for writing to file
//  - allow max workers to be configured
//  - '-r' works without specifying path use PWD
//  - xz support
//
// See TODO section of walk/walk.go

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
			reqs: make(chan WalkRequest, 8),
		}
		for i := 0; i < 8; i++ {
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

		// {
		// 	fmt.Fprintln(os.Stderr, "json start")
		// 	if err := x.EncodeJSON("out.json"); err != nil {
		// 		Fatal(err)
		// 	}
		// 	d = time.Since(start)
		// 	fmt.Fprintln(os.Stderr, "json done:", d, d/time.Duration(len(x.ents)))
		// 	return
		// }

		fmt.Fprintln(os.Stderr, "print start")
		t = time.Now()
		for _, e := range x.ents {
			if e.Log == nil {
				continue // shouldn't happen
			}
			if err := p.EncodePretty(e.Log); err != nil {
				Fatal(err)
			}
			e.Log = nil // WARN: NEW
			e.Raw = nil // WARN: NEW
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

type GlobSet []string

func (g GlobSet) String() string {
	return fmt.Sprintf("%v", []string(g))
}

func (g GlobSet) Get() interface{} { return []string(g) }

func (g *GlobSet) Set(pattern string) error {
	return g.Add(pattern)
}

func (g *GlobSet) Add(pattern string) error {
	// make sure the pattern is valid
	if _, err := filepath.Match(pattern, pattern); err != nil {
		return err
	}
	*g = append(*g, pattern)
	return nil
}

func (g GlobSet) Match(basename string) bool {
	if len(g) == 0 {
		return true
	}
	for _, p := range g {
		if ok, _ := filepath.Match(p, basename); ok {
			return true
		}
	}
	return false
}

// WARN: make sure to add the default include pattern of '*.log*'
type PathConfig struct {
	include    GlobSet
	exclude    GlobSet
	excludeDir GlobSet
}

func (c *PathConfig) AddFlags(set *flag.FlagSet) {
	// TODO (CEV): describe GLOB matching and describe conflicts
	// between Exclude and Include.

	const includeUsage = "Search only files whose base name matches " +
		"GLOB using wildcard matching"

	const excludeUsage = "Skip files whose base name matches " +
		"GLOB using wildcard matching."

	const excludeDirUsage = "Skip directories whose base name matches " +
		"GLOB using wildcard matching."

	set.Var(&c.include, "-include", includeUsage)
	set.Var(&c.exclude, "-exclude", excludeUsage)
	set.Var(&c.excludeDir, "-exclude-dir", excludeUsage)
}

func (p *PathConfig) MatchFile(basename string) bool {
	return (len(p.include) == 0 || p.include.Match(basename)) &&
		(len(p.exclude) == 0 || !p.exclude.Match(basename))
}

func (p *PathConfig) MatchDir(basename string) bool {
	return len(p.excludeDir) == 0 || !p.excludeDir.Match(basename)
}

type Config struct {
	Recursive bool
	Sort      bool
	WriteJSON bool
	NoColor   bool
	Unique    bool

	Path PathConfig

	// TODO:
	// AllLogs        bool
	// FollowSymlinks bool
}

func (c *Config) AddFlags(set *flag.FlagSet) {
	// TODO: use exe name - not chug!
	const recursiveUsage = "Read all files under each directory, recursively.  " +
		"Note that if no file operand is given, chug searches the working directory."

	set.BoolVar(&c.Recursive, "recursive", false, recursiveUsage)
	set.BoolVar(&c.Recursive, "r", false, recursiveUsage)

	const sortUsage = "Sort logs by time.  May conflict with streaming logs from " +
		"STDIN as all log entries must be read before sorting."
	set.BoolVar(&c.Sort, "-sort", false, sortUsage)
	set.BoolVar(&c.Sort, "s", false, sortUsage)

	const jsonUsage = "Write output as JSON.  This is useful for combining multiple " +
		"log files.  This negates any printing options."
	set.BoolVar(&c.WriteJSON, "-json", false, jsonUsage)

	set.BoolVar(&c.NoColor, "-no-color", false, "Disable colored output.")

	set.BoolVar(&c.Unique, "-unique", false, "Remove duplicate log entries, implies --sort.")

	c.Path.AddFlags(set)
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

/*
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
*/
