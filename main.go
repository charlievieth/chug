package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/charlievieth/chug/color"
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

func (w *Walker) doWork() {
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
		xrc, err = util.OpenFile(path)
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

func RipFile(name string) ([]*LogEntry, error) {
	f, err := util.OpenFile(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return RipN(f, runtime.NumCPU())
}

func ripWorker(wg *sync.WaitGroup, lines <-chan []byte, out *[]*LogEntry) {
	defer wg.Done()
	ents := make([]*LogEntry, 0, 128)
	for b := range lines {
		ent, err := ParseLogEntry(b)
		if err != nil || ent.Timestamp.IsZero() {
			continue
		}
		ents = append(ents, ent)
	}
	*out = ents
}

func RipN(rc io.ReadCloser, numCPU int) ([]*LogEntry, error) {
	defer rc.Close()

	if numCPU < 1 {
		numCPU = 1
	}
	lineCh := make(chan []byte, numCPU)
	workerEnts := make([][]*LogEntry, numCPU)
	var wg sync.WaitGroup

	for i := 0; i < numCPU; i++ {
		wg.Add(1)
		go func(pents *[]*LogEntry) {
			defer wg.Done()
			ents := make([]*LogEntry, 0, 128)
			for b := range lineCh {
				ent, err := ParseLogEntry(b)
				if err != nil {
					continue // WARN: handle
				}
				if !ent.Timestamp.IsZero() {
					ents = append(ents, ent)
				}
			}
			*pents = ents
		}(&workerEnts[i])
	}

	rd := util.NewReader(rc)
	var err error
	for {
		b, e := rd.ReadLine()
		if len(b) != 0 && maybeJSON(b) {
			x := make([]byte, len(b))
			copy(x, b)
			lineCh <- x
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	close(lineCh)

	if err != nil {
		return nil, err
	}
	wg.Wait()

	var n int
	for _, a := range workerEnts {
		n += len(a)
	}
	all := make([]*LogEntry, 0, n)
	for _, a := range workerEnts {
		all = append(all, a...)
	}
	return all, nil
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

var Debugf = func(format string, args ...interface{}) {}
var Debugln = func(args ...interface{}) {}

func isDir(name string) bool {
	fi, err := os.Stat(name)
	return err == nil && fi.IsDir()
}

func realMain() error {
	set := flag.NewFlagSet(filepath.Base(os.Args[0]), flag.ExitOnError)
	set.Usage = func() {
		fmt.Fprintf(set.Output(), "%s USAGE: [OPTIONS] FILEPATHS...\n", set.Name())
		set.PrintDefaults()
	}

	var conf Config
	conf.AddFlags(set)

	if err := set.Parse(os.Args[1:]); err != nil {
		return err
	}

	// TODO: move setup to Config
	if conf.Debug || conf.DebugColor {
		prefix := "debug: "
		if conf.DebugColor {
			prefix = color.ColorGreen + prefix + color.StyleDefault
		}
		ll := log.New(os.Stderr, prefix, 0)
		Debugf = ll.Printf
		Debugln = ll.Println
	}

	Debugln("arguments: ", set.Args())
	set.Visit(func(f *flag.Flag) {
		Debugln("flag:", f.Name)
	})

	// TODO: stream
	if set.NArg() == 0 {
		set.Usage()
		return errors.New("missing required argument FILEPATHS...")
	}

	out := bufio.NewWriterSize(os.Stdout, 32*1024)
	p := NewPrinter(out)

	// TODO: make sure the arg is a file!!!
	if set.NArg() == 1 && !isDir(set.Arg(0)) && !hasSuffix(set.Arg(0), ".tar") {
		name := set.Arg(0)
		Debugln("ripping file:", name)
		t := time.Now()
		start := t
		ents, err := RipFile(name)
		if err != nil {
			return err
		}
		Debugln("ripping complete:", len(ents), time.Since(t))

		t = time.Now()
		sort.Sort(logByTime(ents))
		Debugln("sort complete:", len(ents), time.Since(t))

		t = time.Now()
		if conf.WriteJSON {
			enc := json.NewEncoder(out)
			for i := 0; i < len(ents); i++ {
				if err := enc.Encode(ents[i]); err != nil {
					return err
				}
				ents[i] = nil
			}
			Debugln("json complete:", time.Since(t))
		} else {
			for i := 0; i < len(ents); i++ {
				if err := p.EncodePretty(ents[i]); err != nil {
					return err
				}
				ents[i] = nil
			}
			Debugln("print complete:", time.Since(t))
		}
		Debugln("total:", time.Since(start))

	} else {
		numCPU := runtime.NumCPU()
		walker := Walker{
			reqs: make(chan WalkRequest, numCPU),
		}
		for i := 0; i < numCPU; i++ {
			walker.wg.Add(1)
			go walker.doWork()
		}
		for _, name := range set.Args() {
			Debugf("walking (start): %s", name)
			if err := walk.Walk(name, walker.Walk); err != nil {
				return fmt.Errorf("walker: %s", err)
			}
			Debugf("walking (done): %s", name)
		}
		close(walker.reqs)
		walker.wg.Wait()

		sort.Sort(entryByTime(walker.ents))

		if conf.WriteJSON {
			enc := json.NewEncoder(out)
			for _, e := range walker.ents {
				if e.Log == nil {
					continue // shouldn't happen
				}
				if err := enc.Encode(e.Log); err != nil {
					return err
				}
				// free resources
				e.Log = nil
				e.Raw = nil
			}
		} else {
			for _, e := range walker.ents {
				if e.Log == nil {
					continue // shouldn't happen
				}
				if err := p.EncodePretty(e.Log); err != nil {
					return err
				}
				// free resources
				e.Log = nil
				e.Raw = nil
			}
		}
	}

	if err := out.Flush(); err != nil {
		return err
	}

	return nil
}

func main() {
	if err := realMain(); err != nil {
		Fatal(err)
	}
}

/*
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
			go x.doWork()
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
*/

// WARN: make sure to add the default include pattern of '*.log*'
type PathConfig struct {
	allPaths   bool
	include    GlobSet
	exclude    GlobSet
	excludeDir GlobSet
}

func (c *PathConfig) AddFlags(set *flag.FlagSet) {
	// TODO (CEV): describe GLOB matching and describe conflicts
	// between Exclude and Include.

	const includeUsage = "" +
		"Search only files whose base name matches GLOB using wildcard matching."

	const excludeUsage = "" +
		"Skip files whose base name matches GLOB using wildcard matching."

	const excludeDirUsage = "" +
		"Skip directories whose base name matches GLOB using wildcard matching."

	const allPathsUsage = "" +
		"Search all files.  Otherwise only files matching the globs '*.log' and\n" +
		"'*.log.*.gz' are searched.  The '-include', '-exclude' and '-exclude-dir'\n" +
		"flags are still respected."

	set.Var(&c.include, "-include", includeUsage)
	set.Var(&c.exclude, "-exclude", excludeUsage)
	set.Var(&c.excludeDir, "-exclude-dir", excludeUsage)
	set.BoolVar(&c.allPaths, "-all", false, allPathsUsage)
}

func (p *PathConfig) matchDefault(basename string) bool {
	if hasSuffix(basename, ".log") {
		return true
	}
	ok, _ := filepath.Match("*.log.*.gz", basename)
	return ok
}

func (p *PathConfig) MatchFile(basename string) bool {
	if !p.allPaths && p.matchDefault(basename) {
		return true
	}
	return p.include.Match(basename) && !p.exclude.Exclude(basename)
}

func (p *PathConfig) MatchDir(basename string) bool {
	return p.excludeDir.Exclude(basename)
}

type Config struct {
	Debug      bool
	DebugColor bool
	Recursive  bool
	Sort       bool // TODO: change this to no-sort
	Stream     bool
	WriteJSON  bool
	NoColor    bool
	Unique     bool
	LocalTime  bool

	Path PathConfig

	// TODO:
	// AllLogs        bool
	// FollowSymlinks bool
}

func (c *Config) AddFlags(set *flag.FlagSet) {
	// TODO: use exe name - not chug!
	const recursiveUsage = "" +
		"Read all files under each directory, recursively.  Note that if no file\n" +
		"operand is given, chug searches the working directory."

	set.BoolVar(&c.Recursive, "recursive", false, recursiveUsage)
	set.BoolVar(&c.Recursive, "r", false, recursiveUsage)

	set.BoolVar(&c.Debug, "debug", false, "Print gobs of debugging information.")
	set.BoolVar(&c.DebugColor, "debug-color", false, "Colorize debug output.")

	const sortUsage = "" +
		"Sort logs by time.  May conflict with streaming logs from STDIN as all\n" +
		"log entries must be read before sorting."

	// TODO: change this to no-sort
	set.BoolVar(&c.Sort, "sort", false, sortUsage)
	set.BoolVar(&c.Sort, "s", false, sortUsage)

	set.BoolVar(&c.Stream, "stream", false,
		"Treat STDIN as a stream (disables sorting, which requires reading to EOF")

	const jsonUsage = "" +
		"Write output as JSON.  This is useful for combining multiple log files.\n" +
		"This negates any printing options."

	set.BoolVar(&c.WriteJSON, "json", false, jsonUsage)

	set.BoolVar(&c.NoColor, "no-color", false, "Disable colored output.")

	set.BoolVar(&c.Unique, "unique", false, "Remove duplicate log entries, implies --sort.")

	set.BoolVar(&c.LocalTime, "local", false, "Use the local time for log timestamps.  "+
		"By default UTC is used")

	c.Path.AddFlags(set)
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
