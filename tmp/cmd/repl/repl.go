package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

func unquoteNoEscape(b []byte) []byte {
	if len(b) >= 2 && b[0] == '"' && b[len(b)-1] == '"' {
		return b[1 : len(b)-1]
	}
	return b
}

type LogLevel int

const (
	DEBUG = LogLevel(iota)
	INFO
	ERROR
	FATAL
	// TODO: Add INVALID
)

var logLevelStr = [...]string{
	DEBUG: "debug",
	INFO:  "info",
	ERROR: "error",
	FATAL: "fatal",
	// upper case
	"DEBUG",
	"INFO",
	"ERROR",
	"FATAL",
}

func (l LogLevel) valid() bool { return DEBUG <= l && l <= FATAL }

func (l LogLevel) String() string {
	if l.valid() {
		return logLevelStr[l]
	}
	return "invalid"
}

func (l LogLevel) Upper() string {
	if l.valid() {
		return logLevelStr[l+FATAL+1]
	}
	return "INVALID"
}

func (l LogLevel) MarshalJSON() ([]byte, error) {
	return []byte(`"` + l.String() + `"`), nil
}

func (l *LogLevel) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	data = unquoteNoEscape(data)
	if len(data) == 1 {
		n := LogLevel(data[0] - '0')
		if n.valid() {
			*l = n
			return nil
		}
		*l = DEBUG
		return errors.New(`invalid LogLevel: "` + string(data) + `"`)
	}
	switch string(data) {
	case "debug":
		*l = DEBUG
	case "info":
		*l = INFO
	case "error":
		*l = ERROR
	case "fatal":
		*l = FATAL
	default:
		*l = DEBUG
		return errors.New(`invalid LogLevel: "` + string(data) + `"`)
	}
	return nil
}

type Timestamp time.Time

func (t Timestamp) Time() time.Time              { return time.Time(t) }
func (t Timestamp) String() string               { return time.Time(t).String() }
func (t Timestamp) MarshalJSON() ([]byte, error) { return time.Time(t).MarshalJSON() }

func parseUnixTimestamp(b []byte) (time.Time, bool) {
	// N.B. I was a bored when I wrote this so its a bit over-optimized.
	var dot bool
	var nsec int64
	if len(b) == 0 || !('1' <= b[0] && b[0] <= '9') {
		goto Error
	}
	for _, c := range b {
		switch {
		case '0' <= c && c <= '9':
			nsec = nsec*10 + int64(c-'0')
		case c == '.' && !dot:
			dot = true
		default:
			goto Error
		}
	}
	if !dot {
		nsec *= 1e9
	}
	return time.Unix(0, nsec), true

Error:
	return time.Time{}, false
}

func (t *Timestamp) UnmarshalJSON(data []byte) error {
	// Ignore null, like in the main JSON package.
	if string(data) == "null" {
		return nil
	}
	data = unquoteNoEscape(data)
	if tt, ok := parseUnixTimestamp(data); ok {
		*t = Timestamp(tt)
		return nil
	}
	tt, err := time.Parse(time.RFC3339, string(data))
	*t = Timestamp(tt)
	return err
}

type CombinedFormat struct {
	Timestamp Timestamp                  `json:"timestamp"`
	Source    string                     `json:"source"`
	Message   string                     `json:"message"`
	LevelV1   LogLevel                   `json:"log_level"`
	LevelV2   LogLevel                   `json:"level"`
	Data      map[string]json.RawMessage `json:"data,omitempty"` // lazily parsed
	Error     error                      `json:"-"`
}

func (c *CombinedFormat) LogLevel() LogLevel {
	if c.LevelV1 > c.LevelV2 {
		return c.LevelV1
	}
	return c.LevelV2
}

type Report struct {
	Name  string
	Total int
	Error int
	Valid int
}

type byName []*Report

func (b byName) Len() int           { return len(b) }
func (b byName) Less(i, j int) bool { return b[i].Name < b[j].Name }
func (b byName) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

func NewReport(filename string) (*Report, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	p := Report{Name: filename}
	r := bufio.NewReader(f)
	for {
		b, e := r.ReadBytes('\n')
		if len(b) != 0 {
			p.Total++
			var log CombinedFormat
			if err := json.Unmarshal(b, &log); err != nil {
				p.Error++
			} else {
				if !log.Timestamp.Time().IsZero() {
					p.Valid++
				}
			}
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	return &p, err
}

type Walker struct {
	reps []Report
	mu   sync.Mutex
}

func Worker(paths <-chan string, out chan<- *Report, wg *sync.WaitGroup) {
	defer wg.Done()
	for path := range paths {
		rep, err := NewReport(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s\n", path, err)
			continue
		}
		out <- rep
	}
}

func Walk(root string) []*Report {
	all := make([]*Report, 0, 256)
	out := make(chan *Report)
	owg := new(sync.WaitGroup)
	owg.Add(1)
	go func() {
		defer owg.Done()
		for rep := range out {
			all = append(all, rep)
		}
	}()

	wwg := new(sync.WaitGroup)
	paths := make(chan string, 8)
	for i := 0; i < runtime.NumCPU()-1; i++ {
		wwg.Add(1)
		go Worker(paths, out, wwg)
	}

	filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if !fi.Mode().IsRegular() || !strings.HasSuffix(fi.Name(), ".log") {
			return nil
		}
		paths <- path
		return nil
	})
	close(paths)
	wwg.Wait()
	close(out)
	owg.Wait()

	return all
}

type LogEntry struct {
	Timestamp time.Time
	LogLevel  LogLevel
	Source    string
	Message   string
	Session   string
	Error     string
	Trace     string
	Data      json.RawMessage // lazily parsed
}

/*
	Match Fields:

	Timestamp
	LogLevel
	Source
	Message
	Session
	Error
	Trace
	Data
*/

type LagerMatcher struct {
	Timestamp *TimeMatcher
	LogLevel  *MinLogLevelMatcher
	Source    *PatternMatcher
	Message   *PatternMatcher
	Session   *PatternMatcher
	Error     *PatternMatcher
	Trace     *PatternMatcher
	Data      *PatternMatcher
}

func (m *LagerMatcher) isEmpty(allFields bool) bool {
	return (!allFields || m.Timestamp == nil) && (!allFields || m.LogLevel == nil) &&
		m.Source == nil && m.Message == nil && m.Session == nil && m.Error == nil &&
		m.Trace == nil && m.Data == nil
}

func (m *LagerMatcher) Candidate(line []byte) bool {
	if m.isEmpty(false) {
		return true
	}

	return (m.Source != nil && m.Source.Match(line)) ||
		(m.Message != nil && m.Message.Match(line)) ||
		(m.Session != nil && m.Session.Match(line)) ||
		(m.Error != nil && m.Error.Match(line)) ||
		(m.Trace != nil && m.Trace.Match(line)) ||
		(m.Data != nil && m.Data.Match(line))
}

func (m *LagerMatcher) Match(e *LogEntry) bool {
	if m.isEmpty(true) {
		return true
	}

	// exclusive matches
	if m.LogLevel != nil && !m.LogLevel.Match(e.LogLevel) {
		return false
	}
	if m.Timestamp != nil && !m.Timestamp.Match(e.Timestamp) {
		return false
	}

	// TODO:
	//  - require everything to match?
	//  - what about negative matches (exclusions)?
	//
	if m.Source != nil && e.Source != "" && m.Source.MatchString(e.Source) {
		return true
	}
	if m.Message != nil && e.Message != "" && m.Message.MatchString(e.Message) {
		return true
	}
	if m.Session != nil && e.Session != "" && m.Session.MatchString(e.Session) {
		return true
	}
	if m.Error != nil && e.Error != "" && m.Error.MatchString(e.Error) {
		return true
	}
	if m.Trace != nil && e.Trace != "" && m.Trace.MatchString(e.Trace) {
		return true
	}
	if m.Data != nil && m.Data.Match(e.Data) {
		return true
	}
	return false
}

type TimeMatcher struct {
	Min, Max time.Time
}

func NewTimeMatcher(min, max time.Time) (*TimeMatcher, error) {
	if !max.IsZero() && min.After(max) {
		return nil, fmt.Errorf("TimeMatcher: min (%s) occurs after max (%s)", min, max)
	}
	m := &TimeMatcher{
		Min: min,
		Max: max,
	}
	return m, nil
}

func (m *TimeMatcher) Match(t time.Time) bool {
	zMin := m.Min.IsZero()
	zMax := m.Max.IsZero()
	if !zMin && !zMax {
		return m.Min.Before(t) && m.Max.After(t)
	}
	if !zMin {
		return m.Min.Before(t)
	}
	if !zMax {
		return m.Max.After(t)
	}
	return true
}

// TODO: use ("(log_)?level":\s*(1|"info")) to match candidates
type MinLogLevelMatcher LogLevel

func NewMinLogLevelMatcher(v LogLevel) (MinLogLevelMatcher, error) {
	if !v.valid() {
		return 0, fmt.Errorf("invalid LogLevel: %d", v)
	}
	return MinLogLevelMatcher(v), nil
}

func (m MinLogLevelMatcher) Match(v LogLevel) bool { return v >= LogLevel(m) }

type LogLevelMatcher struct {
	levels [FATAL + 1]bool
}

func NewLogLevelMatcher(levels ...LogLevel) (LogLevelMatcher, error) {
	var m LogLevelMatcher
	if len(levels) == 0 {
		return m, errors.New("no LogLevels specified")
	}
	for _, v := range levels {
		if !v.valid() {
			return m, fmt.Errorf("invalid LogLevel: %d", v)
		}
		m.levels[v] = true
	}
	return m, nil
}

func (m LogLevelMatcher) Match(v LogLevel) bool {
	return v.valid() && m.levels[v]
}

func isRegex(s string) bool { return strings.ContainsAny(s, "$()*+.?[\\]^{|}") }

type PatternMatcher struct {
	expr  string
	bexpr []byte
	re    *regexp.Regexp
}

func NewPatternMatcher(expr string) (*PatternMatcher, error) {
	var re *regexp.Regexp
	if isRegex(expr) {
		var err error
		re, err = regexp.Compile(expr)
		if err != nil {
			return nil, err
		}
	}
	m := &PatternMatcher{
		expr:  expr,
		bexpr: []byte(expr),
		re:    re,
	}
	return m, nil
}

func (m *PatternMatcher) Match(b []byte) bool {
	if m.re != nil {
		return m.re.Match(b)
	}
	return bytes.Contains(b, m.bexpr)
}

func (m *PatternMatcher) MatchString(s string) bool {
	if m.re != nil {
		return m.re.MatchString(s)
	}
	return strings.Contains(s, m.expr)
}

type Matcher interface {
	Match(b []byte) bool
	MatchString(s string) bool
}

type GlobSet []string

func (g GlobSet) Len() int { return len(g) }

func (g *GlobSet) Add(pattern string) error {
	// make sure the pattern is valid
	if _, err := filepath.Match(pattern, pattern); err != nil {
		return err
	}
	*g = append(*g, pattern)
	return nil
}

func (g GlobSet) Validate() error {
	for _, p := range g {
		if _, err := filepath.Match(p, p); err != nil {
			return err
		}
	}
	return nil
}

func (g GlobSet) MatchAny(basename string) bool {
	for _, p := range g {
		if ok, _ := filepath.Match(p, basename); ok {
			return true
		}
	}
	return false
}

// Rename
type FileSelection struct {
	include    GlobSet
	exclude    GlobSet
	excludeDir GlobSet
	walkLinks  bool

	// NOTE (unix): use fs1.sys.Dev && fs1.sys.Ino to check for link loop
	// NOTE (windows): os/types_windows.go (this won't be fun...), or just
	// use filepath.EvalSymlinks
}

func (f *FileSelection) XInclude(path string, typ os.FileMode) bool {
	base := filepath.Base(path)
	if typ.IsDir() {
		return f.excludeDir.Len() == 0 || !f.excludeDir.MatchAny(base)
	}

	if f.exclude.MatchAny(base) {
		return false
	}
	// if no includes are specified match anything
	return f.include.Len() == 0 || f.include.MatchAny(base)
}

func (f *FileSelection) Include(path string) bool {
	base := filepath.Base(path)
	if f.exclude.MatchAny(base) {
		return false
	}
	// if no includes are specified match anything
	return f.include.Len() == 0 || f.include.MatchAny(base)
}

type Config struct {
	Recursive bool
	Sort      bool
	WriteJSON bool
	NoColor   bool
	Unique    bool

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
}

func main() {
	{
		set := flag.NewFlagSet(filepath.Base(os.Args[0]), flag.ExitOnError)
		var c Config
		c.AddFlags(set)
		set.Parse(os.Args[1:])
		return
	}

	globs := []string{
		"*abc*",
		"*a?c*",
		"*a[Ab]c*",
		"*a[Abc*",
	}
	for _, p := range globs {
		ok, err := filepath.Match(p, p)
		fmt.Printf("%s: %v - %v\n", p, ok, err)
	}
	return

	fmt.Println(time.Now().Format("01/02 15:04:05.00"))
	return

	const root = "/Users/charlie/Desktop/ditmars-logs/warp-drive/out_logs"
	reps := Walk(root)
	sort.Sort(byName(reps))
	var a []*Report
	for _, r := range reps {
		if r.Error > 0 && r.Valid > 0 {
			a = append(a, r)
		}
	}
	sort.Slice(a, func(i, j int) bool {
		return float64(a[i].Error)/float64(a[i].Total) < float64(a[j].Error)/float64(a[j].Total)
		// return a[i].Total-a[i].Error < a[j].Total-a[j].Error
	})
	for _, r := range a {
		name := strings.TrimLeft(strings.TrimPrefix(r.Name, root), "/")
		fmt.Printf("%s (%d -- %.2f): %d - %d\n", name, r.Total-r.Error, (float64(r.Error)/float64(r.Total))*100, r.Total, r.Error)
		// fmt.Printf("%s:\n", name)
		// fmt.Println("  Total:", r.Total)
		// fmt.Println("  Error:", r.Error)
		// fmt.Printf("  Valid: %d - %.2f\n", r.Valid, (float64(r.Valid)/float64(r.Total))*100)
	}
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
