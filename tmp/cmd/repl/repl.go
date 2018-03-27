package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func main() {
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
