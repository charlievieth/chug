package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type LogLevel int

const (
	DEBUG = LogLevel(iota)
	INFO
	ERROR
	FATAL
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

func (l LogLevel) valid() bool { return 0 <= l && l <= FATAL }

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
	data = unquote(data)
	if len(data) == 1 {
		n := LogLevel(data[0] - '0')
		if DEBUG <= n && n <= FATAL {
			*l = n
			return nil
		}
		*l = 0
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
		*l = 0
		return errors.New(`invalid LogLevel: "` + string(data) + `"`)
	}
	return nil
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

type Entry struct {
	Log *LogEntry
	Raw []byte
}

func (e *Entry) Lager() bool { return e.Log != nil }

func ParseEntry(b []byte) (e Entry) {
	if !maybeJSON(b) {
		e.Raw = make([]byte, len(b))
		copy(e.Raw, b)
		return
	}

	var log CombinedFormat
	if err := json.Unmarshal(b, &log); err != nil {
		e.Raw = make([]byte, len(b))
		copy(e.Raw, b)
		return
	}

	e.Log = &LogEntry{
		Timestamp: log.Timestamp.Time(),
		LogLevel:  log.LogLevel,
		Source:    log.Source,
		Message:   log.Message,
		Data:      log.Data,
	}
	switch {
	case log.LogLevel == ERROR || log.LogLevel == FATAL:
		var data ErrorData
		if err := json.Unmarshal(e.Log.Data, &data); err == nil {
			e.Log.Session = data.Session
			e.Log.Trace = data.Trace
			e.Log.Error = data.Error
		}
	case bytes.Contains(log.Data, []byte(`"session"`)):
		var data SessionData
		if err := json.Unmarshal(e.Log.Data, &data); err == nil {
			e.Log.Session = data.Session
		}
	}
	// TODO: remove 'session', 'error' and 'trace' fields (especially trace)
	// TODO: nil data if empty

	return
}

var readerPool sync.Pool

func newReader(rd io.Reader) *Reader {
	if v := readerPool.Get(); v != nil {
		r := v.(*Reader)
		r.Reset(rd)
		return r
	}
	return &Reader{b: bufio.NewReaderSize(rd, 32*1014)}
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
	r.buf = append(r.buf, frag...)
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
	defer readerPool.Put(r)
	var ents []Entry
	var err error
	var buf []byte
	for err == nil {
		buf, err = r.ReadLine()
		// trim trailing newline
		if len(buf) >= 1 && buf[len(buf)-1] == '\n' {
			buf = buf[:len(buf)-1]
		}
		if len(buf) != 0 {
			ents = append(ents, ParseEntry(buf))
		}
	}
	if err != io.EOF {
		return nil, err
	}
	return ents, nil
}

type Data map[string]interface{}

// ErrorData is a reduced set of the fields typically stored in Data when
// there is an error - this is done to conserve memory.
type ErrorData struct {
	Session string `json:"session"`
	Trace   string `json:"trace"`
	Error   string `json:"error"`
}

// SessionData is used for extracting only the session field from Data -
// this is done to conserve memory.
type SessionData struct {
	Session string `json:"session"`
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
	data = unquote(data)
	if tt, ok := parseUnixTimestamp(data); ok {
		*t = Timestamp(tt)
		return nil
	}
	tt, err := time.Parse(time.RFC3339, string(data))
	*t = Timestamp(tt)
	return err
}

// unquote, returns the unquoted form of JSON value b
func unquote(b []byte) []byte {
	if len(b) >= 2 && b[0] == '"' || b[len(b)-1] == '"' {
		return b[1 : len(b)-1]
	}
	return b
}

type CombinedFormat struct {
	Timestamp Timestamp       `json:"timestamp"`
	Source    string          `json:"source"`
	Message   string          `json:"message"`
	LogLevel  LogLevel        `json:"level,log_level"`
	Data      json.RawMessage `json:"data"` // lazily parsed
	Error     error           `json:"-"`
}

func ParseFile(name string) ([]CombinedFormat, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var logs []CombinedFormat
	dec := json.NewDecoder(f)
	for {
		var log CombinedFormat
		err := dec.Decode(&log)
		if err != nil {
			if err == io.EOF {
				break // WARN: is the last log populated??
			}
			return nil, err
		}
		logs = append(logs, log)
	}
	return logs, nil
}

// type WalkFunc func(path string, info os.FileInfo, err error) error
/*
func Walk(root string) {
	// TODO: check if root is a file

	var (
		CountTotal int64
		CountErr   int64
		CountOk    int64
	)

	var wg sync.WaitGroup
	gate := make(chan struct{}, runtime.NumCPU()-1)
	filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if !fi.Mode().IsRegular() || !strings.HasSuffix(fi.Name(), ".log") {
			return nil
		}
		wg.Add(1)
		go func(path string) {
			gate <- struct{}{}
			defer func() { <-gate; wg.Done() }()
			atomic.AddInt64(&CountTotal, 1)
			_, err := ParseFile(path)
			if err != nil {
				base := strings.TrimLeft(strings.TrimPrefix(path, root), "/")
				fmt.Fprintf(os.Stderr, "%s: %s\n", base, err)
				atomic.AddInt64(&CountErr, 1)
			} else {
				atomic.AddInt64(&CountOk, 1)
			}
		}(path)
		return nil
	})

	fmt.Println("CountTotal:", CountTotal)
	fmt.Println("CountErr:", CountErr)
	fmt.Println("CountOk:", CountOk)
}
*/

type Walker struct {
	ents []Entry
	mu   sync.Mutex
}

func Worker(paths <-chan string, out chan<- []Entry, wg *sync.WaitGroup) {
	defer wg.Done()
	for path := range paths {
		ents, err := DecodeEntriesFile(path)
		if err != nil {
			continue
		}
		out <- ents
	}
}

func Walk(root string) []Entry {
	// TODO: check if root is a file

	// var (
	// 	CountTotal int64
	// 	CountErr   int64
	// 	CountOk    int64
	// )

	all := make([]Entry, 0, 256)
	out := make(chan []Entry)
	owg := new(sync.WaitGroup)
	owg.Add(1)
	go func() {
		defer owg.Done()
		for ents := range out {
			all = append(all, ents...)
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

	// fmt.Println("CountTotal:", CountTotal)
	// fmt.Println("CountErr:", CountErr)
	// fmt.Println("CountOk:", CountOk)

	return all
}

func main() {
	if len(os.Args) != 2 {
		Fatal("USAGE: PATH")
	}
	ents := Walk(os.Args[1])
	fmt.Println("Entries:", len(ents))

	// data := []byte(`{"level": 3, "log_level": "info"}`)
	// var f CombinedFormat
	// if err := json.Unmarshal(data, &f); err != nil {
	// 	Fatal(err)
	// }
	// fmt.Println(f.LogLevel)

	// b, err := json.Marshal("debug")
	// if err != nil {
	// 	Fatal(err)
	// }
	// var lv LogLevel
	// if err := json.Unmarshal(b, &lv); err != nil {
	// 	Fatal(err)
	// }
	// fmt.Println(lv)

	// data := []byte(`{"timestamp": "1520986770.534132957", "time": "2018-03-13T20:19:30-04:00"}`)
	// var m map[string]Timestamp
	// if err := json.Unmarshal(data, &m); err != nil {
	// 	Fatal(err)
	// }
	// ts := time.Time(m["timestamp"])
	// tt := time.Time(m["time"])
	// fmt.Println(ts)
	// fmt.Println(tt)
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

func extractStringValue(m map[string]interface{}, key string) string {
	if v := m[key]; v != nil {
		if s, ok := v.(string); ok {
			delete(m, key)
			return s
		}
	}
	return ""
}

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

type logByTime []LogEntry

func (e logByTime) Len() int           { return len(e) }
func (e logByTime) Less(i, j int) bool { return e[i].Timestamp.Before(e[j].Timestamp) }
func (e logByTime) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

type entryByTime []Entry

func (e entryByTime) Len() int           { return len(e) }
func (e entryByTime) Less(i, j int) bool { return e[i].Log.Timestamp.Before(e[j].Log.Timestamp) }
func (e entryByTime) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
