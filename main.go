package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	if len(data) == 1 && data[0] >= '0' {
		switch LogLevel(data[0] - '0') {
		case DEBUG:
			*l = DEBUG
		case INFO:
			*l = INFO
		case ERROR:
			*l = ERROR
		case FATAL:
			*l = FATAL
		default:
			*l = 0
			return errors.New(`invalid LogLevel: "` + string(data) + `"`)
		}
		return nil
	}
	switch string(data) {
	case "debug", "DEBUG":
		*l = DEBUG
	case "info", "INFO":
		*l = INFO
	case "error", "ERROR":
		*l = ERROR
	case "fatal", "FATAL":
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
	Error     string // used to error
	Trace     string
	Data      Data
}

type Entry struct {
	Raw   []byte
	Log   LogEntry
	Lager bool
}

func extractStringValue(m map[string]interface{}, key string) string {
	if v := m[key]; v != nil {
		if s, ok := v.(string); ok {
			delete(m, key)
			return s
		}
	}
	return ""
}

func ParseEntry(b []byte) (e Entry) {
	e.Raw = make([]byte, len(b))
	copy(e.Raw, b)
	if !maybeJSON(b) {
		return
	}

	var log CombinedFormat
	if err := json.Unmarshal(b, &log); err != nil {
		return
	}

	data := log.Data
	e.Log = LogEntry{
		Timestamp: log.Timestamp.Time(),
		LogLevel:  log.LogLevel,
		Source:    log.Source,
		Message:   log.Message,
		Session:   extractStringValue(data, "session"),
		Trace:     extractStringValue(data, "trace"),
		Data:      data,
	}
	if log.LogLevel == ERROR || log.LogLevel == FATAL {
		e.Log.Error = extractStringValue(data, "error")
	}
	// TODO: nil data if empty

	return
}

type Data map[string]interface{}

type Timestamp time.Time

func (t Timestamp) Time() time.Time              { return time.Time(t) }
func (t Timestamp) String() string               { return time.Time(t).String() }
func (t Timestamp) MarshalJSON() ([]byte, error) { return time.Time(t).MarshalJSON() }

func (t *Timestamp) UnmarshalJSON(data []byte) error {
	// Ignore null, like in the main JSON package.
	if string(data) == "null" {
		return nil
	}
	data = unquote(data)
	if isFloat(data) {
		f, err := strconv.ParseFloat(string(data), 64)
		*t = Timestamp(time.Unix(0, int64(f*1e9)))
		return err
	}
	tt, err := time.Parse(time.RFC3339, string(data))
	*t = Timestamp(tt)
	return err
}

func isFloat(b []byte) bool {
	dot := uint(0)
	for _, c := range b {
		switch {
		case '0' <= c && c <= '9':
			// ok
		case c == '.':
			dot++
		default:
			return false
		}
	}
	return len(b) != 0 && dot <= 1
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

// unquote, returns the unquoted form of JSON value b
func unquote(b []byte) []byte {
	if len(b) >= 2 && b[0] == '"' || b[len(b)-1] == '"' {
		return b[1 : len(b)-1]
	}
	return b
}

type CombinedFormat struct {
	Timestamp Timestamp `json:"timestamp"`
	Source    string    `json:"source"`
	Message   string    `json:"message"`
	LogLevel  LogLevel  `json:"level,log_level"`
	Data      Data      `json:"data"`
	Error     error     `json:"-"`
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

func main() {
	if len(os.Args) != 2 {
		Fatal("USAGE: PATH")
	}
	Walk(os.Args[1])

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
