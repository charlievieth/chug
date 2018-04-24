package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/charlievieth/chug/util"
)

// TODO: delete if nothing uses it
type Data map[string]interface{}

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

var pow10tab = [...]int64{1e00, 1e01, 1e02, 1e03, 1e04, 1e05, 1e06, 1e07, 1e08, 1e09}

func parseUnixTimestamp(b []byte) (time.Time, bool) {
	// N.B. I was a bored when I wrote this so its a bit over-optimized.
	var (
		dot  int
		sec  int64
		nsec int64
	)
	p := &sec
	if len(b) == 0 || !('1' <= b[0] && b[0] <= '9') {
		goto Error
	}
	for i, c := range b {
		switch {
		case '0' <= c && c <= '9':
			*p = *p*10 + int64(c-'0')
		case c == '.' && dot == 0:
			dot = i
			p = &nsec
		default:
			goto Error
		}
	}
	if dot == len(b)-1 {
		goto Error // trailing dot: "123."
	}
	// convert nsec fraction to nanoseconds
	dot = 9 - (len(b) - (dot + 1))
	if dot < 0 {
		goto Error
	}
	if dot > 0 {
		nsec *= pow10tab[dot]
	}
	return time.Unix(sec, nsec), true

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
	Timestamp Timestamp `json:"timestamp"`
	Source    string    `json:"source"`
	Message   string    `json:"message"`
	LevelV1   LogLevel  `json:"log_level"`
	LevelV2   LogLevel  `json:"level"`

	// We only care about the 'session', 'error' and 'trace' fields,
	// which are strings.  By using type map[string]json.RawMessage
	// we avoid parsing more complex fields.
	Data map[string]json.RawMessage `json:"data,omitempty"`

	// These fields are added by chug when combining logs.
	Session string `json:"session,omitempty"`
	Error   string `json:"error,omitempty"`
	Trace   string `json:"trace,omitempty"`
}

func (c *CombinedFormat) LogLevel() LogLevel {
	if c.LevelV1 >= c.LevelV2 {
		return c.LevelV1
	}
	return c.LevelV2
}

type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	LogLevel  LogLevel  `json:"log_level"`
	Source    string    `json:"source"`
	Message   string    `json:"message"`

	// The 'session', 'error' and 'trace' fields are extracted from the 'data'
	// field of the lager log message.
	Session string `json:"session,omitempty"`
	Error   string `json:"error,omitempty"`
	Trace   string `json:"trace,omitempty"`

	// The JSON encoded lager 'data' field with the 'session', 'error' and
	// 'trace' fields removed.
	Data json.RawMessage `json:"data,omitempty"`
}

func (l *LogEntry) Equal(e *LogEntry) bool {
	return l.Timestamp.Equal(e.Timestamp) &&
		l.LogLevel == e.LogLevel &&
		l.Source == e.Source &&
		l.Message == e.Message &&
		l.Session == e.Session &&
		l.Error == e.Error &&
		l.Trace == e.Trace &&
		bytes.Equal(l.Data, e.Data)
}

func extractStringValue(m map[string]json.RawMessage, key string) string {
	if b, ok := m[key]; ok {
		delete(m, key)
		if len(b) > 2 {
			if ub, ok := unquoteBytes(b); ok && len(ub) != 0 {
				return string(ub)
			}
		}
	}
	return ""
}

func valueOrDefault(value, def string) string {
	if value != "" {
		return value
	}
	return def
}

// TODO: this should only be used when handling a very large number of entries
func ParseLogEntry(b []byte) (*LogEntry, error) {
	var log CombinedFormat
	if err := json.Unmarshal(b, &log); err != nil {
		return nil, err
	}
	ent := &LogEntry{
		Timestamp: log.Timestamp.Time(),
		LogLevel:  log.LogLevel(),
		Source:    log.Source,
		Message:   log.Message,
		Session:   valueOrDefault(log.Session, extractStringValue(log.Data, "session")),
		Trace:     valueOrDefault(log.Trace, extractStringValue(log.Data, "trace")),
		Error:     valueOrDefault(log.Error, extractStringValue(log.Data, "error")),
	}
	if len(log.Data) != 0 {
		// This is a significant source of memory usage.  Go allocates
		// more than we need.  By copying to a slice of the exact size
		// we reduce memory consumption by ~30%.  This is particularly
		// important when handling large log sets.
		buf := util.NewBuffer()
		if err := json.NewEncoder(buf).Encode(log.Data); err != nil {
			util.PutBuffer(buf)
			return nil, err
		}
		buf.UnreadByte() // remove new line
		ent.Data = make([]byte, buf.Len())
		copy(ent.Data, buf.Bytes())
		util.PutBuffer(buf)
	}
	return ent, nil
}

type LogEntryDecoder struct {
	buf          bytes.Buffer
	enc          *json.Encoder
	indentPrefix string
	indentValue  string
}

func (p *LogEntryDecoder) lazyInit() {
	if p.enc == nil {
		p.enc = json.NewEncoder(&p.buf)
		p.enc.SetEscapeHTML(false)
	}
}

func (p *LogEntryDecoder) SetIndent(prefix, indent string) {
	p.lazyInit()
	p.indentPrefix = prefix
	p.indentValue = indent
	p.enc.SetIndent(prefix, indent)
}

func (p *LogEntryDecoder) Decode(b []byte) (*LogEntry, error) {
	p.lazyInit()
	var log CombinedFormat
	if err := json.Unmarshal(b, &log); err != nil {
		return nil, err
	}
	ent := &LogEntry{
		Timestamp: log.Timestamp.Time(),
		LogLevel:  log.LogLevel(),
		Source:    log.Source,
		Message:   log.Message,
		Session:   valueOrDefault(log.Session, extractStringValue(log.Data, "session")),
		Trace:     valueOrDefault(log.Trace, extractStringValue(log.Data, "trace")),
		Error:     valueOrDefault(log.Error, extractStringValue(log.Data, "error")),
	}
	if len(log.Data) != 0 {
		p.buf.Reset()
		if err := p.enc.Encode(log.Data); err != nil {
			// The json.Encoder's internall err field is only set on write
			// errors - so we don't need to create a new one.
			return nil, err
		}
		p.buf.UnreadByte() // Remove newline
		ent.Data = make([]byte, p.buf.Len())
		copy(ent.Data, p.buf.Bytes())
	}
	return ent, nil
}

type logByTime []*LogEntry

func (e logByTime) Len() int           { return len(e) }
func (e logByTime) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e logByTime) Less(i, j int) bool { return e[i].Timestamp.Before(e[j].Timestamp) }

// CEV: Idea for lightweight sorting and parsing
// type MinimalLog struct {
// 	Timestamp Timestamp         `json:"timestamp"`
// 	Data      []json.RawMessage `json:"-"`
// }
