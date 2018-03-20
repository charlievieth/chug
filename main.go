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
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/charlievieth/chug/color"
)

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

func extractStringValue(m map[string]json.RawMessage, key string) string {
	var s string
	b, ok := m[key]
	if ok {
		delete(m, key)
		if len(b) > 2 {
			if ub, ok := unquoteBytes(b); ok && len(ub) != 0 {
				s = string(ub)
			}
		}
	}
	return s
}

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
		Session:   extractStringValue(log.Data, "session"),
		Trace:     extractStringValue(log.Data, "trace"),
		Error:     extractStringValue(log.Data, "error"),
	}
	if len(log.Data) != 0 {
		b, err := json.Marshal(log.Data)
		if err != nil {
			return nil, err
		}
		ent.Data = b
	}
	return ent, nil
}

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
		defer readerPool.Put(r)
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
	data = unquoteNoEscape(data)
	if tt, ok := parseUnixTimestamp(data); ok {
		*t = Timestamp(tt)
		return nil
	}
	tt, err := time.Parse(time.RFC3339, string(data))
	*t = Timestamp(tt)
	return err
}

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

type Printer struct {
	w   io.Writer
	buf bytes.Buffer
}

func SourceColor(source string) string {
	n := strings.IndexByte(source, ':')
	if n != -1 {
		source = source[:n]
	}
	switch source {
	case "auctioneer":
		return "\x1b[95m"
	case "bbs":
		return "\x1b[36m"
	case "converger":
		return "\x1b[94m"
	case "executor":
		return "\x1b[92m"
	case "file-server":
		return "\x1b[34m"
	case "garden-linux":
		return "\x1b[35m"
	case "loggregator":
		return "\x1b[33m"
	case "nsync-listener":
		return "\x1b[98m"
	case "rep":
		return "\x1b[93m"
	case "route-emitter":
		return "\x1b[96m"
	case "router":
		return "\x1b[32m"
	case "tps":
		return "\x1b[97m"
	default:
		return "\x1b[0m"
	}
}

func (p *Printer) reset() { p.buf.Reset() }

func (p *Printer) padString(pad int, s string) {
	p.buf.WriteString(s)
	width := pad - utf8.RuneCountInString(s)
	if width > 0 {
		for i := 0; i < width; i++ {
			p.buf.WriteByte(' ')
		}
	}
}

func (p *Printer) padStringColor(pad int, code, str string) {
	p.buf.WriteString(code)
	p.buf.WriteString(str)
	width := pad - utf8.RuneCountInString(str)
	if width > 0 {
		for i := 0; i < width; i++ {
			p.buf.WriteByte(' ')
		}
	}
	p.buf.WriteString(color.StyleDefault)
}

func (p *Printer) stringColor(code, str string) {
	p.buf.WriteString(code)
	p.buf.WriteString(str)
	p.buf.WriteString(color.StyleDefault)
}

func (p *Printer) indentBytes(pad int, s []byte) {
	padding := make([]byte, pad) // TODO: this is wasteful - fix
	for i := range padding {
		padding[i] = ' '
	}
	for {
		n := bytes.IndexByte(s, '\n')
		if n < 0 {
			break
		}
		p.buf.Write(padding)
		p.buf.Write(s[:n+1]) // include newline
		s = s[n+1:]
	}
	p.buf.Write(padding)
	p.buf.Write(s) // WARN: make sure this is right
	p.buf.WriteByte('\n')
}

func (p *Printer) indentString(pad int, s string) {
	padding := make([]byte, pad) // TODO: this is wasteful - fix
	for i := range padding {
		padding[i] = ' '
	}
	for {
		n := strings.IndexByte(s, '\n')
		if n < 0 {
			break
		}
		p.buf.Write(padding)
		p.buf.WriteString(s[:n+1]) // include newline
		s = s[n+1:]
	}
	p.buf.Write(padding)
	p.buf.WriteString(s) // WARN: make sure this is right
	p.buf.WriteByte('\n')
}

func (p *Printer) indentStringColor(pad int, code, s string) {
	p.buf.WriteString(code)
	p.indentString(pad, s)
	p.buf.WriteString(color.StyleDefault)
}

func (p *Printer) Pretty(log *LogEntry) error {
	p.reset()
	code := SourceColor(log.Source)
	p.padStringColor(16, code, log.Source)
	p.buf.WriteByte(' ')
	switch log.LogLevel {
	case DEBUG:
		p.padStringColor(7, color.ColorGray, "[DEBUG]")
	case INFO:
		p.padStringColor(7, code, "[INFO]")
	case ERROR:
		p.padStringColor(7, color.ColorRed, "[ERROR]")
	case FATAL:
		p.padStringColor(7, color.ColorRed, "[FATAL]")
	}
	p.buf.WriteByte(' ')
	p.stringColor(code, log.Timestamp.Format("01/02 15:04:05.00"))
	p.buf.WriteByte(' ')
	p.padStringColor(10, color.ColorGray, log.Session)
	p.buf.WriteByte(' ')
	p.stringColor(code, log.Message)
	p.buf.WriteByte('\n')

	if log.Error != "" {
		p.indentStringColor(27, color.ColorRed, log.Error)
	}
	if log.Trace != "" {
		p.indentStringColor(27, color.ColorRed, log.Trace)
	}
	// TODO: strip 'session', 'error' and 'trace'
	if len(log.Data) != 0 {
		p.indentBytes(27, log.Data)
	}

	_, err := p.buf.WriteTo(p.w)
	return err
}

type xprinter struct {
	w   io.Writer
	buf []byte
	// padding []byte // padding
	padding [32]byte // padding
}

func (p *xprinter) reset()               { p.buf = p.buf[0:0] }
func (p *xprinter) writeByte(c byte)     { p.buf = append(p.buf, c) }
func (p *xprinter) write(b []byte)       { p.buf = append(p.buf, b...) }
func (p *xprinter) writeString(s string) { p.buf = append(p.buf, s...) }
func (p *xprinter) clear()               { p.buf = append(p.buf, color.StyleDefault...) }

func (p *xprinter) initPad(n int) {
	// if len(p.padding) < n {
	if p.padding[0] == 0 {
		// if n < 32 {
		// 	n = 32
		// }
		// p.padding = make([]byte, n)
		for i := 0; i < len(p.padding); i++ {
			p.padding[i] = ' '
		}
	}
}

func (p *xprinter) pad(n int) {
	if n > len(p.padding) {
		panic("WTF")
	}
	if p.padding[0] == 0 {
		// if len(p.padding) < n {
		p.initPad(n)
	}
	p.write(p.padding[:n])
}

func (p *xprinter) padStringColor(pad int, code, str string) {
	p.writeString(code)
	p.writeString(str)
	width := pad - utf8.RuneCountInString(str)
	if width > 0 {
		p.pad(width)
	}
	p.clear()
}

func (p *xprinter) stringColor(code, str string) {
	p.writeString(code)
	p.writeString(str)
	p.clear()
}

func (p *xprinter) indentBytes(pad int, s []byte) {
	for {
		n := bytes.IndexByte(s, '\n')
		if n < 0 {
			break
		}
		p.pad(pad)
		p.write(s[:n+1]) // include newline
		s = s[n+1:]
	}
	p.pad(pad)
	p.write(s) // WARN: make sure this is right
	p.writeByte('\n')
}

func (p *xprinter) indentString(pad int, s string) {
	for {
		n := strings.IndexByte(s, '\n')
		if n < 0 {
			break
		}
		p.pad(pad)
		p.buf = append(p.buf, s[:n+1]...) // include newline
		s = s[n+1:]
	}
	p.pad(pad)
	p.writeString(s) // WARN: make sure this is right
	p.writeByte('\n')
}

func (p *xprinter) indentStringColor(pad int, code, s string) {
	p.writeString(code)
	p.indentString(pad, s)
	p.clear()
}

func (p *xprinter) WriteTo(w io.Writer) (n int64, err error) {
	if nBytes := len(p.buf); nBytes > 0 {
		m, e := w.Write(p.buf)
		if m > nBytes {
			panic("xprinter.WriteTo: invalid Write count")
		}
		p.reset() // reset printer
		n := int64(m)
		if e != nil {
			return n, e
		}
		if m != nBytes {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}

func (p *xprinter) writeDigit(n int) {
	if n < 10 {
		p.writeByte('0')
	}
	p.buf = strconv.AppendInt(p.buf, int64(n), 10)
}

func formatNano(b []byte, nanosec uint) []byte {
	u := nanosec
	var buf [9]byte
	for start := len(buf); start > 0; {
		start--
		buf[start] = byte(u%10 + '0')
		u /= 10
	}
	b = append(b, '.')
	return append(b, buf[:2]...)
}

func (p *xprinter) formatDateColor(code string, t time.Time) {
	p.writeString(code)

	_, month, day := t.Date()
	p.writeDigit(int(month))
	p.writeByte('/')
	p.writeDigit(int(day))
	p.writeByte(' ')

	p.writeDigit(t.Hour())
	p.writeByte(':')
	p.writeDigit(t.Minute())
	p.writeByte(':')
	p.writeDigit(t.Second())
	p.buf = formatNano(p.buf, uint(t.Nanosecond()))

	p.clear()
}

func (p *xprinter) Pretty(log *LogEntry) error {
	p.reset()
	code := SourceColor(log.Source)
	p.padStringColor(16, code, log.Source)
	p.writeByte(' ')
	switch log.LogLevel {
	case DEBUG:
		p.padStringColor(7, color.ColorGray, "[DEBUG]")
	case INFO:
		p.padStringColor(7, code, "[INFO]")
	case ERROR:
		p.padStringColor(7, color.ColorRed, "[ERROR]")
	case FATAL:
		p.padStringColor(7, color.ColorRed, "[FATAL]")
	}
	p.writeByte(' ')
	// p.stringColor(code, log.Timestamp.Format("01/02 15:04:05.00"))
	p.formatDateColor(code, log.Timestamp)
	p.writeByte(' ')
	p.padStringColor(10, color.ColorGray, log.Session)
	p.writeByte(' ')
	p.stringColor(code, log.Message)
	p.writeByte('\n')

	if log.Error != "" {
		p.indentStringColor(27, color.ColorRed, log.Error)
	}
	if log.Trace != "" {
		p.indentStringColor(27, color.ColorRed, log.Trace)
	}
	// TODO: strip 'session', 'error' and 'trace'
	if len(log.Data) != 0 {
		p.indentBytes(27, log.Data)
	}

	_, err := p.WriteTo(p.w)
	return err
}

func main() {

	if len(os.Args) != 2 {
		Fatal("USAGE: PATH")
	}
	// p := Printer{w: os.Stdout}
	p := xprinter{w: os.Stdout}
	ents := Walk(os.Args[1])
	for _, e := range ents {
		if err := p.Pretty(e.Log); err != nil {
			Fatal(err)
		}
	}

	// enc := json.NewEncoder(os.Stdout)
	// for _, e := range ents {
	// 	if err := enc.Encode(e.Log); err != nil {
	// 		Fatal(err)
	// 	}
	// }
	// fmt.Println("Entries:", len(ents))

	// data := []byte(`{"level": "fatal"}`)
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
