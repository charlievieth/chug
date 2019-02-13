package main

import (
	"bytes"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charlievieth/chug/color"
)

type Printer struct {
	buf []byte
	w   io.Writer
}

func NewPrinter(w io.Writer) *Printer {
	return &Printer{
		w:   w,
		buf: make([]byte, 0, 128),
	}
}

func (p *Printer) reset()               { p.buf = p.buf[:0] }
func (p *Printer) write(b []byte)       { p.buf = append(p.buf, b...) }
func (p *Printer) writeString(s string) { p.buf = append(p.buf, s...) }
func (p *Printer) writeByte(c byte)     { p.buf = append(p.buf, c) }
func (p *Printer) clear()               { p.buf = append(p.buf, color.StyleDefault...) }

func (p *Printer) pad(n int) {
	const padding32 = "                                " // 32 spaces
	if n > len(padding32) {
		panic("Printer.pad: invalid pad length")
	}
	p.buf = append(p.buf, padding32[:n]...)
}

func (p *Printer) padStringColor(pad int, code, str string) {
	p.writeString(code)
	p.writeString(str)
	width := pad - utf8.RuneCountInString(str)
	if width > 0 {
		p.pad(width)
	}
	p.clear()
}

func (p *Printer) stringColor(code, str string) {
	p.writeString(code)
	p.writeString(str)
	p.clear()
}

func (p *Printer) indentBytes(pad int, s []byte) {
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

func (p *Printer) indentString(pad int, s string) {
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

func (p *Printer) indentStringColor(pad int, code, s string) {
	p.writeString(code)
	p.indentString(pad, s)
	p.clear()
}

const smallsString = "00010203040506070809" +
	"10111213141516171819" +
	"20212223242526272829" +
	"30313233343536373839" +
	"40414243444546474849" +
	"50515253545556575859" +
	"60616263646566676869" +
	"70717273747576777879" +
	"80818283848586878889" +
	"90919293949596979899"

func (p *Printer) itoaSmall(i int) {
	const nSmalls = 100

	if i >= nSmalls || i < 0 {
		panic("Printer.itoaSmall: digit must be between 0 and 99")
	}
	p.writeString(smallsString[i*2 : i*2+2])
}

func (p *Printer) formatNano(nanosec uint) {
	u := nanosec
	var buf [9]byte
	for start := len(buf); start > 0; {
		start--
		buf[start] = byte(u%10 + '0')
		u /= 10
	}
	p.writeByte('.')
	p.write(buf[:2])
}

func (p *Printer) formatDateColor(code string, t time.Time) {
	p.writeString(code)

	_, month, day := t.Date()
	p.itoaSmall(int(month))
	p.writeByte('/')
	p.itoaSmall(int(day))
	p.writeByte(' ')

	p.itoaSmall(t.Hour())
	p.writeByte(':')
	p.itoaSmall(t.Minute())
	p.writeByte(':')
	p.itoaSmall(t.Second())

	p.formatNano(uint(t.Nanosecond()))

	p.clear()
}

func (p *Printer) WriteTo(w io.Writer) (n int64, err error) {
	if nBytes := len(p.buf); nBytes > 0 {
		m, e := w.Write(p.buf)
		if m > nBytes {
			panic("Printer.WriteTo: invalid Write count")
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

func (p *Printer) EncodePretty(log *LogEntry) error {
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
	default:
		i := int(log.LogLevel)
		s := smallsString[i*2 : i*2+2]
		p.padStringColor(7, code, "["+s+"]")
	}
	p.writeByte(' ')
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
	if len(log.Data) != 0 {
		p.indentBytes(27, log.Data)
	}

	_, err := p.WriteTo(p.w)
	return err
}

// TODO: add colors for more sources
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

// TODO: Use random colors for unknown sources
/*
var availableColors [256]bool

func init() {
	// 16 <= i && i <= 231  --  ignore grayscale
	for i := 16; i <= 231; i++ {
		availableColors[i] = true
	}
	uglyavailableColors := []int{16, 17, 18, 19, 52, 53, 88}
	for _, i := range uglyavailableColors {
		availableColors[i] = false
	}
}

// "\x1b[38;5;33m"
var uniqueColorMap struct {
	sync.Mutex
	color map[string]string
}

func init() {
	panic("DO THIS SHIT!!")
}

func getColor(source string) string {
	uniqueColorMap.Lock()
	defer uniqueColorMap.Unlock()
	if s, ok := uniqueColorMap.color[source]; ok {
		return s
	}
	return ""
}
*/
