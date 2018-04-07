package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"

	"github.com/charlievieth/chug/util"
)

func Readlines(rd io.Reader) (int, error) {
	var lines int
	var err error
	r := newReader(rd)
	for {
		b, e := r.ReadLine()
		if len(b) != 0 {
			lines++
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	putReader(r)
	return lines, err
}

func ReadlinesNoColor(rd io.Reader) (int, error) {
	var lines int
	var err error
	r := newReader(rd)
	for {
		b, e := r.ReadLine()
		b = StripColor(b)
		// b = StripColor2(b)
		if len(b) != 0 {
			lines++
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	putReader(r)
	return lines, err
}

var colorRe = regexp.MustCompile("\x1b\\[[0-?]*[ -/]*[@-~]")

func StripColor(b []byte) []byte {
	if bytes.IndexByte(b, '\x1b') == -1 {
		return b
	}
	return colorRe.ReplaceAllFunc(b, func(_ []byte) []byte {
		return nil
	})
}

func colorless(rd io.Reader, out io.Writer) error {
	var err error
	r := newReader(rd)
	for {
		b, e := r.ReadLine()
		b = StripColor(b)
		if len(b) != 0 {
			out.Write(append(b, '\n'))
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	putReader(r)
	return err
}

func xmain() {
	const name = "../../../testdata/test.json"
	b, err := ioutil.ReadFile(name)
	if err != nil {
		panic(err)
	}
	line := bytes.Split(b, []byte{'\n'})[0]

	x := colorRe.FindAllIndex(line, -1)
	fmt.Println(x)
}

func main() {
	{
		xmain()
		return
	}
	if err := colorless(os.Stdin, os.Stdout); err != nil {
		Fatal(err)
	}
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

var readerPool sync.Pool

func newReader(rd io.Reader) *util.Reader {
	if v := readerPool.Get(); v != nil {
		r := v.(*util.Reader)
		r.Reset(rd)
		return r
	}
	return util.NewReader(rd)
}

func putReader(r *util.Reader) {
	r.Reset(nil) // Remove reference
	readerPool.Put(r)
}
