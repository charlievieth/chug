package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/charlievieth/chug/util"
	"github.com/charlievieth/chug/walk"
)

var RepLogReader *bytes.Reader
var RepLogData []byte

func init() {
	f, err := os.Open("testdata/rep.out.log.gz")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		panic(err)
	}
	RepLogData, err = ioutil.ReadAll(gz)
	if err != nil {
		panic(err)
	}
	if err := gz.Close(); err != nil {
		panic(err)
	}
	RepLogReader = bytes.NewReader(RepLogData)
}

func BenchmarkDecode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		RepLogReader.Seek(0, 0)
		if _, err := DecodeEntries(RepLogReader); err != nil {
			b.Fatal(err)
		}
	}
}

// func BenchmarkDecodeFast(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		RepLogReader.Seek(0, 0)
// 		if _, err := DecodeEntriesFast(RepLogReader); err != nil {
// 			b.Fatal(err)
// 		}
// 	}
// }

func BenchmarkWalk(b *testing.B) {
	const root = "/Users/charlie/Desktop/ditmars-logs/warp-drive/out_logs"
	for i := 0; i < b.N; i++ {
		var w Walker
		if err := walk.Walk(root, w.Walk); err != nil {
			b.Fatal(err)
		}
		w.ents = nil
	}
}

func BenchmarkLogLevelUnmarshalJSON(b *testing.B) {
	data := []byte(`"fatal"`)
	var ll LogLevel
	for i := 0; i < b.N; i++ {
		if err := ll.UnmarshalJSON(data); err != nil {
			b.Fatal(err)
		}
	}
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

var LogEntries []Entry
var initLogEntriesOnce sync.Once

// TODO: delete this test!
func BenchmarkPrint(b *testing.B) {
	initLogEntriesOnce.Do(func() {
		RepLogReader.Seek(0, 0)
		ents, err := DecodeEntries(RepLogReader)
		if err != nil {
			b.Fatal(err)
		}
		LogEntries = ents
	})
	p := Printer{w: nopWriter{}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, e := range LogEntries {
			if err := p.EncodePretty(e.Log); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkEncodePretty(b *testing.B) {
	log := &LogEntry{
		Timestamp: time.Date(2018, 05, 10, 13, 01, 02, 03, time.UTC),
		LogLevel:  INFO,
		Source:    "rep",
		Message:   "rep.running-bulker.sync.batch-operations.executing-container-operation.starting",
		Session:   "14.2865.1.44",
		Data:      []byte(`{"container-guid": "278d8c4b-3929-455c-4278-42e9"}`),
	}
	p := Printer{w: nopWriter{}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.EncodePretty(log)
	}
}

func benchReadLines() (int, error) {
	RepLogReader.Seek(0, 0)
	r := util.NewReader(RepLogReader)
	var lines int
	var err error
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
	return lines, err
}

func benchCandidateLines(re *regexp.Regexp) (int, error) {
	RepLogReader.Seek(0, 0)
	r := util.NewReader(RepLogReader)
	var err error
	var lines int
	for {
		b, e := r.ReadLine()
		if len(b) != 0 && re.Match(b) {
			lines++
		}
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
	}
	return lines, err
}

func BenchmarkReadLines(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := benchReadLines(); err != nil {
			b.Fatal(err)
		}
	}
}

// var candidateRe = regexp.MustCompile(`"(log_)?level":\s*(1|"info")`)
var candidateRe = regexp.MustCompile(`level":\s*(?:[123]|"(info|error|fatal)")`)

func BenchmarkCandidate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := benchCandidateLines(candidateRe); err != nil {
			b.Fatal(err)
		}
	}
}
