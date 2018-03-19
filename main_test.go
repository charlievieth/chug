package main

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"testing"
)

var RepLogData *bytes.Reader

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
	data, err := ioutil.ReadAll(gz)
	if err != nil {
		panic(err)
	}
	if err := gz.Close(); err != nil {
		panic(err)
	}
	RepLogData = bytes.NewReader(data)
}

func BenchmarkDecode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		RepLogData.Seek(0, 0)
		if _, err := DecodeEntries(RepLogData); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeFast(b *testing.B) {
	for i := 0; i < b.N; i++ {
		RepLogData.Seek(0, 0)
		if _, err := DecodeEntriesFast(RepLogData); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWalk(b *testing.B) {
	const root = "/Users/charlie/Desktop/ditmars-logs/warp-drive/out_logs"
	// const root = "/Users/charlie/Desktop/ditmars-logs/warp-drive/out_logs/diego-cell_0ff998a2-1b9e-4182-82f9-f8dbb5f844b6/rep"
	for i := 0; i < b.N; i++ {
		Walk(root)
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
