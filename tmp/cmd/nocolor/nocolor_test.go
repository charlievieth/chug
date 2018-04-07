package main

import (
	"bytes"
	"encoding/base64"
	"io/ioutil"
	"testing"
)

var TestReader *bytes.Reader

func init() {
	const name = "../../../testdata/test.json"
	b, err := ioutil.ReadFile(name)
	if err != nil {
		panic(err)
	}
	TestReader = bytes.NewReader(b)
}

func TestStripColor(t *testing.T) {
	// base64 encoded log lines (saves the trouble of dealing with JSON and
	// ANSI escape sequences)

	const NoColorLogLine = "eyJ0aW1lc3RhbXAiOiIxNTIwOTgzODAyLjQyOTA0NjE1NCIsInNvdXJjZSI6InJl" +
		"cCIsIm1lc3NhZ2UiOiJyZXAuY29udGFpbmVyLW1ldHJpY3MtcmVwb3J0ZXIudGljay5nZXQtYWxsLW1ldHJ" +
		"pY3MuY29udGFpbmVyc3RvcmUtbWV0cmljcy5zdGFydGluZyIsImxvZ19sZXZlbCI6MSwiZGF0YSI6eyJzZX" +
		"NzaW9uIjoiOS41NzI0LjEuMSJ9fQo="

	const ColorLogLine = "eyJ0aW1lc3RhbXAiOiIxNTIwOTgzODAyLjQyOTA0NjE1NCIsInNvdXJjZSI6IhtbMD" +
		"E7MzFtG1tLcmVwG1ttG1tLIiwibWVzc2FnZSI6IhtbMDE7MzFtG1tLcmVwG1ttG1tLLmNvbnRhaW5lci1tZ" +
		"XRyaWNzLRtbMDE7MzFtG1tLcmVwG1ttG1tLb3J0ZXIudGljay5nZXQtYWxsLW1ldHJpY3MuY29udGFpbmVy" +
		"c3RvcmUtbWV0cmljcy5zdGFydGluZyIsImxvZ19sZXZlbCI6MSwiZGF0YSI6eyJzZXNzaW9uIjoiOS41NzI" +
		"0LjEuMSJ9fQo="

	expected, err := base64.StdEncoding.DecodeString(NoColorLogLine)
	if err != nil {
		t.Fatal(err)
	}
	color, err := base64.StdEncoding.DecodeString(ColorLogLine)
	if err != nil {
		t.Fatal(err)
	}
	x := StripColor([]byte(color))
	if !bytes.Equal(expected, x) {
		t.Errorf("StripColor2:\nGot:  %s\nWant: %s\n", string(x), string(expected))
	}
}

func BenchmarkReadlines(b *testing.B) {
	for i := 0; i < b.N; i++ {
		TestReader.Seek(0, 0)
		if _, err := Readlines(TestReader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadlinesNoColor(b *testing.B) {
	for i := 0; i < b.N; i++ {
		TestReader.Seek(0, 0)
		if _, err := ReadlinesNoColor(TestReader); err != nil {
			b.Fatal(err)
		}
	}
}
