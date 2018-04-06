package main

import (
	"bytes"
	"io/ioutil"
	"testing"
)

var TestReader *bytes.Reader

func init() {
	const name = "/Users/charlie/go/src/github.com/charlievieth/chug/testdata/test.json"
	b, err := ioutil.ReadFile(name)
	if err != nil {
		panic(err)
	}
	TestReader = bytes.NewReader(b)
}

func TestStripColor2(t *testing.T) {
	TestReader.Seek(0, 0)
	b, _ := ioutil.ReadAll(TestReader)
	line := bytes.Split(b, []byte{'\n'})[0]

	expected := StripColor(line)
	x := StripColor2(line)
	if !bytes.Equal(expected, x) {
		t.Errorf("StripColor2:\nGot:  %s\nWant: %s\n", string(x), string(expected))
	}
}

func TestStripColor3(t *testing.T) {
	TestReader.Seek(0, 0)
	b, _ := ioutil.ReadAll(TestReader)
	line := bytes.Split(b, []byte{'\n'})[0]

	expected := StripColor(line)
	x := StripColor3(line)
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
