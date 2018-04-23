package xz

import (
	"bytes"
	"io"
	"io/ioutil"
	"os/exec"
	"testing"
)

var xzNotFound bool
var testData []byte

func init() {
	var err error
	testData, err = ioutil.ReadFile("testdata/bench.log.xz")
	if err != nil {
		panic("setup failed: " + err.Error())
	}
}

func init() {
	_, err := exec.LookPath("xz")
	xzNotFound = (err == nil)
}

func TestXZ(t *testing.T) {
	if !xzNotFound {
		t.Skip("xz binary required for tests")
	}
	if err := Validate(); err != nil {
		t.Error("Validation failed:", err)
	}
	if !Enabled() {
		t.Error("Expected Enabled() to be true")
	}
}

func BenchmarkXZ(b *testing.B) {
	if !xzNotFound {
		b.Skip("xz binary required for tests")
	}
	var w nopWriter
	rd := bytes.NewReader(testData)
	buf := make([]byte, 32*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rd.Reset(testData)
		r, err := NewReader(rd)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.CopyBuffer(w, r, buf); err != nil {
			b.Fatal(err)
		}
	}
}

// TODO: remove ~4% gain is not worth it
func BenchmarkXZ_File(b *testing.B) {
	if !xzNotFound {
		b.Skip("xz binary required for tests")
	}
	var w nopWriter
	buf := make([]byte, 32*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := NewFileReader("testdata/bench.log.xz")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.CopyBuffer(w, r, buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXZParallel(b *testing.B) {
	if !xzNotFound {
		b.Skip("xz binary required for tests")
	}
	b.RunParallel(func(pb *testing.PB) {
		var w nopWriter
		rd := bytes.NewReader(testData)
		buf := make([]byte, 32*1024)
		for pb.Next() {
			rd.Reset(testData)
			r, err := NewReader(rd)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.CopyBuffer(w, r, buf); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TODO: remove ~4% gain is not worth it
func BenchmarkXZParallel_File(b *testing.B) {
	if !xzNotFound {
		b.Skip("xz binary required for tests")
	}
	b.RunParallel(func(pb *testing.PB) {
		var w nopWriter
		buf := make([]byte, 32*1024)
		for pb.Next() {
			r, err := NewFileReader("testdata/bench.log.xz")
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.CopyBuffer(w, r, buf); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
