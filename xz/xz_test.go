package xz

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

var xzNotFound bool
var testData []byte

func init() {
	_, err := exec.LookPath("xz")
	xzNotFound = (err != nil)
	if xzNotFound {
		return
	}

	testData, err = ioutil.ReadFile("testdata/bench.log.xz")
	if err != nil {
		panic("setup failed: " + err.Error())
	}
}

func TestValidate(t *testing.T) {
	if xzNotFound {
		t.Skip("xz required for tests")
	}
	if err := Validate(); err != nil {
		t.Error("Validation failed:", err)
	}
	if !Enabled() {
		t.Error("Expected Enabled() to be true")
	}
}

func TestXZ(t *testing.T) {
	if xzNotFound {
		t.Skip("xz required for tests")
	}
	f, err := os.Open("testdata/bench.log.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := ioutil.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}

	xz, err := NewReader(bytes.NewReader(testData))
	if err != nil {
		t.Fatal(err)
	}
	defer xz.Close()
	received, err := ioutil.ReadAll(xz)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(expected, received) {
		dir, err := ioutil.TempDir("", "xz_test_")
		if err != nil {
			return
		}
		ioutil.WriteFile(filepath.Join(dir, "expected_content.log"), expected, 0644)
		ioutil.WriteFile(filepath.Join(dir, "received_content.log"), received, 0644)
		t.Fatalf("xz decompressed content did not match expected content.\n"+
			"The expected and received contents were saved to %q for comparison.", dir)
	}
}

func pidExists(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil { // non-unix only
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func TestClose(t *testing.T) {
	if xzNotFound {
		t.Skip("xz required for tests")
	}
	xz, err := NewReader(bytes.NewReader(testData))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := xz.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	pid := xz.cmd.Process.Pid
	if !pidExists(pid) {
		t.Errorf("invalid process pid: %d", pid)
	}
	if err := xz.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := xz.Read(make([]byte, 1)); err != io.ErrClosedPipe {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if !pidExists(pid) {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
	t.Error("closing the xz reader should have terminated the process,\n" +
		"timed out waiting for the process to exit after 5 seconds")
}

func TestCloseNoStart(t *testing.T) {
	if xzNotFound {
		t.Skip("xz required for tests")
	}
	xz, err := NewReader(bytes.NewReader(testData))
	if err != nil {
		t.Fatal(err)
	}
	if err := xz.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := xz.Read(make([]byte, 1)); err != io.ErrClosedPipe {
		t.Fatal(err)
	}
}

func BenchmarkXZ(b *testing.B) {
	if xzNotFound {
		b.Skip("xz required for tests")
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
			r.Close()
			b.Fatal(err)
		}
	}
}

func BenchmarkXZParallel(b *testing.B) {
	if xzNotFound {
		b.Skip("xz required for tests")
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
				r.Close()
				b.Fatal(err)
			}
		}
	})
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
