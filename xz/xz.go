package xz

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strings"
	"sync"
)

var (
	// Path to XZ executable
	xzFullPath string

	// Validation error if any the type will always be *ValidationError
	xzValidationErr error
	xzValidateOnce  sync.Once
)

type ValidationError struct {
	Op  string
	Err error
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("xz: invalid binary (%s): %s: %s", xzFullPath, e.Op, e.Err)
}

// Validate returns if the found version of XZ is valid.
func Validate() error {
	xzValidateOnce.Do(validateXZ)
	return xzValidationErr
}

// Enabled reports if the XZ package is enabled.
func Enabled() bool { return Validate() == nil }

func validateXZ() {
	// command: printf 'Hello, World!\n' | xz -9 | base64
	const testMessage = "/Td6WFoAAATm1rRGAgAhARwAAAAQz1jMAQANSGVsbG8sIFdvcmxkI" +
		"QoAAADYaZJh4xDmawABJg4IG+AEH7bzfQEAAAAABFla"

	// command: printf 'Hello, World!\n' | shasum
	const expectedSha = "60fde9c2310b0d4cad4dab8d126b04387efba289"

	path, err := exec.LookPath("xz")
	if err != nil {
		xzValidationErr = &ValidationError{Op: "missing xz binary", Err: err}
		return
	}
	xzFullPath = path

	r := newReader(base64.NewDecoder(
		base64.StdEncoding,
		strings.NewReader(testMessage)),
	)
	defer r.Close()
	h := sha1.New()
	if _, err := io.CopyBuffer(h, r, make([]byte, 64)); err != nil {
		xzValidationErr = &ValidationError{Op: "copy", Err: err}
		return
	}

	s := hex.EncodeToString(h.Sum(nil))
	if s != expectedSha {
		xzValidationErr = &ValidationError{
			Op:  "sha1",
			Err: errors.New("want: " + expectedSha + " got: " + s),
		}
		return
	}
}

type Reader struct {
	cmd       *exec.Cmd
	pr        *io.PipeReader
	pw        *io.PipeWriter
	initOnce  sync.Once
	closeOnce sync.Once
	stderr    bytes.Buffer
	done      chan struct{} // used to abort the process
}

func NewReader(rd io.Reader) (*Reader, error) {
	if err := Validate(); err != nil {
		return nil, err
	}
	return newReader(rd), nil
}

// newReader is the internal implementation that does not validate the xz
// binary - this is to prevent a deadlock during validation.
func newReader(rd io.Reader) *Reader {
	pr, pw := io.Pipe()
	cmd := exec.Command(xzFullPath, "--no-warn", "--stdout", "--decompress", "-")
	cmd.Stdin = rd
	cmd.Stdout = pw
	xz := &Reader{
		cmd:  cmd,
		pr:   pr,
		pw:   pw,
		done: make(chan struct{}),
	}
	cmd.Stderr = &xz.stderr
	runtime.SetFinalizer(xz, (*Reader).Close)
	return xz
}

func (r *Reader) wait() {
	errCh := make(chan error, 1)
	go func() { errCh <- r.cmd.Wait() }()
	select {
	case err := <-errCh:
		if err != nil {
			err = fmt.Errorf("runtime error: %s: %s", err,
				strings.TrimSpace(r.stderr.String()))
		}
		r.pw.CloseWithError(err)
	case <-r.done:
		r.cmd.Process.Kill()
	}
}

func (r *Reader) lazyInit() {
	if err := r.cmd.Start(); err != nil {
		r.pw.CloseWithError(err)
		return
	}
	go r.wait()
}

// Read implements the standard Read interface.  The first call to Read starts
// the underlying XZ process.
func (r *Reader) Read(p []byte) (n int, err error) {
	r.initOnce.Do(r.lazyInit)
	n, err = r.pr.Read(p)
	runtime.KeepAlive(r)
	return
}

// Close closes the reader and stops the underlying XZ process if it is running,
// but does not wait for it to exit.  It is safe to call Close multiple times.
func (r *Reader) Close() error {
	if r == nil {
		return errors.New("xz: call to Close on nil *Reader")
	}
	r.closeOnce.Do(func() {
		// no-op if initOnce has been called,
		// otherwise this prevents the call
		r.initOnce.Do(func() {})
		r.pr.CloseWithError(io.ErrClosedPipe)
		r.pw.CloseWithError(io.ErrClosedPipe)
		close(r.done)
		runtime.SetFinalizer(r, nil)
	})
	return nil
}
