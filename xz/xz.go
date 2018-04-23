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
		xzValidationErr = &ValidationError{Op: "locate xz binary", Err: err}
		return
	}
	xzFullPath = path

	r := newReader(base64.NewDecoder(
		base64.StdEncoding,
		strings.NewReader(testMessage)),
	)
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
	cmd    *exec.Cmd
	pr     *io.PipeReader
	pw     *io.PipeWriter
	once   sync.Once
	stderr bytes.Buffer
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
		cmd: cmd,
		pr:  pr,
		pw:  pw,
	}
	cmd.Stderr = &xz.stderr
	return xz
}

// TODO: remove ~4% gain is not worth it
func NewFileReader(name string) (*Reader, error) {
	if err := Validate(); err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	cmd := exec.Command(xzFullPath, "--no-warn", "--stdout", "--decompress", name)
	cmd.Stdout = pw
	xz := &Reader{
		cmd: cmd,
		pr:  pr,
		pw:  pw,
	}
	cmd.Stderr = &xz.stderr
	return xz, nil
}

func (r *Reader) wait() {
	err := r.cmd.Wait()
	if err != nil {
		err = fmt.Errorf("runtime error: %s: %s", err, r.stderr.String())
	}
	r.pw.CloseWithError(err)
}

func (r *Reader) lazyInit() {
	r.once.Do(func() {
		if err := r.cmd.Start(); err != nil {
			r.pw.CloseWithError(err)
		}
		go r.wait()
	})
}

func (r *Reader) Read(p []byte) (int, error) {
	r.lazyInit()
	return r.pr.Read(p)
}
