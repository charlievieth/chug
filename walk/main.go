package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// TraverseLink is a sentinel error for Walk, similar to filepath.SkipDir.
var TraverseLink = errors.New("traverse symlink, assuming target is a directory")

type WalkFn func(path string, typ os.FileMode, rc io.ReadCloser) error

// Walk walks the file tree rooted at root, calling walkFn for
// each file or directory in the tree, including root.
//
// If Walk returns filepath.SkipDir, the directory is skipped.
//
// Unlike filepath.Walk:
//   * file stat calls must be done by the user.
//     The only provided metadata is the file type, which does not include
//     any permission bits.
//   * multiple goroutines stat the filesystem concurrently. The provided
//     walkFn must be safe for concurrent use.
//   * Walk can follow symlinks if walkFn returns the TraverseLink
//     sentinel error. It is the walkFn's responsibility to prevent
//     Walk from going into symlink cycles.
func Walk(root string, walkFn func(path string, typ os.FileMode, rc io.ReadCloser) error) error {
	// TODO(bradfitz): make numWorkers configurable? We used a
	// minimum of 4 to give the kernel more info about multiple
	// things we want, in hopes its I/O scheduling can take
	// advantage of that. Hopefully most are in cache. Maybe 4 is
	// even too low of a minimum. Profile more.
	//
	numWorkers := 4
	if n := runtime.NumCPU(); n > numWorkers {
		numWorkers = n
	}
	return walkN(filepath.Clean(root), walkFn, numWorkers)
}

func walkN(root string, walkFn func(path string, typ os.FileMode, rc io.ReadCloser) error, numWorkers int) error {
	w := &walker{
		fn:       walkFn,
		enqueuec: make(chan walkItem, numWorkers), // buffered for performance
		workc:    make(chan walkItem, numWorkers), // buffered for performance
		donec:    make(chan struct{}),

		// buffered for correctness & not leaking goroutines:
		resc: make(chan error, numWorkers),
	}
	defer close(w.donec)
	// TODO(bradfitz): start the workers as needed? maybe not worth it.
	for i := 0; i < numWorkers; i++ {
		go w.doWork()
	}
	todo := []walkItem{{dir: root}}
	out := 0
	for {
		workc := w.workc
		var workItem walkItem
		if len(todo) == 0 {
			workc = nil
		} else {
			workItem = todo[len(todo)-1]
		}
		select {
		case workc <- workItem:
			todo = todo[:len(todo)-1]
			out++
		case it := <-w.enqueuec:
			todo = append(todo, it)
		case err := <-w.resc:
			out--
			if err != nil {
				return err
			}
			if out == 0 && len(todo) == 0 {
				// It's safe to quit here, as long as the buffered
				// enqueue channel isn't also readable, which might
				// happen if the worker sends both another unit of
				// work and its result before the other select was
				// scheduled and both w.resc and w.enqueuec were
				// readable.
				select {
				case it := <-w.enqueuec:
					todo = append(todo, it)
				default:
					return nil
				}
			}
		}
	}
}

// doWork reads directories as instructed (via workc) and runs the
// user's callback function.
func (w *walker) doWork() {
	for {
		select {
		case <-w.donec:
			return
		case it := <-w.workc:
			w.resc <- w.walk(it.dir, !it.callbackDone, it.archive)
		}
	}
}

type walker struct {
	fn func(path string, typ os.FileMode, rc io.ReadCloser) error

	donec    chan struct{} // closed on Walk's return
	workc    chan walkItem // to workers
	enqueuec chan walkItem // from workers
	resc     chan error    // from workers
}

type walkItem struct {
	dir          string
	archive      io.ReadCloser
	callbackDone bool // callback already called; don't do it again
}

func (w *walker) enqueue(it walkItem) {
	select {
	case w.enqueuec <- it:
	case <-w.donec:
	}
}

func (w *walker) onDirEnt(dirName, baseName string, typ os.FileMode) error {
	if len(baseName) == 0 || baseName[0] == '.' {
		return nil
	}

	joined := dirName + string(os.PathSeparator) + baseName
	if typ == os.ModeDir {
		w.enqueue(walkItem{dir: joined})
		return nil
	}
	if extArchive(joined) {
		f, err := openFile(joined)
		if err != nil {
			return err
		}
		w.enqueue(walkItem{dir: joined, archive: f})
		return nil
	}

	err := w.fn(joined, typ, nil)
	if typ == os.ModeSymlink {
		if err == TraverseLink {
			// Set callbackDone so we don't call it twice for both the
			// symlink-as-symlink and the symlink-as-directory later:
			w.enqueue(walkItem{dir: joined, callbackDone: true})
			return nil
		}
		if err == filepath.SkipDir {
			// Permit SkipDir on symlinks too.
			return nil
		}
	}
	return err
}

func (w *walker) onTarEnt(dirName string, hdr *tar.Header, tr *tar.Reader) error {
	// TODO (CEV): maybe some of this logic should live in readArchive ???
	if extArchive(hdr.Name) {
		buf := newBuffer()
		buf.Grow(int(hdr.Size) + bytes.MinRead)
		if _, err := buf.ReadFrom(tr); err != nil {
			bufferPool.Put(buf)
			return err
		}
		if extGzip(hdr.Name) {
			gz, err := gzip.NewReader(buf)
			if err != nil {
				bufferPool.Put(buf)
				return err
			}
			w.enqueue(walkItem{dir: dirName, archive: gzipReadCloser{
				buf: buf,
				gz:  gz,
			}})
			return nil
		}
		w.enqueue(walkItem{dir: dirName, archive: bufferReadCloser{Buffer: buf}})
		return nil
	}
	// CEV: do we want to check if we care about the file first?
	return w.fn(hdr.Name, 0, ioutil.NopCloser(tr))
}

func (w *walker) walk(root string, runUserCallback bool, archive io.ReadCloser) error {
	if archive != nil {
		return w.readArchive(root, archive)
	}
	if runUserCallback {
		err := w.fn(root, os.ModeDir, nil)
		if err == filepath.SkipDir {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return readDir(root, w.onDirEnt)
}

func (w *walker) readArchive(dirName string, rc io.ReadCloser) error {
	var err error
	tr := tar.NewReader(rc)
	for {
		hdr, e := tr.Next()
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
		if hdr.Typeflag != tar.TypeReg || hdr.Size == 0 {
			continue
		}
		if err = w.onTarEnt(dirName, hdr, tr); err != nil {
			break
		}
	}
	if e := rc.Close(); err == nil {
		err = e
	}
	return err
}

// TODO (CEV): Remove logic for handling files - that should be done elsewhere.
//
// readDir calls fn for each directory entry in dirName. It does not descend
// into directories or follow symlinks. If fn returns a non-nil error, readDir
// returns with that error immediately.
//
// As a special case it is permitted for dirName to be a regular file.
//
func readDir(dirName string, fn func(dirName, entName string, typ os.FileMode) error) error {
	f, err := os.Open(dirName)
	if err != nil {
		return err
	}
	list, err := f.Readdir(-1)
	// allow regular files
	if err != nil {
		fi, e := f.Stat()
		f.Close()
		if e == nil && fi.Mode().IsRegular() {
			dir := filepath.Dir(dirName)
			return fn(dir, fi.Name(), fi.Mode()&os.ModeType)
		}
		return err
	}
	f.Close()

	for _, fi := range list {
		if err := fn(dirName, fi.Name(), fi.Mode()&os.ModeType); err != nil {
			return err
		}
	}
	return nil
}

func openFile(name string) (io.ReadCloser, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	if extGzip(name) {
		// gzip will wrap the reading in a bufio.Reader if it
		// does not implement ByteReader, use a slightly larger
		// buffer than the default size of 4096 to reduce the
		// number of syscalls.
		gz, err := gzip.NewReader(bufio.NewReaderSize(f, 64*1024))
		if err != nil {
			f.Close()
			return nil, err
		}
		return gzipFileCloser{gz: gz, file: f}, nil
	}
	return f, nil
}

func extGzip(name string) bool {
	return hasSuffix(name, ".gz") || hasSuffix(name, ".tgz")
}

func extArchive(name string) bool {
	return hasSuffix(name, ".tar") || hasSuffix(name, ".tgz") ||
		hasSuffix(name, ".tar.gz")
}

// hasSuffix tests whether the string s ends with suffix.  Same as
// strings.HasSuffix, but with a shorter name.
func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

////////////////////////////////////////////////////////////////////////////////
////// TODO WE CAN PROBABLY GET RID OF ALL THIS POOL NONSENSE AT NO COST ///////
////////////////////////////////////////////////////////////////////////////////

var bufferPool sync.Pool

func newBuffer() *bytes.Buffer {
	if v := bufferPool.Get(); v != nil {
		b := v.(*bytes.Buffer)
		b.Reset()
		return b
	}
	return new(bytes.Buffer)
}

type bufferReadCloser struct {
	*bytes.Buffer
}

func (b bufferReadCloser) Close() error {
	if b.Buffer != nil {
		bufferPool.Put(b.Buffer)
	}
	return nil
}

type gzipFileCloser struct {
	gz   *gzip.Reader
	file *os.File
}

func (g gzipFileCloser) Read(p []byte) (int, error) {
	return g.gz.Read(p)
}

func (g gzipFileCloser) Close() error {
	g.file.Close()
	return g.gz.Close()
}

type gzipReadCloser struct {
	buf *bytes.Buffer
	gz  *gzip.Reader
}

func (g gzipReadCloser) Read(p []byte) (int, error) {
	return g.gz.Read(p)
}

func (g gzipReadCloser) Close() error {
	bufferPool.Put(g.buf)
	return g.gz.Close()
}

// type WalkFn func(path string, typ os.FileMode, rc io.ReadCloser) error

func main() {
	// const root = "/Users/charlie/Desktop/ditmars-logs/warp-drive/out_logs.tgz"
	const root = "/Users/charlie/Desktop/ditmars-logs/warp-drive"
	var count int64
	var nill int64
	var errs int64
	start := time.Now()
	err := Walk(root, func(path string, typ os.FileMode, rc io.ReadCloser) error {
		// if path == "/Users/charlie/Desktop/ditmars-logs/warp-drive/out_logs" {
		// 	return filepath.SkipDir
		// }
		if rc != nil {
			if _, err := ioutil.ReadAll(rc); err != nil {
				fmt.Printf("error (%s): %s\n", path, err)
				atomic.AddInt64(&errs, 1)
			}
		} else {
			atomic.AddInt64(&nill, 1)
		}
		atomic.AddInt64(&count, 1)
		// fmt.Println(path)
		return nil
	})
	d := time.Since(start)
	fmt.Println("Error:", err)
	fmt.Println("Count:", count)
	fmt.Println("Nill:", nill)
	fmt.Println("Errors:", errs)
	fmt.Println("Time:", d, d/time.Duration(count))
	fmt.Println()
}

/*
type lazyFile struct {
	file *os.File
	name string
}

func (l *lazyFile) lazyInit() error {
	if l.file != nil {
		return nil
	}
	var err error
	l.file, err = os.Open(l.name)
	return err
}

func (l *lazyFile) Read(p []byte) (int, error) {
	if err := l.lazyInit(); err != nil {
		return 0, err
	}
	return l.file.Read(p)
}

func (l *lazyFile) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}
*/

/*
var gzipReaderPool sync.Pool

func newGzipReader(r io.Reader) (*gzip.Reader, error) {
	if v := gzipReaderPool.Get(); v != nil {
		gz := v.(*gzip.Reader)
		if err := gz.Reset(r); err != nil {
			gzipReaderPool.Put(gz)
			return nil, err
		}
		return gz, nil
	}
	return gzip.NewReader(r)
}
*/
