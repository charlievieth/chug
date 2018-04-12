package walk

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/charlievieth/chug/util"
)

// RACE RACE RACE RACE
//   Looks like we're racing on the gzip reader - likely due to
//   mutliple threads trying to access it - this is umm bad...
// RACE RACE RACE RACE

// TODO:
//  - Add files to walk (dirs and regular files)
//   * One thing to consider will be checking filetype by compress
//     header (gzip, xz, etc...)
//  - Consider using a seperate function to determine what to walk

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
func Walk(root string, walkFn WalkFn) error {
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
	return walkN([]string{filepath.Clean(root)}, walkFn, numWorkers)
}

func WalkRoots(roots []string, walkFn WalkFn) error {
	numWorkers := 4
	if n := runtime.NumCPU(); n > numWorkers {
		numWorkers = n
	}
	for i, s := range roots {
		roots[i] = filepath.Clean(s)
	}
	return walkN(roots, walkFn, numWorkers)
}

func walkN(roots []string, walkFn WalkFn, numWorkers int) error {
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
	todo := make([]walkItem, len(roots))
	for i, root := range roots {
		todo[i] = walkItem{dir: root}
	}
	fmt.Println("TODO:", todo)
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
	fn       WalkFn
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
		// unlike onTarEnt pass the file directly
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

// WARN: as predicted the race occurs here for the obvious reasons - nested
// readers in a highly parallel environment - consuming readers solves this
// but is a shot in the head to efficiency.
//
// Questions: this worked on ditmars - what did I break; an additional
// function to check if files should be parsed would eliminate the need
// for wasteful reading.
func (w *walker) onTarEnt(dirName string, hdr *tar.Header, tr *tar.Reader) error {
	// TODO (CEV): maybe some of this logic should live in readArchive ???

	if extArchive(hdr.Name) {
		buf, err := util.ReadAllSize(tr, hdr.Size)
		if err != nil {
			return err
		}
		if extGzip(hdr.Name) {
			gz, err := util.NewGzipReadCloser(buf)
			if err != nil {
				buf.Close()
				return err
			}
			w.enqueue(walkItem{dir: dirName, archive: gz})
			return nil
		}
		w.enqueue(walkItem{dir: dirName, archive: buf})
		return nil
	}

	// WARN: this appears to remove the race - but is really wasteful
	// considering we don't even know if we need the file
	//
	// CEV: do we want to check if we care about the file first?
	buf, err := util.ReadAllSize(tr, hdr.Size)
	if err != nil {
		buf.Close()
		return err
	}
	return w.fn(hdr.Name, 0, buf)
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
	return w.readDir(root)
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
		// WARN: shared gzip reader may/will race
		// TODO: continue on error?
		if err = w.onTarEnt(dirName, hdr, tr); err != nil {
			break
		}
	}
	if e := rc.Close(); e != nil && err == nil {
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
func (w *walker) readDir(dirName string) error {
	fmt.Println("readDir:", dirName)
	f, err := os.Open(dirName)
	if err != nil {
		return err
	}
	list, err := f.Readdir(-1)
	// allow regular files
	if err != nil {
		fmt.Println("readDir (error):", err)
		fi, e := f.Stat()
		f.Close()
		if e == nil && fi.Mode().IsRegular() {
			dir := filepath.Dir(dirName)
			return w.onDirEnt(dir, fi.Name(), fi.Mode()&os.ModeType)
			// fmt.Println("OK")
			// return w.fn(dirName, fi.Mode()&os.ModeType, nil)
		}
		fmt.Println("readDir (error): FUCK", err)
		return err
	}
	f.Close()

	for _, fi := range list {
		if err := w.onDirEnt(dirName, fi.Name(), fi.Mode()&os.ModeType); err != nil {
			return err
		}
	}
	return nil
}

// TODO: move to utils package
func openFile(name string) (io.ReadCloser, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	if extGzip(name) {
		gr, err := util.NewGzipReadCloser(f)
		if err != nil {
			f.Close()
		}
		return gr, err
	}
	return f, nil
}

// TODO: move to utils package
func extGzip(name string) bool {
	return hasSuffix(name, ".gz") || hasSuffix(name, ".tgz")
}

// TODO: move to utils package
func extArchive(name string) bool {
	return hasSuffix(name, ".tar") || hasSuffix(name, ".tgz") ||
		hasSuffix(name, ".tar.gz")
}

// TODO: move to utils package
//
// hasSuffix tests whether the string s ends with suffix.  Same as
// strings.HasSuffix, but with a shorter name.
func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
