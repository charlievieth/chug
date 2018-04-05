package util

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
	"unsafe"
)

var BenchmarkData []byte
var BenchmarkReader *bytes.Reader

func init() {
	var err error
	BenchmarkData, err = ioutil.ReadFile("../testdata/rep.out.log")
	if err != nil {
		panic(err)
	}
	BenchmarkReader = bytes.NewReader(BenchmarkData)
}

func BenchmarkReadLine(b *testing.B) {
	r := NewReader(BenchmarkReader)
	for i := 0; i < b.N; i++ {
		for {
			_, err := r.ReadLine()
			if err != nil {
				if err != io.EOF {
					b.Fatal(err)
				}
				BenchmarkReader.Seek(0, 0)
				break
			}
		}
	}
}

func BenchmarkReadSlice(b *testing.B) {
	r := bufio.NewReader(BenchmarkReader)
	for i := 0; i < b.N; i++ {
		for {
			_, err := r.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					b.Fatal(err)
				}
				BenchmarkReader.Seek(0, 0)
				break
			}
		}
	}
}

type stringFinder struct {
	// pattern is the string that we are searching for in the text.
	pattern string

	// badCharSkip[b] contains the distance between the last byte of pattern
	// and the rightmost occurrence of b in pattern. If b is not in pattern,
	// badCharSkip[b] is len(pattern).
	//
	// Whenever a mismatch is found with byte b in the text, we can safely
	// shift the matching frame at least badCharSkip[b] until the next time
	// the matching char could be in alignment.
	badCharSkip [256]int

	// goodSuffixSkip[i] defines how far we can shift the matching frame given
	// that the suffix pattern[i+1:] matches, but the byte pattern[i] does
	// not. There are two cases to consider:
	//
	// 1. The matched suffix occurs elsewhere in pattern (with a different
	// byte preceding it that we might possibly match). In this case, we can
	// shift the matching frame to align with the next suffix chunk. For
	// example, the pattern "mississi" has the suffix "issi" next occurring
	// (in right-to-left order) at index 1, so goodSuffixSkip[3] ==
	// shift+len(suffix) == 3+4 == 7.
	//
	// 2. If the matched suffix does not occur elsewhere in pattern, then the
	// matching frame may share part of its prefix with the end of the
	// matching suffix. In this case, goodSuffixSkip[i] will contain how far
	// to shift the frame to align this portion of the prefix to the
	// suffix. For example, in the pattern "abcxxxabc", when the first
	// mismatch from the back is found to be in position 3, the matching
	// suffix "xxabc" is not found elsewhere in the pattern. However, its
	// rightmost "abc" (at position 6) is a prefix of the whole pattern, so
	// goodSuffixSkip[3] == shift+len(suffix) == 6+5 == 11.
	goodSuffixSkip []int
}

func makeStringFinder(pattern string) *stringFinder {
	f := &stringFinder{
		pattern:        pattern,
		goodSuffixSkip: make([]int, len(pattern)),
	}
	// last is the index of the last character in the pattern.
	last := len(pattern) - 1

	// Build bad character table.
	// Bytes not in the pattern can skip one pattern's length.
	for i := range f.badCharSkip {
		f.badCharSkip[i] = len(pattern)
	}
	// The loop condition is < instead of <= so that the last byte does not
	// have a zero distance to itself. Finding this byte out of place implies
	// that it is not in the last position.
	for i := 0; i < last; i++ {
		f.badCharSkip[pattern[i]] = last - i
	}

	// Build good suffix table.
	// First pass: set each value to the next index which starts a prefix of
	// pattern.
	lastPrefix := last
	for i := last; i >= 0; i-- {
		if HasPrefix(pattern, pattern[i+1:]) {
			lastPrefix = i + 1
		}
		// lastPrefix is the shift, and (last-i) is len(suffix).
		f.goodSuffixSkip[i] = lastPrefix + last - i
	}
	// Second pass: find repeats of pattern's suffix starting from the front.
	for i := 0; i < last; i++ {
		lenSuffix := longestCommonSuffix(pattern, pattern[1:i+1])
		if pattern[i-lenSuffix] != pattern[last-lenSuffix] {
			// (last-i) is the shift, and lenSuffix is len(suffix).
			f.goodSuffixSkip[last-lenSuffix] = lenSuffix + last - i
		}
	}

	return f
}

func longestCommonSuffix(a, b string) (i int) {
	for ; i < len(a) && i < len(b); i++ {
		if a[len(a)-1-i] != b[len(b)-1-i] {
			break
		}
	}
	return
}

// next returns the index in text of the first occurrence of the pattern. If
// the pattern is not found, it returns -1.
func (f *stringFinder) next(text string) int {
	i := len(f.pattern) - 1
	for i < len(text) {
		// Compare backwards from the end until the first unmatching character.
		j := len(f.pattern) - 1
		for j >= 0 && text[i] == f.pattern[j] {
			i--
			j--
		}
		if j < 0 {
			return i + 1 // match
		}
		i += max(f.badCharSkip[text[i]], f.goodSuffixSkip[j])
	}
	return -1
}

func (f *stringFinder) Contains(text string) bool {
	i := len(f.pattern) - 1
	for i < len(text) {
		// Compare backwards from the end until the first unmatching character.
		j := len(f.pattern) - 1
		for j >= 0 && text[i] == f.pattern[j] {
			i--
			j--
		}
		if j < 0 {
			return true
		}
		i += max(f.badCharSkip[text[i]], f.goodSuffixSkip[j])
	}
	return false
}

func AlignDown(base, size uintptr) uintptr {
	return base & -size
}

func isASCIIShort(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

func IsASCII(s string) bool {
	const Size = 8 // sizeof(uintptr)

	if len(s) < Size {
		return isASCIIShort(s)
	}

	_ = s[7] // eliminate bounds check
	if s[0] >= utf8.RuneSelf || s[1] >= utf8.RuneSelf ||
		s[2] >= utf8.RuneSelf || s[3] >= utf8.RuneSelf ||
		s[4] >= utf8.RuneSelf || s[5] >= utf8.RuneSelf ||
		s[6] >= utf8.RuneSelf || s[7] >= utf8.RuneSelf {
		return false
	}

	data := (*(*reflect.StringHeader)(unsafe.Pointer(&s))).Data
	p := AlignDown(data, Size)
	n := int(data - p)
	u := *(*[]uintptr)(unsafe.Pointer(&reflect.SliceHeader{
		Data: p,
		Cap:  len(s) / Size,
		Len:  len(s) / Size,
	}))

	for i := 1; i < len(u); i++ {
		if u[i]&0x8080808080808080 != 0 {
			return false
		}
		n += Size
	}
	return true
}

func ToUpper(r byte) byte {
	if 'a' <= r && r <= 'z' {
		r -= 'a' - 'A'
	}
	return r
}

func (f *stringFinder) IContains(text string) bool {
	if !IsASCII(text) {
		return false
	}
	i := len(f.pattern) - 1
	for i < len(text) {
		// Compare backwards from the end until the first unmatching character.
		j := len(f.pattern) - 1
		for j >= 0 && ToUpper(text[i]) == f.pattern[j] {
			i--
			j--
		}
		if j < 0 {
			return true
		}
		i += max(f.badCharSkip[text[i]], f.goodSuffixSkip[j])
	}
	return false
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func HasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[0:len(prefix)] == prefix
}

const LogLine = `{"timestamp":"1520983802.429046154","source":"rep","message":"rep.container-metrics-reporter.tick.get-all-metrics.containerstore-metrics.starting","log_level":1,"data":{"session":"9.5724.1.1"}}`
const SearchString = "reporter"
const SearchStringCase = "REPORTER"

func BenchmarkContains(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if !strings.Contains(LogLine, SearchString) {
			b.Fatal("WTF")
		}
	}
}

func BenchmarkBoyer(b *testing.B) {
	f := makeStringFinder(SearchString)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !f.Contains(LogLine) {
			b.Fatal("WTF")
		}
	}
}

func BenchmarkContains_Case(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if !strings.Contains(strings.ToUpper(LogLine), SearchStringCase) {
			b.Fatal("WTF")
		}
	}
}

func BenchmarkBoyer_Case(b *testing.B) {
	f := makeStringFinder(SearchStringCase)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !f.IContains(LogLine) {
			b.Fatal("WTF")
		}
	}
}
