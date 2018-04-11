package main

import (
	"bytes"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

/*
type LagerMatcher struct {
	Timestamp *TimeMatcher
	LogLevel  *MinLogLevelMatcher
	Source    *PatternMatcher
	Message   *PatternMatcher
	Session   *PatternMatcher
	Error     *PatternMatcher
	Trace     *PatternMatcher
	Data      *PatternMatcher
}

func (m *LagerMatcher) isEmpty(allFields bool) bool {
	return (!allFields || m.Timestamp == nil) && (!allFields || m.LogLevel == nil) &&
		m.Source == nil && m.Message == nil && m.Session == nil && m.Error == nil &&
		m.Trace == nil && m.Data == nil
}

func (m *LagerMatcher) Candidate(line []byte) bool {
	if m.isEmpty(false) {
		return true
	}

	return (m.Source != nil && m.Source.Match(line)) ||
		(m.Message != nil && m.Message.Match(line)) ||
		(m.Session != nil && m.Session.Match(line)) ||
		(m.Error != nil && m.Error.Match(line)) ||
		(m.Trace != nil && m.Trace.Match(line)) ||
		(m.Data != nil && m.Data.Match(line))
}

func (m *LagerMatcher) Match(e *LogEntry) bool {
	if m.isEmpty(true) {
		return true
	}

	// exclusive matches
	if m.LogLevel != nil && !m.LogLevel.Match(e.LogLevel) {
		return false
	}
	if m.Timestamp != nil && !m.Timestamp.Match(e.Timestamp) {
		return false
	}

	// TODO:
	//  - require everything to match?
	//  - what about negative matches (exclusions)?
	//
	if m.Source != nil && e.Source != "" && m.Source.MatchString(e.Source) {
		return true
	}
	if m.Message != nil && e.Message != "" && m.Message.MatchString(e.Message) {
		return true
	}
	if m.Session != nil && e.Session != "" && m.Session.MatchString(e.Session) {
		return true
	}
	if m.Error != nil && e.Error != "" && m.Error.MatchString(e.Error) {
		return true
	}
	if m.Trace != nil && e.Trace != "" && m.Trace.MatchString(e.Trace) {
		return true
	}
	if m.Data != nil && m.Data.Match(e.Data) {
		return true
	}
	return false
}
*/

type LagerMatcher struct {
	Timestamp *TimeMatcher
	LogLevel  *MinLogLevelMatcher
	Regex     PatternSet // Match against line (maybe rename to line or something)
	Source    PatternSet
	Message   PatternSet
	Session   PatternSet
	Error     PatternSet
	Trace     PatternSet
	Data      PatternSet
}

type TimeMatcher struct {
	Min, Max time.Time
}

func NewTimeMatcher(min, max time.Time) (*TimeMatcher, error) {
	if !max.IsZero() && min.After(max) {
		return nil, fmt.Errorf("TimeMatcher: min (%s) occurs after max (%s)", min, max)
	}
	m := &TimeMatcher{
		Min: min,
		Max: max,
	}
	return m, nil
}

func (m *TimeMatcher) Match(t time.Time) bool {
	zMin := m.Min.IsZero()
	zMax := m.Max.IsZero()
	if !zMin && !zMax {
		return (m.Min.Equal(t) || m.Min.Before(t)) &&
			(m.Max.Equal(t) || m.Max.After(t))
	}
	if !zMin {
		return m.Min.Equal(t) || m.Min.Before(t)
	}
	if !zMax {
		return m.Max.Equal(t) || m.Max.After(t)
	}
	return true
}

// TODO: use `level":\s*(?:1|"info")` to match candidates
type MinLogLevelMatcher struct {
	min LogLevel
	re  *regexp.Regexp
}

func NewMinLogLevelMatcher(v LogLevel) (*MinLogLevelMatcher, error) {
	if !v.valid() {
		return nil, fmt.Errorf("invalid LogLevel: %d", v)
	}
	var intLevels []string
	var strLevels []string
	for i := v; i <= FATAL; i++ {
		intLevels = append(intLevels, strconv.Itoa(int(i)))
		strLevels = append(strLevels, i.String())
	}
	expr := fmt.Sprintf(`level":\s*(?:[%s]|"(%s)")`,
		strings.Join(intLevels, "|"), strings.Join(strLevels, "|"))
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, err
	}
	return &MinLogLevelMatcher{min: v, re: re}, nil
}

func (m MinLogLevelMatcher) Match(v LogLevel) bool      { return m.min <= v }
func (m MinLogLevelMatcher) Candidate(line []byte) bool { return m.re.Match(line) }

func isRegex(s string) bool { return strings.ContainsAny(s, "$()*+.?[\\]^{|}") }

// ^              at beginning of text or line (flag m=true)
// $              at end of text (like \z not Perl's \Z) or line (flag m=true)
// \A             at beginning of text
// \b             at ASCII word boundary (\w on one side and \W, \A, or \z on the other)
// \B             not at ASCII word boundary
// \z             at end of text

type PatternMatcher struct {
	expr  string
	bexpr []byte
	re    *regexp.Regexp
}

func NewPatternMatcher(expr string) (*PatternMatcher, error) {
	var re *regexp.Regexp
	if isRegex(expr) {
		var err error
		re, err = regexp.Compile(expr)
		if err != nil {
			return nil, err
		}
	}
	m := &PatternMatcher{
		expr:  expr,
		bexpr: []byte(expr),
		re:    re,
	}
	return m, nil
}

func (m *PatternMatcher) Match(b []byte) bool {
	if m.re != nil {
		return m.re.Match(b)
	}
	return bytes.Contains(b, m.bexpr)
}

func (m *PatternMatcher) MatchString(s string) bool {
	if m.re != nil {
		return m.re.MatchString(s)
	}
	return strings.Contains(s, m.expr)
}

func (p *PatternMatcher) String() string { return p.expr }

type PatternSet []*PatternMatcher

func (s *PatternSet) Get() interface{} { return []*PatternMatcher(*s) }

func (s PatternSet) String() string {
	a := make([]string, len(s))
	for i, p := range s {
		a[i] = p.String()
	}
	return fmt.Sprintf("%v", a)
}

func (s *PatternSet) Set(expr string) error {
	p, err := NewPatternMatcher(expr)
	if err != nil {
		return err
	}
	*s = append(*s, p)
	return nil
}

func (s PatternSet) Match(b []byte) bool {
	for _, p := range s {
		if p.Match(b) {
			return true
		}
	}
	return false
}

func (s PatternSet) MatchString(str string) bool {
	for _, p := range s {
		if p.MatchString(str) {
			return true
		}
	}
	return false
}

type GlobSet []string

func (g GlobSet) String() string {
	return fmt.Sprintf("%v", []string(g))
}

func (g GlobSet) Get() interface{} { return []string(g) }

func (g *GlobSet) Set(pattern string) error {
	return g.Add(pattern)
}

func (g *GlobSet) Add(pattern string) error {
	// make sure the pattern is valid
	if _, err := filepath.Match(pattern, pattern); err != nil {
		return fmt.Errorf("glob: %s: %s", err, pattern)
	}
	*g = append(*g, pattern)
	return nil
}

func (g GlobSet) match(basename string) bool {
	for _, p := range g {
		if ok, _ := filepath.Match(p, basename); ok {
			return true
		}
	}
	return false
}

// Match reports whether basename matches any of the GlobSet's shell file name
// patterns.  An empty GlobSet matches anything.
func (g GlobSet) Match(basename string) bool {
	return len(g) == 0 || g.match(basename)
}

// Exclude reports whether basename matches any of the GlobSet's shell file name
// patterns.  Unlike Match, an empty GlobSet matches nothing.
func (g GlobSet) Exclude(basename string) bool {
	return len(g) != 0 && g.match(basename)
}
