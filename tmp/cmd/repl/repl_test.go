package main

import (
	"testing"
	"time"
)

func BenchmarkParse1(b *testing.B) {
	s := formatTimestamp(time.Now())
	for i := 0; i < b.N; i++ {
		Parse1(s)
	}
}

func BenchmarkParse2(b *testing.B) {
	s := formatTimestamp(time.Now())
	for i := 0; i < b.N; i++ {
		Parse2(s)
	}
}
