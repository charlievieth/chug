package main

import "testing"

var FloatTests = [][]byte{
	[]byte("1520985621.156153440"),
	[]byte("1520985621"),
}

func BenchmarkIsFloat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if !isFloat(FloatTests[i%2]) {
			b.Fatal("isFloat")
		}
	}
}
