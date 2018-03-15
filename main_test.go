package main

import "testing"

func BenchmarkWalk(b *testing.B) {
	const root = "/Users/charlie/Desktop/ditmars-logs/warp-drive/out_logs"
	// const root = "/Users/charlie/Desktop/ditmars-logs/warp-drive/out_logs/diego-cell_0ff998a2-1b9e-4182-82f9-f8dbb5f844b6/rep"
	for i := 0; i < b.N; i++ {
		Walk(root)
	}
}

func BenchmarkLogLevelUnmarshalJSON(b *testing.B) {
	data := []byte(`"fatal"`)
	var ll LogLevel
	for i := 0; i < b.N; i++ {
		if err := ll.UnmarshalJSON(data); err != nil {
			b.Fatal(err)
		}
	}
}
