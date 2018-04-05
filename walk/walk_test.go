package walk

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkWalkDir(b *testing.B) {
	const root = "/Users/charlie/Desktop/ditmars-logs/warp-drive/out_logs"
	Walk(root, func(path string, typ os.FileMode, rc io.ReadCloser) error {
		return nil
	})
}

func BenchmarkWalkGZ(b *testing.B) {
	const root = "/Users/charlie/Desktop/ditmars-logs/warp-drive"
	Walk(root, func(path string, typ os.FileMode, rc io.ReadCloser) error {
		if path == "/Users/charlie/Desktop/ditmars-logs/warp-drive/out_logs" {
			return filepath.SkipDir
		}
		return nil
	})
}
