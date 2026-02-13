package grizzly

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLazyDoesNotChangeSelectFilterSemantics(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.csv")
	data := "a,b\n1,10\n2,20\n"
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Select drops column b; filtering by b should error.
	_, err := ScanCSV(p, ScanOptions{}).Select("a").Filter(Col("b").Eq(10)).Collect()
	if err == nil {
		t.Fatalf("expected error")
	}
}
