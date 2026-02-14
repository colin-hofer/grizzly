package tests

import (
	"os"
	"path/filepath"
	"testing"

	g "grizzly"
)

func TestLazyDoesNotChangeSelectFilterSemantics(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.csv")
	data := "a,b\n1,10\n2,20\n"
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Select drops column b; filtering by b should error.
	_, err := g.ScanCSV(p, g.ScanOptions{}).Select("a").Filter(g.Col("b").Eq(10)).Collect()
	if err == nil {
		t.Fatalf("expected error")
	}
}
