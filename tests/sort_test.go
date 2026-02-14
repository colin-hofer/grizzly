package tests

import (
	"os"
	"path/filepath"
	"testing"

	g "grizzly"
)

func TestSortStableForEqualKeys(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.csv")
	// sort key is 'k' (second column). Rows with equal 'k' should preserve original order.
	data := "id,k\nA,1\nB,1\nC,0\nD,1\n"
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	df, err := g.ScanCSV(p, g.ScanOptions{}).Sort("k", false).Collect()
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	s, ok := df.Column("id")
	if !ok {
		t.Fatalf("missing id")
	}
	col, ok := s.Utf8()
	if !ok {
		t.Fatalf("expected utf8 id column")
	}
	// Expected order after sort by k: C first (k=0), then A,B,D (k=1) in original order.
	got := []string{col.Value(0), col.Value(1), col.Value(2), col.Value(3)}
	want := []string{"C", "A", "B", "D"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d got %q want %q", i, got[i], want[i])
		}
	}
}
