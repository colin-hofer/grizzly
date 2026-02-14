package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	g "grizzly"
)

func TestScanCSVInferenceNulls(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.csv")
	// column a should infer int64 even with NULLs
	data := "a,b\n1,hello\nNULL,world\n3,zzz\n"
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	df, err := g.ScanCSV(p, g.ScanOptions{}).Collect()
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	s, ok := df.Column("a")
	if !ok {
		t.Fatalf("missing column a")
	}
	ic, ok := s.Int64()
	if !ok {
		t.Fatalf("expected int64 column")
	}
	if ic.IsNull(0) || ic.Value(0) != 1 {
		t.Fatalf("unexpected row 0")
	}
	if !ic.IsNull(1) {
		t.Fatalf("expected null at row 1")
	}
}

func TestScanCSVFilterPushdownEven(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.csv")
	data := "id,val\n-2,a\n-1,b\n0,c\n3,d\n"
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	df, err := g.ScanCSV(p, g.ScanOptions{}).Filter(g.Col("id").Even()).Collect()
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if df.Height() != 2 {
		t.Fatalf("expected 2 rows, got %d", df.Height())
	}
}

func TestScanCSVCollectContextCanceled(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.csv")
	data := "a\n1\n2\n3\n"
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := g.ScanCSV(p, g.ScanOptions{}).CollectContext(ctx)
	if err == nil {
		t.Fatalf("expected cancellation error")
	}
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestLazyGroupByAggCSV(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.csv")
	data := "k,v\n1,10\n1,20\n2,1\n"
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	out, err := g.ScanCSV(p, g.ScanOptions{}).GroupBy("k").Agg(g.Count(), g.Sum("v").As("s")).Collect()
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if out.Height() != 2 {
		t.Fatalf("expected 2 groups got %d", out.Height())
	}
	ks, _ := out.Column("k")
	ki, ok := ks.Int64()
	if !ok {
		t.Fatalf("expected int64 key")
	}
	ss, _ := out.Column("s")
	si, ok := ss.Int64()
	if !ok {
		t.Fatalf("expected int64 sum")
	}
	if ki.Value(0) != 1 || si.Value(0) != 30 {
		t.Fatalf("unexpected group 0")
	}
	if ki.Value(1) != 2 || si.Value(1) != 1 {
		t.Fatalf("unexpected group 1")
	}
}
