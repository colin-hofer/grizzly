package grizzly

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestJSONKeyOrderDeterministic(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.json")
	data := `[ {"b": 1, "a": 2}, {"a": 3} ]`
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	df, err := ScanJSON(p).Collect()
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	cols := df.Columns()
	if len(cols) < 2 {
		t.Fatalf("expected at least 2 columns")
	}
	if cols[0].Name() != "a" {
		t.Fatalf("expected first column 'a', got %q", cols[0].Name())
	}
}

func TestScanJSONCollectContextCanceled(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.json")
	data := `[ {"a": 1}, {"a": 2} ]`
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ScanJSON(p).CollectContext(ctx)
	if err == nil {
		t.Fatalf("expected cancellation error")
	}
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
