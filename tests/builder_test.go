package tests

import (
	g "grizzly"
	"testing"
)

func TestFrameBuilderBasic(t *testing.T) {
	b := g.NewFrameBuilder()
	b.Add("a", []int64{1, 2, 3}).Add("b", []string{"x", "y", "z"})
	df, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if df.Height() != 3 || df.Width() != 2 {
		t.Fatalf("unexpected shape")
	}
}

func TestFrameBuilderLengthMismatch(t *testing.T) {
	b := g.NewFrameBuilder()
	b.Add("a", []int64{1, 2}).Add("b", []string{"x"})
	_, err := b.Build()
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestFromMapDeterministicOrder(t *testing.T) {
	df, err := g.FromMap(map[string]any{"b": []int64{1}, "a": []int64{2}})
	if err != nil {
		t.Fatalf("from map: %v", err)
	}
	cols := df.Columns()
	if len(cols) != 2 {
		t.Fatalf("unexpected cols")
	}
	if cols[0].Name() != "a" || cols[1].Name() != "b" {
		t.Fatalf("expected a then b")
	}
}
