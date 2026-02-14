package tests

import (
	g "grizzly"
	"testing"
)

func TestNewColumnValidityLengthMismatch(t *testing.T) {
	if _, err := g.NewInt64Column("x", []int64{1, 2}, []bool{true}); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := g.NewFloat64Column("x", []float64{1, 2}, []bool{true}); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := g.NewBoolColumn("x", []bool{true, false}, []bool{true}); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := g.NewUtf8Column("x", []string{"a", "b"}, []bool{true}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestValuesAreCopy(t *testing.T) {
	c, err := g.NewInt64Column("x", []int64{10, 20, 30}, nil)
	if err != nil {
		t.Fatalf("NewInt64Column: %v", err)
	}
	v := c.Values()
	v[0] = 999
	if got := c.Value(0); got != 10 {
		t.Fatalf("expected column to remain immutable, got %d", got)
	}
}
