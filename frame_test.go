package grizzly

import (
	"strings"
	"testing"
)

func makeTestFrame(t *testing.T) *Frame {
	t.Helper()
	id := NewColumn("id", []int{3, 1, 2})
	name := NewColumn("name", []string{"zoe", "amy", "bob"})
	age, err := NewNullableColumn("age", []int{30, 0, 25}, []bool{true, false, true})
	if err != nil {
		t.Fatalf("NewNullableColumn failed: %v", err)
	}
	df, err := NewFrame(id, name, age)
	if err != nil {
		t.Fatalf("NewFrame failed: %v", err)
	}
	return df
}

func TestNewFrameValidation(t *testing.T) {
	if _, err := NewFrame(); err == nil {
		t.Fatalf("expected error for empty frame")
	}

	a := NewColumn("a", []int{1, 2})
	b := NewColumn("b", []int{1})
	if _, err := NewFrame(a, b); err == nil {
		t.Fatalf("expected error for mismatched lengths")
	}

	c1 := NewColumn("dup", []int{1})
	c2 := NewColumn("dup", []int{2})
	if _, err := NewFrame(c1, c2); err == nil {
		t.Fatalf("expected error for duplicate column name")
	}
}

func TestSortFilterMaterialize(t *testing.T) {
	df := makeTestFrame(t)

	if err := df.SortBy(SortKey{Name: "name"}); err != nil {
		t.Fatalf("SortBy failed: %v", err)
	}

	filtered := df.Filter(func(r Row) bool {
		v, ok := r.Value("age")
		return ok && v.(int) >= 25
	})
	if got := filtered.NumRows(); got != 2 {
		t.Fatalf("filtered rows=%d, want 2", got)
	}

	mat, err := filtered.Materialize()
	if err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}
	if got := mat.NumRows(); got != 2 {
		t.Fatalf("materialized rows=%d, want 2", got)
	}

	head := mat.Head(2)
	if !strings.Contains(head, "id\tname\tage") {
		t.Fatalf("unexpected head output: %q", head)
	}
}

func TestSortValidation(t *testing.T) {
	df := makeTestFrame(t)
	if err := df.SortBy(); err == nil {
		t.Fatalf("expected error for empty sort keys")
	}
	if err := df.SortBy(SortKey{Name: "missing"}); err == nil {
		t.Fatalf("expected error for missing sort column")
	}
}
