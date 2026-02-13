package grizzly

import "testing"

type person struct {
	ID   int
	Name string
}

func TestStructColumnWorks(t *testing.T) {
	people := NewColumn("person", []person{{ID: 2, Name: "b"}, {ID: 1, Name: "a"}})
	id := NewColumn("id", []int{2, 1})

	df, err := NewFrame(people, id)
	if err != nil {
		t.Fatalf("NewFrame failed: %v", err)
	}

	if got := df.NumRows(); got != 2 {
		t.Fatalf("NumRows=%d, want 2", got)
	}

	if err := df.SortBy(SortKey{Name: "person"}); err == nil {
		t.Fatalf("expected sort error for struct column without comparator")
	}
}

func TestStructColumnWithComparatorSorts(t *testing.T) {
	people := NewColumnWithComparator("person", []person{{ID: 2, Name: "b"}, {ID: 1, Name: "a"}}, func(a, b person) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	id := NewColumn("id", []int{2, 1})

	df, err := NewFrame(people, id)
	if err != nil {
		t.Fatalf("NewFrame failed: %v", err)
	}

	if err := df.SortBy(SortKey{Name: "person"}); err != nil {
		t.Fatalf("SortBy failed: %v", err)
	}

	v, ok := Row{f: df, row: df.order[0]}.Value("id")
	if !ok || v.(int) != 1 {
		t.Fatalf("first id=%v, ok=%v, want 1,true", v, ok)
	}
}

func TestInterfaceColumnWorks(t *testing.T) {
	vals := NewColumn("value", []any{"x", 1, true})
	id := NewColumn("id", []int{3, 1, 2})

	df, err := NewFrame(vals, id)
	if err != nil {
		t.Fatalf("NewFrame failed: %v", err)
	}

	if err := df.SortBy(SortKey{Name: "id"}); err != nil {
		t.Fatalf("SortBy id failed: %v", err)
	}

	if err := df.SortBy(SortKey{Name: "value"}); err == nil {
		t.Fatalf("expected sort error for interface column without comparator")
	}
}
