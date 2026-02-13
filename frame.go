package grizzly

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

type SortKey struct {
	Name string
	Desc bool
}

type Series interface {
	Name() string
	Len() int
	DType() DType
	SupportsSort() bool
	ValidAt(row int) bool
	ValueAt(row int) any
	CompareRows(i, j int) int
	Take(order []int) Series
	Filter(mask []bool) Series
}

type Frame struct {
	columns   map[string]Series
	columnIDs []string
	nRows     int
	order     []int
}

func NewFrame(columns ...Series) (*Frame, error) {
	if len(columns) == 0 {
		return nil, errors.New("at least one column is required")
	}
	n := columns[0].Len()
	cols := make(map[string]Series, len(columns))
	ids := make([]string, 0, len(columns))
	for _, c := range columns {
		if c.Len() != n {
			return nil, fmt.Errorf("column %q has length %d; expected %d", c.Name(), c.Len(), n)
		}
		if _, exists := cols[c.Name()]; exists {
			return nil, fmt.Errorf("duplicate column name %q", c.Name())
		}
		cols[c.Name()] = c
		ids = append(ids, c.Name())
	}
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	return &Frame{columns: cols, columnIDs: ids, nRows: n, order: order}, nil
}

func (f *Frame) NumRows() int {
	return len(f.order)
}

func (f *Frame) NumCols() int {
	return len(f.columnIDs)
}

func (f *Frame) SortBy(keys ...SortKey) error {
	if len(keys) == 0 {
		return errors.New("at least one sort key is required")
	}
	for _, k := range keys {
		col, ok := f.columns[k.Name]
		if !ok {
			return fmt.Errorf("unknown sort column %q", k.Name)
		}
		if !col.SupportsSort() {
			return fmt.Errorf("column %q (dtype %q) is not sortable without a comparator", k.Name, col.DType())
		}
	}
	sort.SliceStable(f.order, func(a, b int) bool {
		ra := f.order[a]
		rb := f.order[b]
		for _, key := range keys {
			cmpResult := f.columns[key.Name].CompareRows(ra, rb)
			if cmpResult == 0 {
				continue
			}
			if key.Desc {
				return cmpResult > 0
			}
			return cmpResult < 0
		}
		return ra < rb
	})
	return nil
}

func (f *Frame) Filter(pred func(Row) bool) *Frame {
	nextOrder := make([]int, 0, len(f.order))
	for _, physicalRow := range f.order {
		if pred(Row{f: f, row: physicalRow}) {
			nextOrder = append(nextOrder, physicalRow)
		}
	}
	out := *f
	out.order = nextOrder
	return &out
}

func (f *Frame) Materialize() (*Frame, error) {
	newCols := make([]Series, 0, len(f.columnIDs))
	for _, id := range f.columnIDs {
		newCols = append(newCols, f.columns[id].Take(f.order))
	}
	return NewFrame(newCols...)
}

func (f *Frame) Head(n int) string {
	if n > len(f.order) {
		n = len(f.order)
	}
	var b strings.Builder
	for i, id := range f.columnIDs {
		if i > 0 {
			b.WriteString("\t")
		}
		b.WriteString(id)
	}
	b.WriteString("\n")
	for i := 0; i < n; i++ {
		row := f.order[i]
		for j, id := range f.columnIDs {
			if j > 0 {
				b.WriteString("\t")
			}
			v := f.columns[id].ValueAt(row)
			if v == nil {
				b.WriteString("null")
			} else {
				b.WriteString(fmt.Sprint(v))
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}

func makeFullValidity(n int) []uint64 {
	bits := make([]uint64, (n+63)/64)
	for i := range bits {
		bits[i] = ^uint64(0)
	}
	if n%64 != 0 {
		bits[len(bits)-1] = (uint64(1) << uint(n%64)) - 1
	}
	return bits
}

func makeValidityFromMask(mask []bool) []uint64 {
	bits := make([]uint64, (len(mask)+63)/64)
	for i, ok := range mask {
		if ok {
			bits[i/64] |= uint64(1) << uint(i%64)
		}
	}
	return bits
}

func bitGet(bits []uint64, i int) bool {
	return (bits[i/64]>>(uint(i%64)))&1 == 1
}

func bitSet(bits []uint64, i int) {
	bits[i/64] |= uint64(1) << uint(i%64)
}
