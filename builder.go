package grizzly

import (
	"errors"
	"fmt"
	"grizzly/internal/array"
	"grizzly/internal/exec"
	"sort"
	"strings"
)

// FrameBuilder is a convenience builder for constructing a DataFrame from
// in-memory Go slices without having to manually build each column.
//
// It collects errors during Add() and returns a single error from Build().
type FrameBuilder struct {
	order  []string
	cols   map[string]array.Column
	nrows  int
	setLen bool
	errs   []error
}

func NewFrameBuilder() *FrameBuilder {
	return &FrameBuilder{cols: make(map[string]array.Column, 16)}
}

// Add adds or replaces a column by name.
//
// Supported value types include:
// - []int, []int64, []int32, []int16, []int8
// - []uint, []uint64, []uint32, []uint16, []uint8 (must fit in int64)
// - []float64, []float32
// - []bool
// - []string
// - pointer slices []*T for nullable columns (T among: int*, uint*, float*, bool, string)
func (b *FrameBuilder) Add(name string, values any) *FrameBuilder {
	if b == nil {
		return b
	}
	name = strings.TrimSpace(name)
	if name == "" {
		b.errs = append(b.errs, fmt.Errorf("column name required"))
		return b
	}
	col, n, err := array.ColumnFromAny(name, values)
	if err != nil {
		b.errs = append(b.errs, fmt.Errorf("add %s: %w", name, err))
		return b
	}
	if !b.setLen {
		b.nrows = n
		b.setLen = true
	} else if n != b.nrows {
		b.errs = append(b.errs, fmt.Errorf("add %s: length %d != frame length %d", name, n, b.nrows))
		return b
	}
	if _, exists := b.cols[name]; !exists {
		b.order = append(b.order, name)
	}
	b.cols[name] = col
	return b
}

// Build returns a DataFrame with columns in insertion order.
func (b *FrameBuilder) Build() (*DataFrame, error) {
	if b == nil {
		return nil, fmt.Errorf("nil builder")
	}
	if len(b.errs) > 0 {
		return nil, joinErrors(b.errs)
	}
	if len(b.order) == 0 {
		return nil, fmt.Errorf("no columns")
	}
	internal := make([]array.Column, 0, len(b.order))
	for _, name := range b.order {
		internal = append(internal, b.cols[name])
	}
	df, err := exec.NewDataFrame(internal...)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: df}, nil
}

// FromMap constructs a DataFrame from a map of column name -> slice.
//
// Column order is deterministic (sorted by key).
func FromMap(m map[string]any) (*DataFrame, error) {
	if len(m) == 0 {
		return nil, fmt.Errorf("empty column map")
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b := NewFrameBuilder()
	for _, k := range keys {
		b.Add(k, m[k])
	}
	return b.Build()
}

func joinErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}
	var b strings.Builder
	b.WriteString("multiple errors:")
	for _, e := range errs {
		b.WriteString("\n- ")
		b.WriteString(e.Error())
	}
	return errors.New(b.String())
}
