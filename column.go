package grizzly

import "strconv"

type Column interface {
	Name() string
	DType() DType
	Len() int
	IsNull(i int) bool
	ValueString(i int) string
	Filter(mask []bool) Column
	Take(order []int) Column
	Less(i, j int) bool
}

type typeOps[T any] struct {
	dtype     DType
	toString  func(T) string
	less      func(a, b T) bool
	fromValid func(v []bool, n int) bitmap
}

type typedColumn[T any] struct {
	name  string
	data  []T
	valid bitmap
	ops   typeOps[T]
}

func (c *typedColumn[T]) Name() string      { return c.name }
func (c *typedColumn[T]) DType() DType      { return c.ops.dtype }
func (c *typedColumn[T]) Len() int          { return len(c.data) }
func (c *typedColumn[T]) IsNull(i int) bool { return !c.valid.get(i) }
func (c *typedColumn[T]) Less(i, j int) bool {
	return c.ops.less(c.data[i], c.data[j])
}
func (c *typedColumn[T]) ValueString(i int) string {
	if c.IsNull(i) {
		return ""
	}
	return c.ops.toString(c.data[i])
}
func (c *typedColumn[T]) Filter(mask []bool) Column {
	n := 0
	for i := range mask {
		if mask[i] {
			n++
		}
	}
	out := make([]T, n)
	valid := bitmapBuilder{}
	idx := 0
	for i := range c.data {
		if !mask[i] {
			continue
		}
		out[idx] = c.data[i]
		valid.Append(!c.IsNull(i))
		idx++
	}
	return &typedColumn[T]{name: c.name, data: out, valid: valid.Build(), ops: c.ops}
}
func (c *typedColumn[T]) Take(order []int) Column {
	out := make([]T, len(order))
	valid := bitmapBuilder{}
	for i := range order {
		row := order[i]
		out[i] = c.data[row]
		valid.Append(!c.IsNull(row))
	}
	return &typedColumn[T]{name: c.name, data: out, valid: valid.Build(), ops: c.ops}
}

type Int64Column struct{ typedColumn[int64] }
type Float64Column struct{ typedColumn[float64] }
type BoolColumn struct{ typedColumn[bool] }
type Utf8Column struct{ typedColumn[string] }

func (c *Int64Column) Value(i int) int64     { return c.data[i] }
func (c *Float64Column) Value(i int) float64 { return c.data[i] }
func (c *BoolColumn) Value(i int) bool       { return c.data[i] }
func (c *Utf8Column) Value(i int) string     { return c.data[i] }
func (c *Int64Column) Slice() []int64        { return c.data }
func (c *Float64Column) Slice() []float64    { return c.data }
func (c *BoolColumn) Slice() []bool          { return c.data }
func (c *Utf8Column) Slice() []string        { return c.data }
func (c *Int64Column) Validity() bitmap      { return c.valid }
func (c *Float64Column) Validity() bitmap    { return c.valid }
func (c *BoolColumn) Validity() bitmap       { return c.valid }
func (c *Utf8Column) Validity() bitmap       { return c.valid }

func NewInt64Column(name string, data []int64, valid []bool) *Int64Column {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &Int64Column{typedColumn[int64]{name: name, data: append([]int64(nil), data...), valid: v, ops: int64Ops}}
}

func NewFloat64Column(name string, data []float64, valid []bool) *Float64Column {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &Float64Column{typedColumn[float64]{name: name, data: append([]float64(nil), data...), valid: v, ops: float64Ops}}
}

func NewBoolColumn(name string, data []bool, valid []bool) *BoolColumn {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &BoolColumn{typedColumn[bool]{name: name, data: append([]bool(nil), data...), valid: v, ops: boolOps}}
}

func NewUtf8Column(name string, data []string, valid []bool) *Utf8Column {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &Utf8Column{typedColumn[string]{name: name, data: append([]string(nil), data...), valid: v, ops: utf8Ops}}
}

func newInt64ColumnOwned(name string, data []int64, valid bitmap) *Int64Column {
	return &Int64Column{typedColumn[int64]{name: name, data: data, valid: valid, ops: int64Ops}}
}

func newFloat64ColumnOwned(name string, data []float64, valid bitmap) *Float64Column {
	return &Float64Column{typedColumn[float64]{name: name, data: data, valid: valid, ops: float64Ops}}
}

func newBoolColumnOwned(name string, data []bool, valid bitmap) *BoolColumn {
	return &BoolColumn{typedColumn[bool]{name: name, data: data, valid: valid, ops: boolOps}}
}

func newUtf8ColumnOwned(name string, data []string, valid bitmap) *Utf8Column {
	return &Utf8Column{typedColumn[string]{name: name, data: data, valid: valid, ops: utf8Ops}}
}

var int64Ops = typeOps[int64]{
	dtype:    DTypeInt64,
	toString: func(v int64) string { return strconv.FormatInt(v, 10) },
	less:     func(a, b int64) bool { return a < b },
}

var float64Ops = typeOps[float64]{
	dtype:    DTypeFloat64,
	toString: func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) },
	less:     func(a, b float64) bool { return a < b },
}

var boolOps = typeOps[bool]{
	dtype: DTypeBool,
	toString: func(v bool) string {
		if v {
			return "true"
		}
		return "false"
	},
	less: func(a, b bool) bool { return !a && b },
}

var utf8Ops = typeOps[string]{
	dtype:    DTypeUtf8,
	toString: func(v string) string { return v },
	less:     func(a, b string) bool { return a < b },
}
