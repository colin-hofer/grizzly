package grizzly

import (
	"bytes"
	"strconv"
)

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
	dtype    DType
	toString func(T) string
	less     func(a, b T) bool
}

type typedColumn[T any] struct {
	name  string
	data  []T
	valid bitmap
	ops   typeOps[T]
	build func(name string, data []T, valid bitmap) Column
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
	return c.build(c.name, out, valid.Build())
}
func (c *typedColumn[T]) Take(order []int) Column {
	out := make([]T, len(order))
	valid := bitmapBuilder{}
	for i := range order {
		row := order[i]
		out[i] = c.data[row]
		valid.Append(!c.IsNull(row))
	}
	return c.build(c.name, out, valid.Build())
}

type Int64Column struct{ typedColumn[int64] }
type Float64Column struct{ typedColumn[float64] }
type BoolColumn struct{ typedColumn[bool] }

type Utf8Column struct {
	name    string
	offsets []int32
	bytes   []byte
	valid   bitmap
}

func (c *Int64Column) Value(i int) int64     { return c.data[i] }
func (c *Float64Column) Value(i int) float64 { return c.data[i] }
func (c *BoolColumn) Value(i int) bool       { return c.data[i] }
func (c *Int64Column) Slice() []int64        { return c.data }
func (c *Float64Column) Slice() []float64    { return c.data }
func (c *BoolColumn) Slice() []bool          { return c.data }
func (c *Int64Column) Validity() bitmap      { return c.valid }
func (c *Float64Column) Validity() bitmap    { return c.valid }
func (c *BoolColumn) Validity() bitmap       { return c.valid }

func (c *Utf8Column) Name() string      { return c.name }
func (c *Utf8Column) DType() DType      { return DTypeUtf8 }
func (c *Utf8Column) Len() int          { return len(c.offsets) - 1 }
func (c *Utf8Column) IsNull(i int) bool { return !c.valid.get(i) }
func (c *Utf8Column) Validity() bitmap  { return c.valid }
func (c *Utf8Column) Slice() []string {
	out := make([]string, c.Len())
	for i := range out {
		out[i] = c.Value(i)
	}
	return out
}
func (c *Utf8Column) Value(i int) string {
	start := c.offsets[i]
	end := c.offsets[i+1]
	return string(c.bytes[start:end])
}
func (c *Utf8Column) ValueString(i int) string {
	if c.IsNull(i) {
		return ""
	}
	return c.Value(i)
}
func (c *Utf8Column) Less(i, j int) bool {
	is, ie := c.byteRange(i)
	js, je := c.byteRange(j)
	return bytes.Compare(c.bytes[is:ie], c.bytes[js:je]) < 0
}
func (c *Utf8Column) compareLiteral(i int, lit []byte) int {
	s, e := c.byteRange(i)
	return bytes.Compare(c.bytes[s:e], lit)
}
func (c *Utf8Column) valueLen(i int) int {
	s, e := c.byteRange(i)
	return e - s
}
func (c *Utf8Column) byteRange(i int) (int, int) {
	return int(c.offsets[i]), int(c.offsets[i+1])
}
func (c *Utf8Column) Filter(mask []bool) Column {
	n := 0
	for i := range mask {
		if mask[i] {
			n++
		}
	}
	offsets := make([]int32, 1, n+1)
	bytesOut := make([]byte, 0, len(c.bytes)/2)
	valid := bitmapBuilder{}
	for i := 0; i < c.Len(); i++ {
		if !mask[i] {
			continue
		}
		s, e := c.byteRange(i)
		bytesOut = append(bytesOut, c.bytes[s:e]...)
		offsets = append(offsets, int32(len(bytesOut)))
		valid.Append(!c.IsNull(i))
	}
	return newUtf8ColumnOwned(c.name, offsets, bytesOut, valid.Build())
}
func (c *Utf8Column) Take(order []int) Column {
	offsets := make([]int32, 1, len(order)+1)
	bytesOut := make([]byte, 0, len(c.bytes))
	valid := bitmapBuilder{}
	for i := range order {
		row := order[i]
		s, e := c.byteRange(row)
		bytesOut = append(bytesOut, c.bytes[s:e]...)
		offsets = append(offsets, int32(len(bytesOut)))
		valid.Append(!c.IsNull(row))
	}
	return newUtf8ColumnOwned(c.name, offsets, bytesOut, valid.Build())
}

func NewInt64Column(name string, data []int64, valid []bool) *Int64Column {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &Int64Column{typedColumn[int64]{name: name, data: append([]int64(nil), data...), valid: v, ops: int64Ops, build: newInt64ColumnOwned}}
}

func NewFloat64Column(name string, data []float64, valid []bool) *Float64Column {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &Float64Column{typedColumn[float64]{name: name, data: append([]float64(nil), data...), valid: v, ops: float64Ops, build: newFloat64ColumnOwned}}
}

func NewBoolColumn(name string, data []bool, valid []bool) *BoolColumn {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	return &BoolColumn{typedColumn[bool]{name: name, data: append([]bool(nil), data...), valid: v, ops: boolOps, build: newBoolColumnOwned}}
}

func NewUtf8Column(name string, data []string, valid []bool) *Utf8Column {
	v := bitmap{}
	if valid == nil {
		v = newBitmap(len(data), true)
	} else {
		v = newBitmapFromBools(valid)
	}
	offsets := make([]int32, 1, len(data)+1)
	buf := make([]byte, 0, len(data)*8)
	for i := range data {
		buf = append(buf, data[i]...)
		offsets = append(offsets, int32(len(buf)))
	}
	return &Utf8Column{name: name, offsets: offsets, bytes: buf, valid: v}
}

func newInt64ColumnOwned(name string, data []int64, valid bitmap) Column {
	return &Int64Column{typedColumn[int64]{name: name, data: data, valid: valid, ops: int64Ops, build: newInt64ColumnOwned}}
}

func newFloat64ColumnOwned(name string, data []float64, valid bitmap) Column {
	return &Float64Column{typedColumn[float64]{name: name, data: data, valid: valid, ops: float64Ops, build: newFloat64ColumnOwned}}
}

func newBoolColumnOwned(name string, data []bool, valid bitmap) Column {
	return &BoolColumn{typedColumn[bool]{name: name, data: data, valid: valid, ops: boolOps, build: newBoolColumnOwned}}
}

func newUtf8ColumnOwned(name string, offsets []int32, bytes []byte, valid bitmap) Column {
	return &Utf8Column{name: name, offsets: offsets, bytes: bytes, valid: valid}
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
