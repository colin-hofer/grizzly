package array

import (
	"bytes"
	"fmt"
	"strconv"
)

// Column is the internal column interface used by execution kernels.
// It is intentionally small: no mutating access is exposed.
type Column interface {
	Name() string
	DType() DataType
	Len() int
	IsNull(i int) bool
	ValueString(i int) string
	Filter(mask []bool) Column
	Take(order []int) Column
}

type typeOps[T any] struct {
	dtype    DataType
	toString func(T) string
}

type typedColumn[T any] struct {
	name  string
	data  []T
	valid Bitmap
	ops   typeOps[T]
	build func(name string, data []T, valid Bitmap) Column
}

func (c *typedColumn[T]) Name() string      { return c.name }
func (c *typedColumn[T]) DType() DataType   { return c.ops.dtype }
func (c *typedColumn[T]) Len() int          { return len(c.data) }
func (c *typedColumn[T]) IsNull(i int) bool { return !c.valid.Get(i) }
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
	valid := BitmapBuilder{}
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
	valid := BitmapBuilder{}
	for i := range order {
		row := order[i]
		out[i] = c.data[row]
		valid.Append(!c.IsNull(row))
	}
	return c.build(c.name, out, valid.Build())
}

type (
	Int64Column   struct{ typedColumn[int64] }
	Float64Column struct{ typedColumn[float64] }
	BoolColumn    struct{ typedColumn[bool] }
)

type Utf8Column struct {
	name    string
	offsets []int32
	bytes   []byte
	valid   Bitmap
}

func (c *Int64Column) Value(i int) int64     { return c.data[i] }
func (c *Float64Column) Value(i int) float64 { return c.data[i] }
func (c *BoolColumn) Value(i int) bool       { return c.data[i] }
func (c *Int64Column) Values() []int64 {
	out := make([]int64, len(c.data))
	copy(out, c.data)
	return out
}

func (c *Float64Column) Values() []float64 {
	out := make([]float64, len(c.data))
	copy(out, c.data)
	return out
}
func (c *BoolColumn) Values() []bool      { out := make([]bool, len(c.data)); copy(out, c.data); return out }
func (c *Int64Column) Validity() Bitmap   { return c.valid }
func (c *Float64Column) Validity() Bitmap { return c.valid }
func (c *BoolColumn) Validity() Bitmap    { return c.valid }
func (c *Utf8Column) Name() string        { return c.name }
func (c *Utf8Column) DType() DataType     { return Utf8() }
func (c *Utf8Column) Len() int            { return len(c.offsets) - 1 }
func (c *Utf8Column) IsNull(i int) bool   { return !c.valid.Get(i) }
func (c *Utf8Column) Validity() Bitmap    { return c.valid }
func (c *Utf8Column) Values() []string {
	out := make([]string, c.Len())
	for i := range out {
		out[i] = c.Value(i)
	}
	return out
}
func (c *Utf8Column) byteRange(i int) (int, int) { return int(c.offsets[i]), int(c.offsets[i+1]) }
func (c *Utf8Column) ByteRange(i int) (int, int) { return c.byteRange(i) }
func (c *Utf8Column) Bytes() []byte              { return c.bytes }
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

func (c *Utf8Column) CompareRows(i, j int) int {
	is, ie := c.byteRange(i)
	js, je := c.byteRange(j)
	return bytes.Compare(c.bytes[is:ie], c.bytes[js:je])
}

func (c *Utf8Column) CompareLiteral(i int, lit []byte) int {
	s, e := c.byteRange(i)
	return bytes.Compare(c.bytes[s:e], lit)
}

func (c *Utf8Column) ValueLen(i int) int {
	s, e := c.byteRange(i)
	return e - s
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
	valid := BitmapBuilder{}
	for i := 0; i < c.Len(); i++ {
		if !mask[i] {
			continue
		}
		s, e := c.byteRange(i)
		bytesOut = append(bytesOut, c.bytes[s:e]...)
		offsets = append(offsets, int32(len(bytesOut)))
		valid.Append(!c.IsNull(i))
	}
	return NewUtf8ColumnOwned(c.name, offsets, bytesOut, valid.Build())
}

func (c *Utf8Column) Take(order []int) Column {
	offsets := make([]int32, 1, len(order)+1)
	bytesOut := make([]byte, 0, len(c.bytes))
	valid := BitmapBuilder{}
	for i := range order {
		row := order[i]
		s, e := c.byteRange(row)
		bytesOut = append(bytesOut, c.bytes[s:e]...)
		offsets = append(offsets, int32(len(bytesOut)))
		valid.Append(!c.IsNull(row))
	}
	return NewUtf8ColumnOwned(c.name, offsets, bytesOut, valid.Build())
}

func NewInt64Column(name string, data []int64, valid []bool) (*Int64Column, error) {
	var v Bitmap
	if valid == nil {
		v = NewBitmap(len(data), true)
	} else {
		if len(valid) != len(data) {
			return nil, fmt.Errorf("valid length %d != data length %d", len(valid), len(data))
		}
		v = NewBitmapFromBools(valid)
	}
	return &Int64Column{typedColumn[int64]{name: name, data: append([]int64(nil), data...), valid: v, ops: int64Ops, build: NewInt64ColumnOwned}}, nil
}

func NewFloat64Column(name string, data []float64, valid []bool) (*Float64Column, error) {
	var v Bitmap
	if valid == nil {
		v = NewBitmap(len(data), true)
	} else {
		if len(valid) != len(data) {
			return nil, fmt.Errorf("valid length %d != data length %d", len(valid), len(data))
		}
		v = NewBitmapFromBools(valid)
	}
	return &Float64Column{typedColumn[float64]{name: name, data: append([]float64(nil), data...), valid: v, ops: float64Ops, build: NewFloat64ColumnOwned}}, nil
}

func NewBoolColumn(name string, data []bool, valid []bool) (*BoolColumn, error) {
	var v Bitmap
	if valid == nil {
		v = NewBitmap(len(data), true)
	} else {
		if len(valid) != len(data) {
			return nil, fmt.Errorf("valid length %d != data length %d", len(valid), len(data))
		}
		v = NewBitmapFromBools(valid)
	}
	return &BoolColumn{typedColumn[bool]{name: name, data: append([]bool(nil), data...), valid: v, ops: boolOps, build: NewBoolColumnOwned}}, nil
}

func NewUtf8Column(name string, data []string, valid []bool) (*Utf8Column, error) {
	var v Bitmap
	if valid == nil {
		v = NewBitmap(len(data), true)
	} else {
		if len(valid) != len(data) {
			return nil, fmt.Errorf("valid length %d != data length %d", len(valid), len(data))
		}
		v = NewBitmapFromBools(valid)
	}
	offsets := make([]int32, 1, len(data)+1)
	buf := make([]byte, 0, len(data)*8)
	for i := range data {
		buf = append(buf, data[i]...)
		offsets = append(offsets, int32(len(buf)))
	}
	return &Utf8Column{name: name, offsets: offsets, bytes: buf, valid: v}, nil
}

func MustNewInt64Column(name string, data []int64, valid []bool) *Int64Column {
	c, err := NewInt64Column(name, data, valid)
	if err != nil {
		panic(err)
	}
	return c
}

func MustNewFloat64Column(name string, data []float64, valid []bool) *Float64Column {
	c, err := NewFloat64Column(name, data, valid)
	if err != nil {
		panic(err)
	}
	return c
}

func MustNewBoolColumn(name string, data []bool, valid []bool) *BoolColumn {
	c, err := NewBoolColumn(name, data, valid)
	if err != nil {
		panic(err)
	}
	return c
}

func MustNewUtf8Column(name string, data []string, valid []bool) *Utf8Column {
	c, err := NewUtf8Column(name, data, valid)
	if err != nil {
		panic(err)
	}
	return c
}

// Owned constructors: avoid copying input slices. These are internal APIs used
// by IO and execution kernels.
func NewInt64ColumnOwned(name string, data []int64, valid Bitmap) Column {
	return &Int64Column{typedColumn[int64]{name: name, data: data, valid: valid, ops: int64Ops, build: NewInt64ColumnOwned}}
}

func NewFloat64ColumnOwned(name string, data []float64, valid Bitmap) Column {
	return &Float64Column{typedColumn[float64]{name: name, data: data, valid: valid, ops: float64Ops, build: NewFloat64ColumnOwned}}
}

func NewBoolColumnOwned(name string, data []bool, valid Bitmap) Column {
	return &BoolColumn{typedColumn[bool]{name: name, data: data, valid: valid, ops: boolOps, build: NewBoolColumnOwned}}
}

func NewUtf8ColumnOwned(name string, offsets []int32, bytes []byte, valid Bitmap) Column {
	return &Utf8Column{name: name, offsets: offsets, bytes: bytes, valid: valid}
}

var int64Ops = typeOps[int64]{
	dtype:    Int(64),
	toString: func(v int64) string { return strconv.FormatInt(v, 10) },
}

var float64Ops = typeOps[float64]{
	dtype:    Float(64),
	toString: func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) },
}

var boolOps = typeOps[bool]{
	dtype: Bool(),
	toString: func(v bool) string {
		if v {
			return "true"
		}
		return "false"
	},
}
