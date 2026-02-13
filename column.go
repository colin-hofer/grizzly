package grizzly

import (
	"fmt"
	"reflect"
)

type CompareFunc[T any] func(a, b T) int

type Column[T any] struct {
	name    string
	data    []T
	valid   []uint64
	compare CompareFunc[T]
	dtype   DType
}

func NewColumn[T any](name string, data []T) *Column[T] {
	return NewColumnWithComparator(name, data, nil)
}

func NewColumnWithComparator[T any](name string, data []T, compare CompareFunc[T]) *Column[T] {
	col := &Column[T]{
		name:    name,
		data:    append([]T(nil), data...),
		valid:   makeFullValidity(len(data)),
		compare: compare,
	}
	col.initTypeAndComparator()
	return col
}

func NewNullableColumn[T any](name string, data []T, validMask []bool) (*Column[T], error) {
	return NewNullableColumnWithComparator(name, data, validMask, nil)
}

func NewNullableColumnWithComparator[T any](name string, data []T, validMask []bool, compare CompareFunc[T]) (*Column[T], error) {
	if len(data) != len(validMask) {
		return nil, fmt.Errorf("column %q: data and valid mask lengths differ", name)
	}
	col := &Column[T]{
		name:    name,
		data:    append([]T(nil), data...),
		valid:   makeValidityFromMask(validMask),
		compare: compare,
	}
	col.initTypeAndComparator()
	return col, nil
}

func (c *Column[T]) initTypeAndComparator() {
	c.dtype = inferColumnDType(c.data)
	if c.compare == nil {
		c.compare = defaultCompareFunc[T]()
	}
}

func (c *Column[T]) Name() string {
	return c.name
}

func (c *Column[T]) Len() int {
	return len(c.data)
}

func (c *Column[T]) DType() DType {
	return c.dtype
}

func (c *Column[T]) SupportsSort() bool {
	return c.compare != nil
}

func (c *Column[T]) ValidAt(row int) bool {
	return bitGet(c.valid, row)
}

func (c *Column[T]) ValueAt(row int) any {
	if !c.ValidAt(row) {
		return nil
	}
	return c.data[row]
}

func (c *Column[T]) CompareRows(i, j int) int {
	if c.compare == nil {
		return 0
	}
	vi := c.ValidAt(i)
	vj := c.ValidAt(j)
	if !vi && !vj {
		return 0
	}
	if !vi {
		return 1
	}
	if !vj {
		return -1
	}
	return c.compare(c.data[i], c.data[j])
}

func (c *Column[T]) Take(order []int) Series {
	out := make([]T, len(order))
	valid := make([]bool, len(order))
	for i, row := range order {
		out[i] = c.data[row]
		valid[i] = c.ValidAt(row)
	}
	next, _ := NewNullableColumnWithComparator(c.name, out, valid, c.compare)
	return next
}

func (c *Column[T]) Filter(mask []bool) Series {
	if len(mask) != c.Len() {
		panic("mask length mismatch")
	}
	out := make([]T, 0, c.Len())
	valid := make([]bool, 0, c.Len())
	for i := range c.data {
		if mask[i] {
			out = append(out, c.data[i])
			valid = append(valid, c.ValidAt(i))
		}
	}
	next, _ := NewNullableColumnWithComparator(c.name, out, valid, c.compare)
	return next
}

func inferColumnDType[T any](data []T) DType {
	for _, v := range data {
		dt := dtypeFromValue(v)
		if dt != DTypeUnknown {
			return dt
		}
		t := reflect.TypeOf(v)
		if t != nil {
			return DType(t.String())
		}
	}
	var z T
	if t := reflect.TypeOf(z); t != nil {
		dt := dtypeFromValue(z)
		if dt != DTypeUnknown {
			return dt
		}
		return DType(t.String())
	}
	return DTypeUnknown
}

func defaultCompareFunc[T any]() CompareFunc[T] {
	var z T
	t := reflect.TypeOf(z)
	if t == nil {
		return nil
	}
	switch t.Kind() {
	case reflect.String,
		reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Float32,
		reflect.Float64:
	default:
		return nil
	}
	return func(a, b T) int {
		res, ok := compareDynamicValues(any(a), any(b))
		if !ok {
			return 0
		}
		return res
	}
}

func compareDynamicValues(a, b any) (int, bool) {
	if a == nil && b == nil {
		return 0, true
	}
	if a == nil {
		return -1, true
	}
	if b == nil {
		return 1, true
	}
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0, false
		}
		if av < bv {
			return -1, true
		}
		if av > bv {
			return 1, true
		}
		return 0, true
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 0, false
		}
		if av == bv {
			return 0, true
		}
		if !av && bv {
			return -1, true
		}
		return 1, true
	case int:
		bv, ok := b.(int)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case int8:
		bv, ok := b.(int8)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case int16:
		bv, ok := b.(int16)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case int32:
		bv, ok := b.(int32)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case uint:
		bv, ok := b.(uint)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case uint8:
		bv, ok := b.(uint8)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case uint16:
		bv, ok := b.(uint16)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case uint32:
		bv, ok := b.(uint32)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case uint64:
		bv, ok := b.(uint64)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case float32:
		bv, ok := b.(float32)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0, false
		}
		return compareOrdered(av, bv), true
	default:
		return 0, false
	}
}

func compareOrdered[T ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
