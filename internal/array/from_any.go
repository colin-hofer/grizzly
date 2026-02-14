package array

import (
	"fmt"
	"reflect"
	"slices"
)

func ColumnFromAny(name string, values any) (Column, int, error) {
	switch v := values.(type) {
	case []int64:
		valid := NewBitmap(len(v), true)
		return NewInt64ColumnOwned(name, slices.Clone(v), valid), len(v), nil
	case []int:
		data := make([]int64, len(v))
		for i := range v {
			data[i] = int64(v[i])
		}
		valid := NewBitmap(len(v), true)
		return NewInt64ColumnOwned(name, data, valid), len(v), nil
	case []int32:
		data := make([]int64, len(v))
		for i := range v {
			data[i] = int64(v[i])
		}
		valid := NewBitmap(len(v), true)
		return NewInt64ColumnOwned(name, data, valid), len(v), nil
	case []int16:
		data := make([]int64, len(v))
		for i := range v {
			data[i] = int64(v[i])
		}
		valid := NewBitmap(len(v), true)
		return NewInt64ColumnOwned(name, data, valid), len(v), nil
	case []int8:
		data := make([]int64, len(v))
		for i := range v {
			data[i] = int64(v[i])
		}
		valid := NewBitmap(len(v), true)
		return NewInt64ColumnOwned(name, data, valid), len(v), nil
	case []uint64:
		data := make([]int64, len(v))
		for i := range v {
			if v[i] > uint64(MaxInt64FromAny()) {
				return nil, 0, fmt.Errorf("uint64 value overflows int64")
			}
			data[i] = int64(v[i])
		}
		valid := NewBitmap(len(v), true)
		return NewInt64ColumnOwned(name, data, valid), len(v), nil
	case []uint:
		data := make([]int64, len(v))
		for i := range v {
			if uint64(v[i]) > uint64(MaxInt64FromAny()) {
				return nil, 0, fmt.Errorf("uint value overflows int64")
			}
			data[i] = int64(v[i])
		}
		valid := NewBitmap(len(v), true)
		return NewInt64ColumnOwned(name, data, valid), len(v), nil
	case []uint32:
		data := make([]int64, len(v))
		for i := range v {
			data[i] = int64(v[i])
		}
		valid := NewBitmap(len(v), true)
		return NewInt64ColumnOwned(name, data, valid), len(v), nil
	case []uint16:
		data := make([]int64, len(v))
		for i := range v {
			data[i] = int64(v[i])
		}
		valid := NewBitmap(len(v), true)
		return NewInt64ColumnOwned(name, data, valid), len(v), nil
	case []uint8:
		data := make([]int64, len(v))
		for i := range v {
			data[i] = int64(v[i])
		}
		valid := NewBitmap(len(v), true)
		return NewInt64ColumnOwned(name, data, valid), len(v), nil
	case []float64:
		valid := NewBitmap(len(v), true)
		return NewFloat64ColumnOwned(name, slices.Clone(v), valid), len(v), nil
	case []float32:
		data := make([]float64, len(v))
		for i := range v {
			data[i] = float64(v[i])
		}
		valid := NewBitmap(len(v), true)
		return NewFloat64ColumnOwned(name, data, valid), len(v), nil
	case []bool:
		valid := NewBitmap(len(v), true)
		return NewBoolColumnOwned(name, slices.Clone(v), valid), len(v), nil
	case []string:
		col := utf8ColumnFromStrings(name, v, nil)
		return col, len(v), nil
	case []*int64:
		data := make([]int64, len(v))
		valid := make([]bool, len(v))
		for i := range v {
			if v[i] == nil {
				continue
			}
			data[i] = *v[i]
			valid[i] = true
		}
		return NewInt64ColumnOwned(name, data, NewBitmapFromBools(valid)), len(v), nil
	case []*int:
		data := make([]int64, len(v))
		valid := make([]bool, len(v))
		for i := range v {
			if v[i] == nil {
				continue
			}
			data[i] = int64(*v[i])
			valid[i] = true
		}
		return NewInt64ColumnOwned(name, data, NewBitmapFromBools(valid)), len(v), nil
	case []*uint64:
		data := make([]int64, len(v))
		valid := make([]bool, len(v))
		max := uint64(MaxInt64FromAny())
		for i := range v {
			if v[i] == nil {
				continue
			}
			if *v[i] > max {
				return nil, 0, fmt.Errorf("uint64 value overflows int64")
			}
			data[i] = int64(*v[i])
			valid[i] = true
		}
		return NewInt64ColumnOwned(name, data, NewBitmapFromBools(valid)), len(v), nil
	case []*float64:
		data := make([]float64, len(v))
		valid := make([]bool, len(v))
		for i := range v {
			if v[i] == nil {
				continue
			}
			data[i] = *v[i]
			valid[i] = true
		}
		return NewFloat64ColumnOwned(name, data, NewBitmapFromBools(valid)), len(v), nil
	case []*float32:
		data := make([]float64, len(v))
		valid := make([]bool, len(v))
		for i := range v {
			if v[i] == nil {
				continue
			}
			data[i] = float64(*v[i])
			valid[i] = true
		}
		return NewFloat64ColumnOwned(name, data, NewBitmapFromBools(valid)), len(v), nil
	case []*bool:
		data := make([]bool, len(v))
		valid := make([]bool, len(v))
		for i := range v {
			if v[i] == nil {
				continue
			}
			data[i] = *v[i]
			valid[i] = true
		}
		return NewBoolColumnOwned(name, data, NewBitmapFromBools(valid)), len(v), nil
	case []*string:
		valid := make([]bool, len(v))
		data := make([]string, len(v))
		for i := range v {
			if v[i] == nil {
				continue
			}
			data[i] = *v[i]
			valid[i] = true
		}
		col := utf8ColumnFromStrings(name, data, valid)
		return col, len(v), nil
	default:
		// Try to support slices of pointers via reflection.
		rv := reflect.ValueOf(values)
		if rv.IsValid() && rv.Kind() == reflect.Slice {
			return columnFromAnyReflect(name, rv)
		}
		return nil, 0, fmt.Errorf("unsupported column values type %T", values)
	}
}

func MaxInt64FromAny() int64 {
	return int64(^uint64(0) >> 1)
}

func columnFromAnyReflect(name string, rv reflect.Value) (Column, int, error) {
	if rv.Type().Elem().Kind() != reflect.Pointer {
		return nil, 0, fmt.Errorf("unsupported slice element type %s", rv.Type().Elem())
	}
	// Handle []*T where T is one of the supported base types.
	et := rv.Type().Elem().Elem()
	switch et.Kind() {
	case reflect.String:
		data := make([]string, rv.Len())
		valid := make([]bool, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			p := rv.Index(i)
			if p.IsNil() {
				continue
			}
			data[i] = p.Elem().String()
			valid[i] = true
		}
		return utf8ColumnFromStrings(name, data, valid), rv.Len(), nil
	case reflect.Bool:
		data := make([]bool, rv.Len())
		valid := make([]bool, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			p := rv.Index(i)
			if p.IsNil() {
				continue
			}
			data[i] = p.Elem().Bool()
			valid[i] = true
		}
		return NewBoolColumnOwned(name, data, NewBitmapFromBools(valid)), rv.Len(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		data := make([]int64, rv.Len())
		valid := make([]bool, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			p := rv.Index(i)
			if p.IsNil() {
				continue
			}
			data[i] = p.Elem().Int()
			valid[i] = true
		}
		return NewInt64ColumnOwned(name, data, NewBitmapFromBools(valid)), rv.Len(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		data := make([]int64, rv.Len())
		valid := make([]bool, rv.Len())
		max := uint64(MaxInt64FromAny())
		for i := 0; i < rv.Len(); i++ {
			p := rv.Index(i)
			if p.IsNil() {
				continue
			}
			u := p.Elem().Uint()
			if u > max {
				return nil, 0, fmt.Errorf("uint value overflows int64")
			}
			data[i] = int64(u)
			valid[i] = true
		}
		return NewInt64ColumnOwned(name, data, NewBitmapFromBools(valid)), rv.Len(), nil
	case reflect.Float32, reflect.Float64:
		data := make([]float64, rv.Len())
		valid := make([]bool, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			p := rv.Index(i)
			if p.IsNil() {
				continue
			}
			data[i] = p.Elem().Convert(reflect.TypeOf(float64(0))).Float()
			valid[i] = true
		}
		return NewFloat64ColumnOwned(name, data, NewBitmapFromBools(valid)), rv.Len(), nil
	default:
		return nil, 0, fmt.Errorf("unsupported slice element type %s", et)
	}
}

func utf8ColumnFromStrings(name string, data []string, valid []bool) Column {
	offsets := make([]int32, 1, len(data)+1)
	bytesOut := make([]byte, 0, len(data)*8)
	for i := range data {
		if valid != nil && !valid[i] {
			offsets = append(offsets, int32(len(bytesOut)))
			continue
		}
		bytesOut = append(bytesOut, data[i]...)
		offsets = append(offsets, int32(len(bytesOut)))
	}
	var bm Bitmap
	if valid == nil {
		bm = NewBitmap(len(data), true)
	} else {
		bm = NewBitmapFromBools(valid)
	}
	return NewUtf8ColumnOwned(name, offsets, bytesOut, bm)
}
