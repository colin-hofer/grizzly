package grizzly

import (
	"fmt"
	"grizzly/internal/array"
	"grizzly/internal/exec"
	"reflect"
	"strings"
)

type FromStructsOption func(*fromStructsOptions)

type fromStructsOptions struct {
	useJSONTags bool
	tagName     string
}

// FromStructsUseJSONTags enables using `json:"name"` tags for column names.
//
// If a field also has a `grizzly:"..."` tag (or custom tag name), that takes priority.
func FromStructsUseJSONTags() FromStructsOption {
	return func(o *fromStructsOptions) { o.useJSONTags = true }
}

// FromStructsTag sets the tag name used for column naming (default: "grizzly").
// Supported values include "grizzly" (recommended) or "db".
func FromStructsTag(tag string) FromStructsOption {
	return func(o *fromStructsOptions) {
		tag = strings.TrimSpace(tag)
		if tag != "" {
			o.tagName = tag
		}
	}
}

// FromStructs constructs a DataFrame from a slice of structs.
//
// Nullability rules:
// - Pointer fields (*T) become nullable columns (nil => NULL)
// - Non-pointer fields are always valid
//
// Supported field types:
// - integers: int*, uint* (uint values must fit in int64)
// - floats: float32, float64
// - bool
// - string
func FromStructs[T any](rows []T, opts ...FromStructsOption) (*DataFrame, error) {
	o := fromStructsOptions{tagName: "grizzly"}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}

	t := reflect.TypeOf((*T)(nil)).Elem()
	ptrElem := false
	if t.Kind() == reflect.Pointer {
		ptrElem = true
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("FromStructs requires struct element type")
	}

	fields, err := structFields(t, o)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("no supported exported fields")
	}

	builders := make([]structColBuilder, len(fields))
	for i := range fields {
		builders[i] = newStructBuilder(fields[i])
	}

	sv := reflect.ValueOf(rows)
	for i := 0; i < sv.Len(); i++ {
		rv := sv.Index(i)
		if ptrElem {
			if rv.IsNil() {
				for j := range builders {
					builders[j].appendNull()
				}
				continue
			}
			rv = rv.Elem()
		}
		for j := range fields {
			fv := rv.Field(fields[j].index)
			if fields[j].nullable {
				if fv.IsNil() {
					builders[j].appendNull()
					continue
				}
				fv = fv.Elem()
			}
			if err := builders[j].appendValue(fv); err != nil {
				return nil, fmt.Errorf("row %d field %s: %w", i, fields[j].name, err)
			}
		}
	}

	cols := make([]array.Column, len(fields))
	for i := range fields {
		cols[i] = builders[i].build(fields[i].name)
	}
	df, err := exec.NewDataFrame(cols...)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: df}, nil
}

type structField struct {
	name     string
	index    int
	nullable bool
	kind     structKind
}

type structKind uint8

const (
	skInvalid structKind = iota
	skInt64
	skFloat64
	skBool
	skUtf8
)

func structFields(t reflect.Type, o fromStructsOptions) ([]structField, error) {
	out := make([]structField, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		name, ok := fieldName(sf, o)
		if !ok {
			continue
		}
		k, nullable, ok := fieldKind(sf.Type)
		if !ok {
			continue
		}
		out = append(out, structField{name: name, index: i, nullable: nullable, kind: k})
	}
	// Detect duplicates early.
	seen := map[string]struct{}{}
	for i := range out {
		if _, ok := seen[out[i].name]; ok {
			return nil, fmt.Errorf("duplicate column name %q", out[i].name)
		}
		seen[out[i].name] = struct{}{}
	}
	return out, nil
}

func fieldName(sf reflect.StructField, o fromStructsOptions) (string, bool) {
	// Tag precedence:
	// 1) custom tag (default: grizzly)
	// 2) json tag if enabled
	// 3) field name
	if o.tagName != "" {
		tag := sf.Tag.Get(o.tagName)
		tag = strings.TrimSpace(tag)
		if tag == "-" {
			return "", false
		}
		if tag != "" {
			return tag, true
		}
	}
	if o.useJSONTags {
		jt := sf.Tag.Get("json")
		if jt != "" {
			parts := strings.Split(jt, ",")
			name := strings.TrimSpace(parts[0])
			if name == "-" {
				return "", false
			}
			if name != "" {
				return name, true
			}
		}
	}
	return sf.Name, true
}

func fieldKind(t reflect.Type) (structKind, bool, bool) {
	nullable := false
	if t.Kind() == reflect.Pointer {
		nullable = true
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return skUtf8, nullable, true
	case reflect.Bool:
		return skBool, nullable, true
	case reflect.Float32, reflect.Float64:
		return skFloat64, nullable, true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return skInt64, nullable, true
	default:
		return skInvalid, false, false
	}
}

type structColBuilder interface {
	appendNull()
	appendValue(v reflect.Value) error
	build(name string) array.Column
}

func newStructBuilder(f structField) structColBuilder {
	switch f.kind {
	case skInt64:
		return &int64StructBuilder{nullable: f.nullable}
	case skFloat64:
		return &float64StructBuilder{nullable: f.nullable}
	case skBool:
		return &boolStructBuilder{nullable: f.nullable}
	case skUtf8:
		return &utf8StructBuilder{nullable: f.nullable}
	default:
		return &utf8StructBuilder{nullable: true}
	}
}

type int64StructBuilder struct {
	data     []int64
	valid    []bool
	nullable bool
}

func (b *int64StructBuilder) appendNull() {
	b.data = append(b.data, 0)
	if b.nullable {
		b.valid = append(b.valid, false)
	}
}

func (b *int64StructBuilder) appendValue(v reflect.Value) error {
	b.data = append(b.data, 0)
	idx := len(b.data) - 1
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.data[idx] = v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := v.Uint()
		if u > uint64(array.MaxInt64FromAny()) {
			return fmt.Errorf("uint value overflows int64")
		}
		b.data[idx] = int64(u)
	default:
		return fmt.Errorf("unsupported int kind %s", v.Kind())
	}
	if b.nullable {
		b.valid = append(b.valid, true)
	}
	return nil
}

func (b *int64StructBuilder) build(name string) array.Column {
	if !b.nullable {
		return array.NewInt64ColumnOwned(name, b.data, array.NewBitmap(len(b.data), true))
	}
	return array.NewInt64ColumnOwned(name, b.data, array.NewBitmapFromBools(b.valid))
}

type float64StructBuilder struct {
	data     []float64
	valid    []bool
	nullable bool
}

func (b *float64StructBuilder) appendNull() {
	b.data = append(b.data, 0)
	if b.nullable {
		b.valid = append(b.valid, false)
	}
}

func (b *float64StructBuilder) appendValue(v reflect.Value) error {
	b.data = append(b.data, 0)
	idx := len(b.data) - 1
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		b.data[idx] = v.Convert(reflect.TypeOf(float64(0))).Float()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.data[idx] = float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := v.Uint()
		if u > uint64(array.MaxInt64FromAny()) {
			return fmt.Errorf("uint value overflows float64 safe range")
		}
		b.data[idx] = float64(u)
	default:
		return fmt.Errorf("unsupported float kind %s", v.Kind())
	}
	if b.nullable {
		b.valid = append(b.valid, true)
	}
	return nil
}

func (b *float64StructBuilder) build(name string) array.Column {
	if !b.nullable {
		return array.NewFloat64ColumnOwned(name, b.data, array.NewBitmap(len(b.data), true))
	}
	return array.NewFloat64ColumnOwned(name, b.data, array.NewBitmapFromBools(b.valid))
}

type boolStructBuilder struct {
	data     []bool
	valid    []bool
	nullable bool
}

func (b *boolStructBuilder) appendNull() {
	b.data = append(b.data, false)
	if b.nullable {
		b.valid = append(b.valid, false)
	}
}

func (b *boolStructBuilder) appendValue(v reflect.Value) error {
	if v.Kind() != reflect.Bool {
		return fmt.Errorf("unsupported bool kind %s", v.Kind())
	}
	b.data = append(b.data, v.Bool())
	if b.nullable {
		b.valid = append(b.valid, true)
	}
	return nil
}

func (b *boolStructBuilder) build(name string) array.Column {
	if !b.nullable {
		return array.NewBoolColumnOwned(name, b.data, array.NewBitmap(len(b.data), true))
	}
	return array.NewBoolColumnOwned(name, b.data, array.NewBitmapFromBools(b.valid))
}

type utf8StructBuilder struct {
	bytes    []byte
	offsets  []int32
	valid    []bool
	nullable bool
}

func (b *utf8StructBuilder) ensureInit() {
	if b.offsets == nil {
		b.offsets = make([]int32, 1, 64)
	}
}

func (b *utf8StructBuilder) appendNull() {
	b.ensureInit()
	if b.nullable {
		b.valid = append(b.valid, false)
	}
	b.offsets = append(b.offsets, int32(len(b.bytes)))
}

func (b *utf8StructBuilder) appendValue(v reflect.Value) error {
	b.ensureInit()
	if v.Kind() != reflect.String {
		return fmt.Errorf("unsupported string kind %s", v.Kind())
	}
	s := v.String()
	b.bytes = append(b.bytes, s...)
	b.offsets = append(b.offsets, int32(len(b.bytes)))
	if b.nullable {
		b.valid = append(b.valid, true)
	}
	return nil
}

func (b *utf8StructBuilder) build(name string) array.Column {
	// Handle the case where there were 0 rows.
	if b.offsets == nil {
		b.offsets = []int32{0}
	}
	var bm array.Bitmap
	if !b.nullable {
		bm = array.NewBitmap(len(b.offsets)-1, true)
	} else {
		bm = array.NewBitmapFromBools(b.valid)
	}
	return array.NewUtf8ColumnOwned(name, b.offsets, b.bytes, bm)
}
