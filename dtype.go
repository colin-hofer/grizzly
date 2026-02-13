package grizzly

import (
	"fmt"
	"reflect"
	"strings"
)

type DType string

const (
	DTypeUnknown DType = ""
	DTypeString  DType = "string"
	DTypeInt     DType = "int"
	DTypeInt8    DType = "int8"
	DTypeInt16   DType = "int16"
	DTypeInt32   DType = "int32"
	DTypeInt64   DType = "int64"
	DTypeUint    DType = "uint"
	DTypeUint8   DType = "uint8"
	DTypeUint16  DType = "uint16"
	DTypeUint32  DType = "uint32"
	DTypeUint64  DType = "uint64"
	DTypeUintptr DType = "uintptr"
	DTypeFloat32 DType = "float32"
	DTypeFloat64 DType = "float64"
)

func (d DType) String() string {
	return string(d)
}

func ParseDType(s string) (DType, error) {
	d := normalizeDType(s)
	if d == DTypeUnknown {
		return DTypeUnknown, fmt.Errorf("empty dtype")
	}
	return d, nil
}

func normalizeDType[T ~string](s T) DType {
	normalized := strings.TrimSpace(string(s))
	normalized = strings.ToLower(normalized)
	return DType(normalized)
}

func dtypeFromValue(v any) DType {
	t := reflect.TypeOf(v)
	if t == nil {
		return DTypeUnknown
	}
	switch t.Kind() {
	case reflect.String:
		return DTypeString
	case reflect.Int:
		return DTypeInt
	case reflect.Int8:
		return DTypeInt8
	case reflect.Int16:
		return DTypeInt16
	case reflect.Int32:
		return DTypeInt32
	case reflect.Int64:
		return DTypeInt64
	case reflect.Uint:
		return DTypeUint
	case reflect.Uint8:
		return DTypeUint8
	case reflect.Uint16:
		return DTypeUint16
	case reflect.Uint32:
		return DTypeUint32
	case reflect.Uint64:
		return DTypeUint64
	case reflect.Uintptr:
		return DTypeUintptr
	case reflect.Float32:
		return DTypeFloat32
	case reflect.Float64:
		return DTypeFloat64
	default:
		return DTypeUnknown
	}
}
