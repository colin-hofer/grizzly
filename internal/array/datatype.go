package array

import (
	"fmt"
	"strings"
)

type Kind uint8

const (
	KindInvalid Kind = iota
	KindInt
	KindUInt
	KindFloat
	KindBool
	KindUtf8
	KindBinary
	KindDate
	KindDatetime
	KindDuration
)

type TimeUnit uint8

const (
	TimeUnitInvalid TimeUnit = iota
	TimeUnitNS
	TimeUnitUS
	TimeUnitMS
)

// DataType describes a column's logical type.
//
// It is intentionally a small value type.
// Use Kind + parameters rather than a wide interface hierarchy.
type DataType struct {
	Kind Kind
	Bits uint8
	Unit TimeUnit
	TZ   string
}

func (t DataType) String() string {
	switch t.Kind {
	case KindInt:
		return fmt.Sprintf("int%d", t.Bits)
	case KindUInt:
		return fmt.Sprintf("uint%d", t.Bits)
	case KindFloat:
		return fmt.Sprintf("float%d", t.Bits)
	case KindBool:
		return "bool"
	case KindUtf8:
		return "utf8"
	case KindBinary:
		return "binary"
	case KindDate:
		return "date"
	case KindDatetime:
		if t.Unit == TimeUnitInvalid {
			return "datetime"
		}
		unit := strings.ToLower(t.Unit.String())
		if t.TZ == "" {
			return fmt.Sprintf("datetime[%s]", unit)
		}
		return fmt.Sprintf("datetime[%s,%s]", unit, t.TZ)
	case KindDuration:
		if t.Unit == TimeUnitInvalid {
			return "duration"
		}
		return fmt.Sprintf("duration[%s]", strings.ToLower(t.Unit.String()))
	default:
		return "invalid"
	}
}

func (u TimeUnit) String() string {
	switch u {
	case TimeUnitNS:
		return "ns"
	case TimeUnitUS:
		return "us"
	case TimeUnitMS:
		return "ms"
	default:
		return "invalid"
	}
}

func Int(bits uint8) DataType   { return DataType{Kind: KindInt, Bits: bits} }
func UInt(bits uint8) DataType  { return DataType{Kind: KindUInt, Bits: bits} }
func Float(bits uint8) DataType { return DataType{Kind: KindFloat, Bits: bits} }
func Bool() DataType            { return DataType{Kind: KindBool} }
func Utf8() DataType            { return DataType{Kind: KindUtf8} }
func Binary() DataType          { return DataType{Kind: KindBinary} }
func Date() DataType            { return DataType{Kind: KindDate} }
func Datetime(unit TimeUnit, tz string) DataType {
	return DataType{Kind: KindDatetime, Unit: unit, TZ: tz}
}
func Duration(unit TimeUnit) DataType { return DataType{Kind: KindDuration, Unit: unit} }
