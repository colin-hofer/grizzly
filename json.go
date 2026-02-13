package grizzly

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type jsonFrame struct {
	Columns []jsonColumn `json:"columns"`
}

type jsonColumn struct {
	Name  string            `json:"name"`
	DType DType             `json:"dtype"`
	Data  []json.RawMessage `json:"data"`
}

func (f *Frame) MarshalJSON() ([]byte, error) {
	if f == nil {
		return []byte("null"), nil
	}
	payload := jsonFrame{Columns: make([]jsonColumn, 0, len(f.columnIDs))}
	for _, id := range f.columnIDs {
		col := f.columns[id]
		out := jsonColumn{
			Name:  id,
			DType: col.DType(),
			Data:  make([]json.RawMessage, len(f.order)),
		}
		for i, row := range f.order {
			if !col.ValidAt(row) {
				out.Data[i] = json.RawMessage("null")
				continue
			}
			b, err := json.Marshal(col.ValueAt(row))
			if err != nil {
				return nil, fmt.Errorf("column %q row %d: %w", id, i, err)
			}
			out.Data[i] = b
		}
		payload.Columns = append(payload.Columns, out)
	}
	return json.Marshal(payload)
}

func (f *Frame) UnmarshalJSON(data []byte) error {
	if strings.TrimSpace(string(data)) == "null" {
		*f = Frame{}
		return nil
	}
	var payload jsonFrame
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	if len(payload.Columns) == 0 {
		return fmt.Errorf("json frame must contain at least one column")
	}
	cols := make([]Series, 0, len(payload.Columns))
	rowCount := -1
	for _, jc := range payload.Columns {
		if rowCount == -1 {
			rowCount = len(jc.Data)
		} else if rowCount != len(jc.Data) {
			return fmt.Errorf("column %q has %d rows; expected %d", jc.Name, len(jc.Data), rowCount)
		}
		col, err := decodeJSONColumn(jc)
		if err != nil {
			return err
		}
		cols = append(cols, col)
	}
	next, err := NewFrame(cols...)
	if err != nil {
		return err
	}
	*f = *next
	return nil
}

func MarshalFrameJSON(f *Frame) ([]byte, error) {
	return json.Marshal(f)
}

func UnmarshalFrameJSON(data []byte) (*Frame, error) {
	var f Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

func decodeJSONColumn(col jsonColumn) (Series, error) {
	if col.Name == "" {
		return nil, fmt.Errorf("column name cannot be empty")
	}
	switch normalizeDType(col.DType) {
	case DTypeString:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (string, error) {
			var v string
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeInt:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (int, error) {
			var v int
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeInt8:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (int8, error) {
			var v int8
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeInt16:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (int16, error) {
			var v int16
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeInt32:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (int32, error) {
			var v int32
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeInt64:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (int64, error) {
			var v int64
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeUint:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (uint, error) {
			var v uint
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeUint8:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (uint8, error) {
			var v uint8
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeUint16:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (uint16, error) {
			var v uint16
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeUint32:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (uint32, error) {
			var v uint32
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeUint64:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (uint64, error) {
			var v uint64
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeFloat32:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (float32, error) {
			var v float32
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	case DTypeFloat64:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (float64, error) {
			var v float64
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	default:
		return decodeJSONTypedColumn(col.Name, col.Data, func(raw json.RawMessage) (any, error) {
			var v any
			err := json.Unmarshal(raw, &v)
			return v, err
		})
	}
}

func decodeJSONTypedColumn[T any](name string, data []json.RawMessage, parse func(json.RawMessage) (T, error)) (Series, error) {
	values := make([]T, len(data))
	valid := make([]uint64, (len(data)+63)/64)
	for i, raw := range data {
		if jsonIsNull(raw) {
			continue
		}
		v, err := parse(raw)
		if err != nil {
			return nil, fmt.Errorf("column %q row %d: %w", name, i, err)
		}
		values[i] = v
		bitSet(valid, i)
	}
	return &Column[T]{
		name:  name,
		data:  values,
		valid: valid,
	}, nil
}

func jsonIsNull(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}
