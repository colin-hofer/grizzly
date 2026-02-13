package grizzly

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type CSVWriteOptions struct {
	Comma     rune
	NullValue string
	NoHeader  bool
}

type CSVReadOptions struct {
	Comma     rune
	NullValue string
	NoHeader  bool
	Schema    map[string]DType
}

func (f *Frame) WriteCSV(w io.Writer, opts CSVWriteOptions) error {
	if f == nil {
		return fmt.Errorf("frame is nil")
	}
	writer := csv.NewWriter(w)
	if opts.Comma != 0 {
		writer.Comma = opts.Comma
	}
	if !opts.NoHeader {
		if err := writer.Write(f.columnIDs); err != nil {
			return err
		}
	}
	for _, row := range f.order {
		record := make([]string, len(f.columnIDs))
		for i, name := range f.columnIDs {
			col := f.columns[name]
			if !col.ValidAt(row) {
				record[i] = opts.NullValue
				continue
			}
			record[i] = fmt.Sprint(col.ValueAt(row))
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func (f *Frame) MarshalCSV(opts CSVWriteOptions) (string, error) {
	var b strings.Builder
	if err := f.WriteCSV(&b, opts); err != nil {
		return "", err
	}
	return b.String(), nil
}

func ReadCSV(r io.Reader, opts CSVReadOptions) (*Frame, error) {
	reader := csv.NewReader(r)
	if opts.Comma != 0 {
		reader.Comma = opts.Comma
	}
	reader.ReuseRecord = true

	first, err := reader.Read()
	if err == io.EOF {
		return nil, fmt.Errorf("csv input is empty")
	}
	if err != nil {
		return nil, err
	}

	var header []string
	if opts.NoHeader {
		width := len(first)
		header = make([]string, width)
		for i := range header {
			header[i] = fmt.Sprintf("col_%d", i+1)
		}
	} else {
		header = append([]string(nil), first...)
	}

	if len(header) == 0 {
		return nil, fmt.Errorf("csv must contain at least one column")
	}
	width := len(header)
	seen := make(map[string]struct{}, width)
	for i, name := range header {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("column %d has empty name", i)
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate column %q", name)
		}
		seen[name] = struct{}{}
	}

	if hasFullSchema(header, opts.Schema) {
		return readCSVSchemaFast(reader, first, header, opts)
	}

	columnValues := make([][]string, width)
	for i := range columnValues {
		columnValues[i] = make([]string, 0, 16384)
	}

	if opts.NoHeader {
		for j := range first {
			columnValues[j] = append(columnValues[j], first[j])
		}
	}

	rowNum := 1
	if !opts.NoHeader {
		rowNum = 2
	}
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) != width {
			return nil, fmt.Errorf("row %d has %d fields; expected %d", rowNum, len(rec), width)
		}
		for j := range rec {
			columnValues[j] = append(columnValues[j], rec[j])
		}
		rowNum++
	}

	cols := make([]Series, 0, width)
	for i, name := range header {
		dtype := DTypeUnknown
		if opts.Schema != nil {
			dtype = normalizeDType(opts.Schema[name])
		}
		if dtype == DTypeUnknown {
			dtype = inferCSVDType(columnValues[i], opts.NullValue)
		}
		col, err := parseCSVColumn(name, dtype, columnValues[i], opts.NullValue)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}

	return NewFrame(cols...)
}

func hasFullSchema(header []string, schema map[string]DType) bool {
	if schema == nil {
		return false
	}
	for _, name := range header {
		if normalizeDType(schema[name]) == DTypeUnknown {
			return false
		}
	}
	return true
}

func readCSVSchemaFast(reader *csv.Reader, first []string, header []string, opts CSVReadOptions) (*Frame, error) {
	builders := make([]csvColumnBuilder, len(header))
	for i, name := range header {
		b, err := newCSVColumnBuilder(normalizeDType(opts.Schema[name]), opts.NullValue)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", name, err)
		}
		builders[i] = b
	}

	rowNum := 1
	if opts.NoHeader {
		if err := appendCSVRecord(builders, first, rowNum); err != nil {
			return nil, err
		}
		rowNum++
	} else {
		rowNum = 2
	}

	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) != len(header) {
			return nil, fmt.Errorf("row %d has %d fields; expected %d", rowNum, len(rec), len(header))
		}
		if err := appendCSVRecord(builders, rec, rowNum); err != nil {
			return nil, err
		}
		rowNum++
	}

	cols := make([]Series, len(header))
	for i, name := range header {
		cols[i] = builders[i].Finalize(name)
	}
	return NewFrame(cols...)
}

func appendCSVRecord(builders []csvColumnBuilder, rec []string, rowNum int) error {
	for i := range rec {
		if err := builders[i].Append(rec[i], rowNum); err != nil {
			return fmt.Errorf("column %d row %d: %w", i+1, rowNum, err)
		}
	}
	return nil
}

func UnmarshalCSV(data string, opts CSVReadOptions) (*Frame, error) {
	return ReadCSV(strings.NewReader(data), opts)
}

func inferCSVDType(values []string, nullValue string) DType {
	allInt := true
	allFloat := true
	hasNonNull := false
	for _, raw := range values {
		if raw == nullValue {
			continue
		}
		hasNonNull = true
		if _, err := strconv.ParseInt(raw, 10, 64); err != nil {
			allInt = false
		}
		if _, err := strconv.ParseFloat(raw, 64); err != nil {
			allFloat = false
		}
	}
	if !hasNonNull {
		return DTypeString
	}
	if allInt {
		return DTypeInt64
	}
	if allFloat {
		return DTypeFloat64
	}
	return DTypeString
}

func parseCSVColumn(name string, dtype DType, values []string, nullValue string) (Series, error) {
	switch normalizeDType(dtype) {
	case DTypeString:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (string, error) {
			return raw, nil
		})
	case DTypeInt:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (int, error) {
			v, err := strconv.ParseInt(raw, 10, strconv.IntSize)
			return int(v), err
		})
	case DTypeInt8:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (int8, error) {
			v, err := strconv.ParseInt(raw, 10, 8)
			return int8(v), err
		})
	case DTypeInt16:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (int16, error) {
			v, err := strconv.ParseInt(raw, 10, 16)
			return int16(v), err
		})
	case DTypeInt32:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (int32, error) {
			v, err := strconv.ParseInt(raw, 10, 32)
			return int32(v), err
		})
	case DTypeInt64:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (int64, error) {
			return strconv.ParseInt(raw, 10, 64)
		})
	case DTypeUint:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (uint, error) {
			v, err := strconv.ParseUint(raw, 10, strconv.IntSize)
			return uint(v), err
		})
	case DTypeUint8:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (uint8, error) {
			v, err := strconv.ParseUint(raw, 10, 8)
			return uint8(v), err
		})
	case DTypeUint16:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (uint16, error) {
			v, err := strconv.ParseUint(raw, 10, 16)
			return uint16(v), err
		})
	case DTypeUint32:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (uint32, error) {
			v, err := strconv.ParseUint(raw, 10, 32)
			return uint32(v), err
		})
	case DTypeUint64:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (uint64, error) {
			return strconv.ParseUint(raw, 10, 64)
		})
	case DTypeFloat32:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (float32, error) {
			v, err := strconv.ParseFloat(raw, 32)
			return float32(v), err
		})
	case DTypeFloat64:
		return parseCSVTypedColumn(name, values, nullValue, func(raw string) (float64, error) {
			return strconv.ParseFloat(raw, 64)
		})
	default:
		return nil, fmt.Errorf("column %q: unsupported dtype %q", name, dtype)
	}
}

func parseCSVTypedColumn[T any](name string, values []string, nullValue string, parse func(string) (T, error)) (Series, error) {
	data := make([]T, len(values))
	valid := make([]uint64, (len(values)+63)/64)
	for i, raw := range values {
		if raw == nullValue {
			continue
		}
		v, err := parse(raw)
		if err != nil {
			return nil, fmt.Errorf("column %q row %d: %w", name, i+1, err)
		}
		data[i] = v
		bitSet(valid, i)
	}
	return &Column[T]{
		name:  name,
		data:  data,
		valid: valid,
	}, nil
}

type csvColumnBuilder interface {
	Append(raw string, rowNum int) error
	Finalize(name string) Series
}

type typedCSVBuilder[T any] struct {
	data      []T
	valid     []uint64
	nullValue string
	parse     func(string) (T, error)
}

func (b *typedCSVBuilder[T]) Append(raw string, _ int) error {
	idx := len(b.data)
	var zero T
	b.data = append(b.data, zero)
	if idx%64 == 0 {
		b.valid = append(b.valid, 0)
	}
	if raw == b.nullValue {
		return nil
	}
	v, err := b.parse(raw)
	if err != nil {
		return err
	}
	b.data[idx] = v
	bitSet(b.valid, idx)
	return nil
}

func (b *typedCSVBuilder[T]) Finalize(name string) Series {
	return &Column[T]{
		name:  name,
		data:  b.data,
		valid: b.valid,
	}
}

func newCSVColumnBuilder(dtype DType, nullValue string) (csvColumnBuilder, error) {
	switch normalizeDType(dtype) {
	case DTypeString:
		return &typedCSVBuilder[string]{nullValue: nullValue, parse: func(raw string) (string, error) { return raw, nil }}, nil
	case DTypeInt:
		return &typedCSVBuilder[int]{nullValue: nullValue, parse: func(raw string) (int, error) {
			v, err := strconv.ParseInt(raw, 10, strconv.IntSize)
			return int(v), err
		}}, nil
	case DTypeInt8:
		return &typedCSVBuilder[int8]{nullValue: nullValue, parse: func(raw string) (int8, error) {
			v, err := strconv.ParseInt(raw, 10, 8)
			return int8(v), err
		}}, nil
	case DTypeInt16:
		return &typedCSVBuilder[int16]{nullValue: nullValue, parse: func(raw string) (int16, error) {
			v, err := strconv.ParseInt(raw, 10, 16)
			return int16(v), err
		}}, nil
	case DTypeInt32:
		return &typedCSVBuilder[int32]{nullValue: nullValue, parse: func(raw string) (int32, error) {
			v, err := strconv.ParseInt(raw, 10, 32)
			return int32(v), err
		}}, nil
	case DTypeInt64:
		return &typedCSVBuilder[int64]{nullValue: nullValue, parse: func(raw string) (int64, error) {
			return strconv.ParseInt(raw, 10, 64)
		}}, nil
	case DTypeUint:
		return &typedCSVBuilder[uint]{nullValue: nullValue, parse: func(raw string) (uint, error) {
			v, err := strconv.ParseUint(raw, 10, strconv.IntSize)
			return uint(v), err
		}}, nil
	case DTypeUint8:
		return &typedCSVBuilder[uint8]{nullValue: nullValue, parse: func(raw string) (uint8, error) {
			v, err := strconv.ParseUint(raw, 10, 8)
			return uint8(v), err
		}}, nil
	case DTypeUint16:
		return &typedCSVBuilder[uint16]{nullValue: nullValue, parse: func(raw string) (uint16, error) {
			v, err := strconv.ParseUint(raw, 10, 16)
			return uint16(v), err
		}}, nil
	case DTypeUint32:
		return &typedCSVBuilder[uint32]{nullValue: nullValue, parse: func(raw string) (uint32, error) {
			v, err := strconv.ParseUint(raw, 10, 32)
			return uint32(v), err
		}}, nil
	case DTypeUint64:
		return &typedCSVBuilder[uint64]{nullValue: nullValue, parse: func(raw string) (uint64, error) {
			return strconv.ParseUint(raw, 10, 64)
		}}, nil
	case DTypeFloat32:
		return &typedCSVBuilder[float32]{nullValue: nullValue, parse: func(raw string) (float32, error) {
			v, err := strconv.ParseFloat(raw, 32)
			return float32(v), err
		}}, nil
	case DTypeFloat64:
		return &typedCSVBuilder[float64]{nullValue: nullValue, parse: func(raw string) (float64, error) {
			return strconv.ParseFloat(raw, 64)
		}}, nil
	default:
		return nil, fmt.Errorf("unsupported dtype %q", dtype)
	}
}
