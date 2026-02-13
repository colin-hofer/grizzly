package json

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"grizzly/internal/array"
	"grizzly/internal/exec"
	csvio "grizzly/internal/io/csv"
)

func Read(ctx context.Context, path string) (*exec.DataFrame, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Avoid per-element cancellation checks for non-cancellable contexts.
	cancellable := ctx.Done() != nil
	if cancellable {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	first, err := peekFirstNonSpaceByte(br)
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(br)
	if first == '[' {
		// Stream large top-level arrays so cancellation can interrupt scans.
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		d, ok := tok.(json.Delim)
		if !ok || d != '[' {
			return nil, fmt.Errorf("expected json array")
		}
		rows := make([]map[string]any, 0, 1024)
		const ctxCheckMask = 1024 - 1
		iter := 0
		for dec.More() {
			iter++
			if cancellable && (iter&ctxCheckMask) == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			var elem any
			if err := dec.Decode(&elem); err != nil {
				return nil, err
			}
			obj, ok := elem.(map[string]any)
			if !ok {
				obj = map[string]any{"value": elem}
			}
			rows = append(rows, obj)
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return recordsToFrame(ctx, rows)
	}

	// For object roots, decode as a whole (common for nested exports).
	var payload any
	if err := dec.Decode(&payload); err != nil {
		return nil, err
	}
	rows, err := normalizeJSONRows(ctx, payload)
	if err != nil {
		return nil, err
	}
	return recordsToFrame(ctx, rows)
}

func peekFirstNonSpaceByte(br *bufio.Reader) (byte, error) {
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		if b == ' ' || b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		if err := br.UnreadByte(); err != nil {
			return 0, err
		}
		return b, nil
	}
}

func normalizeJSONRows(ctx context.Context, v any) ([]map[string]any, error) {
	cancellable := ctx != nil && ctx.Done() != nil
	const ctxCheckMask = 1024 - 1
	switch x := v.(type) {
	case []any:
		rows := make([]map[string]any, 0, len(x))
		for i := range x {
			if cancellable && (i&ctxCheckMask) == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			obj, ok := x[i].(map[string]any)
			if !ok {
				obj = map[string]any{"value": x[i]}
			}
			rows = append(rows, obj)
		}
		return rows, nil
	case map[string]any:
		if tasks, ok := x["tasks"].([]any); ok {
			rows := make([]map[string]any, 0, len(tasks))
			for i := range tasks {
				if cancellable && (i&ctxCheckMask) == 0 {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
				}
				obj, ok := tasks[i].(map[string]any)
				if !ok {
					obj = map[string]any{"value": tasks[i]}
				}
				rows = append(rows, obj)
			}
			return rows, nil
		}
		return []map[string]any{x}, nil
	default:
		return nil, fmt.Errorf("unsupported json root type")
	}
}

func recordsToFrame(ctx context.Context, rows []map[string]any) (*exec.DataFrame, error) {
	cancellable := ctx != nil && ctx.Done() != nil
	const ctxCheckMask = 1024 - 1
	if len(rows) == 0 {
		return nil, fmt.Errorf("json rows empty")
	}
	keySet := make(map[string]struct{}, 64)
	for i := range rows {
		if cancellable && (i&ctxCheckMask) == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		for k := range rows[i] {
			keySet[k] = struct{}{}
		}
	}
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	cols := make([]array.Column, 0, len(keys))
	nulls := csvio.NewNullMatcher([]string{""})
	for _, key := range keys {
		if cancellable {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		raw := make([]string, 0, len(rows))
		for i := range rows {
			v, ok := rows[i][key]
			if !ok || v == nil {
				raw = append(raw, "")
				continue
			}
			raw = append(raw, fmt.Sprint(v))
		}
		dtype := inferType(raw, nulls)
		b := newBuilder(dtype, nulls, len(raw))
		for i := range raw {
			if cancellable && (i&ctxCheckMask) == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if err := b.Append(raw[i], i+1); err != nil {
				return nil, err
			}
		}
		cols = append(cols, b.Build(key))
	}
	return exec.NewDataFrame(cols...)
}

// minimal builder logic for JSON read path (shares NullMatcher with CSV IO).
type typedBuilder interface {
	Append(raw string, row int) error
	Build(name string) array.Column
}

type genericBuilder[T any] struct {
	data      []T
	valid     array.BitmapBuilder
	nulls     csvio.NullMatcher
	parse     func(string) (T, error)
	construct func(name string, data []T, valid array.Bitmap) array.Column
}

func (b *genericBuilder[T]) Append(raw string, row int) error {
	var zero T
	b.data = append(b.data, zero)
	if b.nulls.IsNull(raw) {
		b.valid.Append(false)
		return nil
	}
	v, err := b.parse(raw)
	if err != nil {
		return fmt.Errorf("row %d parse value: %w", row, err)
	}
	b.data[len(b.data)-1] = v
	b.valid.Append(true)
	return nil
}
func (b *genericBuilder[T]) Build(name string) array.Column {
	return b.construct(name, b.data, b.valid.Build())
}

type utf8Builder struct {
	offsets []int32
	bytes   []byte
	valid   array.BitmapBuilder
	nulls   csvio.NullMatcher
}

func (b *utf8Builder) Append(raw string, _ int) error {
	if b.nulls.IsNull(raw) {
		b.valid.Append(false)
		b.offsets = append(b.offsets, int32(len(b.bytes)))
		return nil
	}
	b.valid.Append(true)
	b.bytes = append(b.bytes, raw...)
	b.offsets = append(b.offsets, int32(len(b.bytes)))
	return nil
}
func (b *utf8Builder) Build(name string) array.Column {
	return array.NewUtf8ColumnOwned(name, b.offsets, b.bytes, b.valid.Build())
}

func newBuilder(dtype array.DataType, nulls csvio.NullMatcher, rowsCap int) typedBuilder {
	if rowsCap < 16 {
		rowsCap = 16
	}
	switch dtype.Kind {
	case array.KindInt:
		return &genericBuilder[int64]{
			data:  make([]int64, 0, rowsCap),
			nulls: nulls,
			parse: func(raw string) (int64, error) { return strconv.ParseInt(raw, 10, 64) },
			construct: func(name string, data []int64, valid array.Bitmap) array.Column {
				return array.NewInt64ColumnOwned(name, data, valid)
			},
		}
	case array.KindFloat:
		return &genericBuilder[float64]{
			data:  make([]float64, 0, rowsCap),
			nulls: nulls,
			parse: func(raw string) (float64, error) { return strconv.ParseFloat(raw, 64) },
			construct: func(name string, data []float64, valid array.Bitmap) array.Column {
				return array.NewFloat64ColumnOwned(name, data, valid)
			},
		}
	case array.KindBool:
		return &genericBuilder[bool]{
			data:  make([]bool, 0, rowsCap),
			nulls: nulls,
			parse: func(raw string) (bool, error) { return strconv.ParseBool(strings.ToLower(raw)) },
			construct: func(name string, data []bool, valid array.Bitmap) array.Column {
				return array.NewBoolColumnOwned(name, data, valid)
			},
		}
	default:
		return &utf8Builder{offsets: make([]int32, 1, rowsCap+1), bytes: make([]byte, 0, rowsCap*8), nulls: nulls}
	}
}

func inferType(values []string, nulls csvio.NullMatcher) array.DataType {
	allInt := true
	allFloat := true
	allBool := true
	for i := range values {
		if nulls.IsNull(values[i]) {
			continue
		}
		if _, err := strconv.ParseInt(values[i], 10, 64); err != nil {
			allInt = false
		}
		if _, err := strconv.ParseFloat(values[i], 64); err != nil {
			allFloat = false
		}
		if _, err := strconv.ParseBool(strings.ToLower(values[i])); err != nil {
			allBool = false
		}
		if !allInt && !allFloat && !allBool {
			return array.Utf8()
		}
	}
	if allInt {
		return array.Int(64)
	}
	if allFloat {
		return array.Float(64)
	}
	if allBool {
		return array.Bool()
	}
	return array.Utf8()
}
