package json

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"

	"grizzly/internal/array"
	"grizzly/internal/exec"
)

const fastPathMaxBytes = 64 << 20

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
	if !cancellable {
		rows, usedFastPath, err := readAllRowsFast(ctx, path)
		if err != nil {
			return nil, err
		}
		if usedFastPath {
			return recordsToFrame(ctx, rows)
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

func readAllRowsFast(ctx context.Context, path string) ([]map[string]any, bool, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, false, err
	}
	if fi.Size() > fastPathMaxBytes {
		return nil, false, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, true, err
	}
	rows, err := normalizeJSONRows(ctx, payload)
	if err != nil {
		return nil, true, err
	}
	return rows, true, nil
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
	for _, key := range keys {
		if cancellable {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		dtype := inferType(rows, key)
		col, err := buildColumn(ctx, key, dtype, rows)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return exec.NewDataFrame(cols...)
}

func buildColumn(ctx context.Context, name string, dtype array.DataType, rows []map[string]any) (array.Column, error) {
	cancellable := ctx != nil && ctx.Done() != nil
	const ctxCheckMask = 1024 - 1
	switch dtype.Kind {
	case array.KindInt:
		data := make([]int64, len(rows))
		valid := array.BitmapBuilder{}
		for i := range rows {
			if cancellable && (i&ctxCheckMask) == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			v, ok := rows[i][name]
			if !ok || v == nil || isNullString(v) {
				valid.Append(false)
				continue
			}
			n, ok := toInt64(v)
			if !ok {
				return nil, fmt.Errorf("row %d parse value: cannot parse int64", i+1)
			}
			data[i] = n
			valid.Append(true)
		}
		return array.NewInt64ColumnOwned(name, data, valid.Build()), nil
	case array.KindFloat:
		data := make([]float64, len(rows))
		valid := array.BitmapBuilder{}
		for i := range rows {
			if cancellable && (i&ctxCheckMask) == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			v, ok := rows[i][name]
			if !ok || v == nil || isNullString(v) {
				valid.Append(false)
				continue
			}
			n, ok := toFloat64(v)
			if !ok {
				return nil, fmt.Errorf("row %d parse value: cannot parse float64", i+1)
			}
			data[i] = n
			valid.Append(true)
		}
		return array.NewFloat64ColumnOwned(name, data, valid.Build()), nil
	case array.KindBool:
		data := make([]bool, len(rows))
		valid := array.BitmapBuilder{}
		for i := range rows {
			if cancellable && (i&ctxCheckMask) == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			v, ok := rows[i][name]
			if !ok || v == nil || isNullString(v) {
				valid.Append(false)
				continue
			}
			n, ok := toBool(v)
			if !ok {
				return nil, fmt.Errorf("row %d parse value: cannot parse bool", i+1)
			}
			data[i] = n
			valid.Append(true)
		}
		return array.NewBoolColumnOwned(name, data, valid.Build()), nil
	default:
		offsets := make([]int32, 1, len(rows)+1)
		bytesOut := make([]byte, 0, len(rows)*8)
		valid := array.BitmapBuilder{}
		for i := range rows {
			if cancellable && (i&ctxCheckMask) == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			v, ok := rows[i][name]
			if !ok || v == nil || isNullString(v) {
				valid.Append(false)
				offsets = append(offsets, int32(len(bytesOut)))
				continue
			}
			raw := toStringValue(v)
			valid.Append(true)
			bytesOut = append(bytesOut, raw...)
			offsets = append(offsets, int32(len(bytesOut)))
		}
		return array.NewUtf8ColumnOwned(name, offsets, bytesOut, valid.Build()), nil
	}
}

func toStringValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case int64:
		return strconv.FormatInt(x, 10)
	case int:
		return strconv.Itoa(x)
	default:
		return fmt.Sprint(v)
	}
}

func inferType(rows []map[string]any, key string) array.DataType {
	allInt := true
	allFloat := true
	allBool := true
	for i := range rows {
		v, ok := rows[i][key]
		if !ok || v == nil || isNullString(v) {
			continue
		}
		if _, ok := toInt64(v); !ok {
			allInt = false
		}
		if _, ok := toFloat64(v); !ok {
			allFloat = false
		}
		if _, ok := toBool(v); !ok {
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

func isNullString(v any) bool {
	s, ok := v.(string)
	return ok && s == ""
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case float64:
		if x < float64(-1<<63) || x > float64((1<<63)-1) {
			return 0, false
		}
		n := int64(x)
		if float64(n) != x {
			return 0, false
		}
		return n, true
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, false
		}
		return x, true
	case int64:
		return float64(x), true
	case int:
		return float64(x), true
	case string:
		n, err := strconv.ParseFloat(x, 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func toBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case string:
		b, err := parseBoolASCII(x)
		return b, err == nil
	default:
		return false, false
	}
}

func parseBoolASCII(raw string) (bool, error) {
	if len(raw) == 4 {
		if (raw[0] == 't' || raw[0] == 'T') &&
			(raw[1] == 'r' || raw[1] == 'R') &&
			(raw[2] == 'u' || raw[2] == 'U') &&
			(raw[3] == 'e' || raw[3] == 'E') {
			return true, nil
		}
	}
	if len(raw) == 5 {
		if (raw[0] == 'f' || raw[0] == 'F') &&
			(raw[1] == 'a' || raw[1] == 'A') &&
			(raw[2] == 'l' || raw[2] == 'L') &&
			(raw[3] == 's' || raw[3] == 'S') &&
			(raw[4] == 'e' || raw[4] == 'E') {
			return false, nil
		}
	}
	return strconv.ParseBool(raw)
}
