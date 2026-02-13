package grizzly

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func readJSON(path string) (*DataFrame, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var payload any
	if err := json.Unmarshal(b, &payload); err != nil {
		return nil, err
	}
	rows, err := normalizeJSONRows(payload)
	if err != nil {
		return nil, err
	}
	return recordsToFrame(rows)
}

func normalizeJSONRows(v any) ([]map[string]any, error) {
	switch x := v.(type) {
	case []any:
		rows := make([]map[string]any, 0, len(x))
		for i := range x {
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

func recordsToFrame(rows []map[string]any) (*DataFrame, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("json rows empty")
	}
	keySet := make(map[string]struct{}, 64)
	for i := range rows {
		for k := range rows[i] {
			keySet[k] = struct{}{}
		}
	}
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	cols := make([]Column, 0, len(keys))
	nullSet := newNullMatcher([]string{""})
	for _, key := range keys {
		raw := make([]string, 0, len(rows))
		for i := range rows {
			v, ok := rows[i][key]
			if !ok || v == nil {
				raw = append(raw, "")
				continue
			}
			raw = append(raw, fmt.Sprint(v))
		}
		b := newBuilder(inferType(raw, nullSet), nullSet, len(raw))
		for i := range raw {
			if err := b.Append(raw[i], i+1); err != nil {
				return nil, err
			}
		}
		cols = append(cols, b.Build(key))
	}
	return NewDataFrame(cols...)
}
