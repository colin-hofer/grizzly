package exec

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"grizzly/internal/array"
	"grizzly/internal/expr"
	"hash"
	"io"
	"runtime"
	"sort"
	"strconv"
	"sync"
)

const parallelSortThreshold = 250000

type DataFrame struct {
	columns []array.Column
	index   map[string]int
	nrows   int
	schema  array.Schema
}

func NewDataFrame(cols ...array.Column) (*DataFrame, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("at least one column required")
	}
	n := cols[0].Len()
	idx := make(map[string]int, len(cols))
	fields := make([]array.Field, len(cols))
	for i := range cols {
		if cols[i].Len() != n {
			return nil, fmt.Errorf("column %s has mismatched length", cols[i].Name())
		}
		if _, ok := idx[cols[i].Name()]; ok {
			return nil, fmt.Errorf("duplicate column %s", cols[i].Name())
		}
		idx[cols[i].Name()] = i
		fields[i] = array.Field{Name: cols[i].Name(), Type: cols[i].DType()}
	}
	schema, err := array.NewSchema(fields)
	if err != nil {
		return nil, err
	}
	return &DataFrame{columns: cols, index: idx, nrows: n, schema: schema}, nil
}

func (df *DataFrame) Height() int          { return df.nrows }
func (df *DataFrame) Width() int           { return len(df.columns) }
func (df *DataFrame) Schema() array.Schema { return df.schema }

func (df *DataFrame) Columns() []array.Column {
	out := make([]array.Column, len(df.columns))
	copy(out, df.columns)
	return out
}

func (df *DataFrame) Column(name string) (array.Column, bool) {
	i, ok := df.index[name]
	if !ok {
		return nil, false
	}
	return df.columns[i], true
}

func (df *DataFrame) Select(names ...string) (*DataFrame, error) {
	cols := make([]array.Column, 0, len(names))
	for _, name := range names {
		c, ok := df.Column(name)
		if !ok {
			return nil, fmt.Errorf("unknown column %s", name)
		}
		cols = append(cols, c)
	}
	return NewDataFrame(cols...)
}

func (df *DataFrame) Filter(e expr.Expr) (*DataFrame, error) {
	mask, err := e.Eval(df)
	if err != nil {
		return nil, err
	}
	if len(mask.Data) != df.nrows {
		return nil, fmt.Errorf("mask length mismatch")
	}
	vals := mask.Data
	cols := make([]array.Column, len(df.columns))
	for i := range df.columns {
		cols[i] = df.columns[i].Filter(vals)
	}
	return NewDataFrame(cols...)
}

func (df *DataFrame) Slice(offset, length int) (*DataFrame, error) {
	if offset < 0 {
		return nil, fmt.Errorf("slice offset must be >= 0")
	}
	if length < 0 {
		return nil, fmt.Errorf("slice length must be >= 0")
	}
	if offset >= df.nrows || length == 0 {
		order := []int{}
		cols := make([]array.Column, len(df.columns))
		for i := range cols {
			cols[i] = df.columns[i].Take(order)
		}
		return NewDataFrame(cols...)
	}
	end := offset + length
	if end > df.nrows {
		end = df.nrows
	}
	order := make([]int, end-offset)
	for i := range order {
		order[i] = offset + i
	}
	cols := make([]array.Column, len(df.columns))
	for i := range cols {
		cols[i] = df.columns[i].Take(order)
	}
	return NewDataFrame(cols...)
}

func (df *DataFrame) Head(n int) (*DataFrame, error) {
	if n < 0 {
		return nil, fmt.Errorf("head n must be >= 0")
	}
	if n > df.nrows {
		n = df.nrows
	}
	return df.Slice(0, n)
}

func (df *DataFrame) Tail(n int) (*DataFrame, error) {
	if n < 0 {
		return nil, fmt.Errorf("tail n must be >= 0")
	}
	if n > df.nrows {
		n = df.nrows
	}
	return df.Slice(df.nrows-n, n)
}

func (df *DataFrame) Limit(n int) (*DataFrame, error) {
	return df.Head(n)
}

func (df *DataFrame) Drop(names ...string) (*DataFrame, error) {
	if len(names) == 0 {
		return df, nil
	}
	drop := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return nil, fmt.Errorf("drop requires non-empty column name")
		}
		if _, ok := df.index[name]; !ok {
			return nil, fmt.Errorf("unknown column %s", name)
		}
		drop[name] = struct{}{}
	}
	kept := make([]array.Column, 0, len(df.columns))
	for i := range df.columns {
		if _, ok := drop[df.columns[i].Name()]; ok {
			continue
		}
		kept = append(kept, df.columns[i])
	}
	if len(kept) == 0 {
		return nil, fmt.Errorf("cannot drop all columns")
	}
	return NewDataFrame(kept...)
}

func (df *DataFrame) Rename(oldName, newName string) (*DataFrame, error) {
	if oldName == "" || newName == "" {
		return nil, fmt.Errorf("rename requires non-empty names")
	}
	idx, ok := df.index[oldName]
	if !ok {
		return nil, fmt.Errorf("unknown column %s", oldName)
	}
	if oldName == newName {
		return df, nil
	}
	if _, exists := df.index[newName]; exists {
		return nil, fmt.Errorf("duplicate column %s", newName)
	}
	renamed, err := array.WithName(df.columns[idx], newName)
	if err != nil {
		return nil, err
	}
	cols := make([]array.Column, len(df.columns))
	copy(cols, df.columns)
	cols[idx] = renamed
	return NewDataFrame(cols...)
}

func (df *DataFrame) WithColumns(cols ...array.Column) (*DataFrame, error) {
	if len(cols) == 0 {
		return df, nil
	}
	out := make([]array.Column, len(df.columns))
	copy(out, df.columns)
	idx := make(map[string]int, len(df.columns))
	for i := range out {
		idx[out[i].Name()] = i
	}
	seen := make(map[string]struct{}, len(cols))
	for i := range cols {
		c := cols[i]
		if c == nil {
			return nil, fmt.Errorf("nil column")
		}
		name := c.Name()
		if name == "" {
			return nil, fmt.Errorf("column name required")
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate input column %s", name)
		}
		seen[name] = struct{}{}
		if c.Len() != df.nrows {
			return nil, fmt.Errorf("column %s has mismatched length", name)
		}
		if j, ok := idx[name]; ok {
			out[j] = c
		} else {
			idx[name] = len(out)
			out = append(out, c)
		}
	}
	return NewDataFrame(out...)
}

func (df *DataFrame) SortBy(column string, desc bool) (*DataFrame, error) {
	c, ok := df.Column(column)
	if !ok {
		return nil, fmt.Errorf("unknown column %s", column)
	}
	order := make([]int, df.nrows)
	for i := range order {
		order[i] = i
	}
	var cmp func(a, b int) int
	switch col := c.(type) {
	case *array.Int64Column:
		cmp = func(a, b int) int {
			return compareNullAware(col.IsNull(a), col.IsNull(b), compareInt64(col.Value(a), col.Value(b), desc))
		}
	case *array.Float64Column:
		cmp = func(a, b int) int {
			return compareNullAware(col.IsNull(a), col.IsNull(b), compareFloat64(col.Value(a), col.Value(b), desc))
		}
	case *array.BoolColumn:
		cmp = func(a, b int) int {
			return compareNullAware(col.IsNull(a), col.IsNull(b), compareBool(col.Value(a), col.Value(b), desc))
		}
	case *array.Utf8Column:
		cmp = func(a, b int) int {
			ord := col.CompareRows(a, b)
			if desc {
				ord = -ord
			}
			return compareNullAware(col.IsNull(a), col.IsNull(b), ord)
		}
	default:
		return nil, fmt.Errorf("unsupported sort dtype %s", c.DType())
	}
	if df.nrows >= parallelSortThreshold && runtime.GOMAXPROCS(0) > 1 {
		parallelStableSort(order, cmp)
	} else {
		sort.SliceStable(order, func(i, j int) bool {
			o := cmp(order[i], order[j])
			if o == 0 {
				return order[i] < order[j]
			}
			return o < 0
		})
	}
	cols := make([]array.Column, len(df.columns))
	for i := range df.columns {
		cols[i] = df.columns[i].Take(order)
	}
	return NewDataFrame(cols...)
}

func (df *DataFrame) ProjectionChecksum(maxCols int) string {
	if maxCols <= 0 {
		maxCols = 1
	}
	if maxCols > len(df.columns) {
		maxCols = len(df.columns)
	}
	h := sha256.New()
	views := buildColumnViews(df.columns[:maxCols])
	var numScratch [64]byte
	for i := 0; i < df.nrows; i++ {
		for j := range views {
			if j > 0 {
				hashWriteByte(h, 0x1f)
			}
			v := views[j]
			if v.isNull(i) {
				continue
			}
			switch v.kind {
			case colKindInt64:
				b := strconv.AppendInt(numScratch[:0], v.i64.Value(i), 10)
				_, _ = h.Write(b)
			case colKindFloat64:
				b := strconv.AppendFloat(numScratch[:0], v.f64.Value(i), 'g', -1, 64)
				_, _ = h.Write(b)
			case colKindBool:
				if v.b.Value(i) {
					hashWriteString(h, "true")
				} else {
					hashWriteString(h, "false")
				}
			case colKindUtf8:
				s, e := v.s.ByteRange(i)
				_, _ = h.Write(v.s.Bytes()[s:e])
			}
		}
		hashWriteByte(h, '\n')
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (df *DataFrame) MarshalRowsJSON() ([]byte, error) {
	var buf bytes.Buffer
	quotedKeys := make([]string, len(df.columns))
	views := buildColumnViews(df.columns)
	for i := range df.columns {
		quotedKeys[i] = strconv.Quote(df.columns[i].Name())
	}
	buf.WriteByte('[')
	for i := 0; i < df.nrows; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('{')
		for j := range df.columns {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(quotedKeys[j])
			buf.WriteByte(':')
			v := views[j]
			if v.isNull(i) {
				buf.WriteString("null")
				continue
			}
			switch v.kind {
			case colKindInt64:
				buf.WriteString(strconv.FormatInt(v.i64.Value(i), 10))
			case colKindFloat64:
				buf.WriteString(strconv.FormatFloat(v.f64.Value(i), 'g', -1, 64))
			case colKindBool:
				if v.b.Value(i) {
					buf.WriteString("true")
				} else {
					buf.WriteString("false")
				}
			case colKindUtf8:
				s, e := v.s.ByteRange(i)
				writeJSONStringEscapedBytes(&buf, v.s.Bytes()[s:e])
			}
		}
		buf.WriteByte('}')
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

func hashWriteString(h hash.Hash, s string) {
	_, _ = io.WriteString(h, s)
}

func hashWriteByte(h hash.Hash, b byte) {
	var one [1]byte
	one[0] = b
	_, _ = h.Write(one[:])
}

type columnKind uint8

const (
	colKindInt64 columnKind = iota + 1
	colKindFloat64
	colKindBool
	colKindUtf8
)

type utf8View interface {
	ByteRange(i int) (int, int)
	Bytes() []byte
	IsNull(i int) bool
}

type columnView struct {
	kind columnKind
	i64  *array.Int64Column
	f64  *array.Float64Column
	b    *array.BoolColumn
	s    *array.Utf8Column
}

func (v columnView) isNull(i int) bool {
	switch v.kind {
	case colKindInt64:
		return v.i64.IsNull(i)
	case colKindFloat64:
		return v.f64.IsNull(i)
	case colKindBool:
		return v.b.IsNull(i)
	default:
		return v.s.IsNull(i)
	}
}

func buildColumnViews(cols []array.Column) []columnView {
	out := make([]columnView, len(cols))
	for i := range cols {
		switch c := cols[i].(type) {
		case *array.Int64Column:
			out[i] = columnView{kind: colKindInt64, i64: c}
		case *array.Float64Column:
			out[i] = columnView{kind: colKindFloat64, f64: c}
		case *array.BoolColumn:
			out[i] = columnView{kind: colKindBool, b: c}
		case *array.Utf8Column:
			out[i] = columnView{kind: colKindUtf8, s: c}
		default:
			panic("unsupported column type")
		}
	}
	return out
}

func writeJSONStringEscapedBytes(buf *bytes.Buffer, b []byte) {
	buf.WriteByte('"')
	for _, c := range b {
		switch c {
		case '\\':
			buf.WriteString("\\\\")
		case '"':
			buf.WriteString("\\\"")
		case '\n':
			buf.WriteString("\\n")
		case '\r':
			buf.WriteString("\\r")
		case '\t':
			buf.WriteString("\\t")
		default:
			if c < 0x20 {
				buf.WriteString("\\u00")
				hex := "0123456789abcdef"
				buf.WriteByte(hex[c>>4])
				buf.WriteByte(hex[c&0x0f])
			} else {
				buf.WriteByte(c)
			}
		}
	}
	buf.WriteByte('"')
}

func compareNullAware(aNull, bNull bool, ord int) int {
	if aNull && bNull {
		return 0
	}
	if aNull {
		return 1
	}
	if bNull {
		return -1
	}
	return ord
}

func compareInt64(a, b int64, desc bool) int {
	if a < b {
		if desc {
			return 1
		}
		return -1
	}
	if a > b {
		if desc {
			return -1
		}
		return 1
	}
	return 0
}

func compareFloat64(a, b float64, desc bool) int {
	if a < b {
		if desc {
			return 1
		}
		return -1
	}
	if a > b {
		if desc {
			return -1
		}
		return 1
	}
	return 0
}

func compareBool(a, b bool, desc bool) int {
	if a == b {
		return 0
	}
	if !a && b {
		if desc {
			return 1
		}
		return -1
	}
	if desc {
		return -1
	}
	return 1
}

type mergeRun struct {
	data []int
	pos  int
}

type mergeItem struct {
	run int
	row int
}

type mergeHeap struct {
	items []mergeItem
	runs  []mergeRun
	cmp   func(a, b int) int
}

func (h mergeHeap) Len() int { return len(h.items) }
func (h mergeHeap) Less(i, j int) bool {
	a := h.items[i].row
	b := h.items[j].row
	o := h.cmp(a, b)
	if o == 0 {
		return a < b
	}
	return o < 0
}
func (h mergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *mergeHeap) Push(x any)   { h.items = append(h.items, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	n := len(h.items)
	v := h.items[n-1]
	h.items = h.items[:n-1]
	return v
}

func parallelStableSort(order []int, cmp func(a, b int) int) {
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 || len(order) < workers*4096 {
		sort.SliceStable(order, func(i, j int) bool {
			o := cmp(order[i], order[j])
			if o == 0 {
				return order[i] < order[j]
			}
			return o < 0
		})
		return
	}
	runs := make([]mergeRun, 0, workers)
	chunk := (len(order) + workers - 1) / workers
	for start := 0; start < len(order); start += chunk {
		end := start + chunk
		if end > len(order) {
			end = len(order)
		}
		runs = append(runs, mergeRun{data: order[start:end]})
	}

	var wg sync.WaitGroup
	for i := range runs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sort.SliceStable(runs[idx].data, func(a, b int) bool {
				x := runs[idx].data[a]
				y := runs[idx].data[b]
				o := cmp(x, y)
				if o == 0 {
					return x < y
				}
				return o < 0
			})
		}(i)
	}
	wg.Wait()

	out := make([]int, 0, len(order))
	h := &mergeHeap{runs: runs, cmp: cmp, items: make([]mergeItem, 0, len(runs))}
	for i := range runs {
		if len(runs[i].data) == 0 {
			continue
		}
		h.items = append(h.items, mergeItem{run: i, row: runs[i].data[0]})
		runs[i].pos = 1
	}
	heap.Init(h)
	for h.Len() > 0 {
		item := heap.Pop(h).(mergeItem)
		out = append(out, item.row)
		r := &runs[item.run]
		if r.pos < len(r.data) {
			heap.Push(h, mergeItem{run: item.run, row: r.data[r.pos]})
			r.pos++
		}
	}
	copy(order, out)
}
