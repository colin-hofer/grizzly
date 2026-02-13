package grizzly

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"runtime"
	"sort"
	"strconv"
	"sync"
)

const parallelSortThreshold = 250000

type DataFrame struct {
	columns []Column
	index   map[string]int
	nrows   int
}

func NewDataFrame(cols ...Column) (*DataFrame, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("at least one column required")
	}
	n := cols[0].Len()
	idx := make(map[string]int, len(cols))
	for i := range cols {
		if cols[i].Len() != n {
			return nil, fmt.Errorf("column %s has mismatched length", cols[i].Name())
		}
		if _, ok := idx[cols[i].Name()]; ok {
			return nil, fmt.Errorf("duplicate column %s", cols[i].Name())
		}
		idx[cols[i].Name()] = i
	}
	return &DataFrame{columns: cols, index: idx, nrows: n}, nil
}

func (df *DataFrame) Height() int { return df.nrows }
func (df *DataFrame) Width() int  { return len(df.columns) }

func (df *DataFrame) Columns() []Column {
	out := make([]Column, len(df.columns))
	copy(out, df.columns)
	return out
}

func (df *DataFrame) Column(name string) (Column, bool) {
	i, ok := df.index[name]
	if !ok {
		return nil, false
	}
	return df.columns[i], true
}

func (df *DataFrame) Select(names ...string) (*DataFrame, error) {
	cols := make([]Column, 0, len(names))
	for _, name := range names {
		c, ok := df.Column(name)
		if !ok {
			return nil, fmt.Errorf("unknown column %s", name)
		}
		cols = append(cols, c)
	}
	return NewDataFrame(cols...)
}

func (df *DataFrame) Filter(expr Expr) (*DataFrame, error) {
	mask, err := expr.Eval(df)
	if err != nil {
		return nil, err
	}
	if mask.Len() != df.nrows {
		return nil, fmt.Errorf("mask length mismatch")
	}
	vals := mask.data
	cols := make([]Column, len(df.columns))
	for i := range df.columns {
		cols[i] = df.columns[i].Filter(vals)
	}
	return NewDataFrame(cols...)
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
	case *Int64Column:
		cmp = func(a, b int) int {
			return compareNullAware(col.IsNull(a), col.IsNull(b), compareInt64(col.data[a], col.data[b], desc))
		}
	case *Float64Column:
		cmp = func(a, b int) int {
			return compareNullAware(col.IsNull(a), col.IsNull(b), compareFloat64(col.data[a], col.data[b], desc))
		}
	case *BoolColumn:
		cmp = func(a, b int) int {
			return compareNullAware(col.IsNull(a), col.IsNull(b), compareBool(col.data[a], col.data[b], desc))
		}
	case *Utf8Column:
		cmp = func(a, b int) int {
			ord := 0
			if col.Less(a, b) {
				ord = -1
			} else if col.Less(b, a) {
				ord = 1
			}
			if desc {
				ord = -ord
			}
			return compareNullAware(col.IsNull(a), col.IsNull(b), ord)
		}
	default:
		cmp = func(a, b int) int {
			ord := 0
			if c.Less(a, b) {
				ord = -1
			} else if c.Less(b, a) {
				ord = 1
			}
			if desc {
				ord = -ord
			}
			return compareNullAware(c.IsNull(a), c.IsNull(b), ord)
		}
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
	cols := make([]Column, len(df.columns))
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
	for i := 0; i < df.nrows; i++ {
		for j := 0; j < maxCols; j++ {
			if j > 0 {
				hashWriteByte(h, 0x1f)
			}
			hashWriteString(h, df.columns[j].ValueString(i))
		}
		hashWriteByte(h, '\n')
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (df *DataFrame) MarshalRowsJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i := 0; i < df.nrows; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('{')
		for j, c := range df.columns {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(strconv.Quote(c.Name()))
			buf.WriteByte(':')
			if c.IsNull(i) {
				buf.WriteString("null")
				continue
			}
			switch cc := c.(type) {
			case *Int64Column:
				buf.WriteString(strconv.FormatInt(cc.Value(i), 10))
			case *Float64Column:
				buf.WriteString(strconv.FormatFloat(cc.Value(i), 'g', -1, 64))
			case *BoolColumn:
				if cc.Value(i) {
					buf.WriteString("true")
				} else {
					buf.WriteString("false")
				}
			case *Utf8Column:
				buf.WriteString(strconv.Quote(cc.Value(i)))
			default:
				buf.WriteString(strconv.Quote(c.ValueString(i)))
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
