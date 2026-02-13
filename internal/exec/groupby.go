package exec

import (
	"fmt"
	"math"

	"grizzly/internal/array"
)

const (
	maxInt64 = int64(^uint64(0) >> 1)
	minInt64 = -maxInt64 - 1
)

// GroupBy is a grouped dataframe.
type GroupBy struct {
	df   *DataFrame
	keys []string
}

func (df *DataFrame) GroupBy(keys ...string) (*GroupBy, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("groupby requires at least one key")
	}
	for _, k := range keys {
		if k == "" {
			return nil, fmt.Errorf("groupby key must be non-empty")
		}
		if _, ok := df.index[k]; !ok {
			return nil, fmt.Errorf("unknown column %s", k)
		}
	}
	return &GroupBy{df: df, keys: append([]string(nil), keys...)}, nil
}

type AggFunc uint8

const (
	AggInvalid AggFunc = iota
	AggCount
	AggSum
	AggMean
	AggMin
	AggMax
)

type AggSpec struct {
	Col   string
	Func  AggFunc
	Alias string
}

// Count returns one row per group with a "count" column.
//
// Current limitations:
// - Only a single int64 key column is supported.
// - NULL keys form their own group.
func (g *GroupBy) Count() (*DataFrame, error) {
	return g.Agg(AggSpec{Func: AggCount, Alias: "count"})
}

// Agg computes one or more aggregations per group.
//
// Current limitations:
// - Only a single int64 key column is supported.
// - Aggregations support int64 and float64 value columns.
// - NULL keys form their own group.
// - Aggregations ignore NULL values; if all values are NULL for a group, the result is NULL.
func (g *GroupBy) Agg(specs ...AggSpec) (*DataFrame, error) {
	if g == nil || g.df == nil {
		return nil, fmt.Errorf("nil groupby")
	}
	if len(g.keys) != 1 {
		return nil, fmt.Errorf("groupby: only single key supported")
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("agg requires at least one spec")
	}

	keyName := g.keys[0]
	keyCol, ok := g.df.Column(keyName)
	if !ok {
		return nil, fmt.Errorf("unknown column %s", keyName)
	}
	ic, ok := keyCol.(*array.Int64Column)
	if !ok {
		return nil, fmt.Errorf("groupby: only int64 key supported")
	}

	resolved, err := resolveAggs(g.df, specs)
	if err != nil {
		return nil, err
	}

	// Preserve first-seen group order.
	idx := make(map[int64]int, 256)
	keys := make([]int64, 0, 256)
	keyValid := make([]bool, 0, 256)
	nullIdx := -1

	newGroup := func(key int64, valid bool) int {
		gi := len(keys)
		keys = append(keys, key)
		keyValid = append(keyValid, valid)
		for i := range resolved {
			resolved[i].newGroup()
		}
		return gi
	}

	for row := 0; row < g.df.nrows; row++ {
		var gi int
		if ic.IsNull(row) {
			if nullIdx < 0 {
				nullIdx = newGroup(0, false)
			}
			gi = nullIdx
		} else {
			k := ic.Value(row)
			var ok bool
			gi, ok = idx[k]
			if !ok {
				gi = newGroup(k, true)
				idx[k] = gi
			}
		}
		for i := range resolved {
			resolved[i].observe(gi, row)
		}
	}

	keyOut := array.NewInt64ColumnOwned(keyName, keys, array.NewBitmapFromBools(keyValid))
	outCols := make([]array.Column, 0, 1+len(resolved))
	outCols = append(outCols, keyOut)
	for i := range resolved {
		outCols = append(outCols, resolved[i].build())
	}
	return NewDataFrame(outCols...)
}

type aggResolved interface {
	newGroup()
	observe(groupIdx int, row int)
	build() array.Column
}

func resolveAggs(df *DataFrame, specs []AggSpec) ([]aggResolved, error) {
	seenAlias := map[string]struct{}{}
	out := make([]aggResolved, 0, len(specs))
	for _, s := range specs {
		alias := s.Alias
		if alias == "" {
			switch s.Func {
			case AggCount:
				alias = "count"
			case AggSum:
				alias = s.Col + "_sum"
			case AggMean:
				alias = s.Col + "_mean"
			case AggMin:
				alias = s.Col + "_min"
			case AggMax:
				alias = s.Col + "_max"
			default:
				return nil, fmt.Errorf("unknown agg func")
			}
		}
		if _, ok := seenAlias[alias]; ok {
			return nil, fmt.Errorf("duplicate agg output %s", alias)
		}
		seenAlias[alias] = struct{}{}

		switch s.Func {
		case AggCount:
			out = append(out, &aggCount{alias: alias})
			continue
		case AggSum, AggMean, AggMin, AggMax:
			// continue below
		default:
			return nil, fmt.Errorf("invalid agg func")
		}
		if s.Col == "" {
			return nil, fmt.Errorf("agg requires column")
		}
		col, ok := df.Column(s.Col)
		if !ok {
			return nil, fmt.Errorf("unknown column %s", s.Col)
		}
		switch c := col.(type) {
		case *array.Int64Column:
			switch s.Func {
			case AggSum:
				out = append(out, &aggInt64Sum{alias: alias, col: c})
			case AggMean:
				out = append(out, &aggInt64Mean{alias: alias, col: c})
			case AggMin:
				out = append(out, &aggInt64Min{alias: alias, col: c})
			case AggMax:
				out = append(out, &aggInt64Max{alias: alias, col: c})
			}
		case *array.Float64Column:
			switch s.Func {
			case AggSum:
				out = append(out, &aggFloat64Sum{alias: alias, col: c})
			case AggMean:
				out = append(out, &aggFloat64Mean{alias: alias, col: c})
			case AggMin:
				out = append(out, &aggFloat64Min{alias: alias, col: c})
			case AggMax:
				out = append(out, &aggFloat64Max{alias: alias, col: c})
			}
		default:
			return nil, fmt.Errorf("unsupported agg dtype")
		}
	}
	return out, nil
}

type aggCount struct {
	alias  string
	counts []int64
}

func (a *aggCount) newGroup() { a.counts = append(a.counts, 0) }
func (a *aggCount) observe(groupIdx int, _ int) {
	a.counts[groupIdx]++
}
func (a *aggCount) build() array.Column {
	return array.NewInt64ColumnOwned(a.alias, a.counts, array.NewBitmap(len(a.counts), true))
}

type aggInt64Sum struct {
	alias string
	col   *array.Int64Column
	sums  []int64
	valid []bool
}

func (a *aggInt64Sum) newGroup() {
	a.sums = append(a.sums, 0)
	a.valid = append(a.valid, false)
}
func (a *aggInt64Sum) observe(groupIdx int, row int) {
	if a.col.IsNull(row) {
		return
	}
	a.sums[groupIdx] += a.col.Value(row)
	a.valid[groupIdx] = true
}
func (a *aggInt64Sum) build() array.Column {
	return array.NewInt64ColumnOwned(a.alias, a.sums, array.NewBitmapFromBools(a.valid))
}

type aggFloat64Sum struct {
	alias string
	col   *array.Float64Column
	sums  []float64
	valid []bool
}

func (a *aggFloat64Sum) newGroup() {
	a.sums = append(a.sums, 0)
	a.valid = append(a.valid, false)
}
func (a *aggFloat64Sum) observe(groupIdx int, row int) {
	if a.col.IsNull(row) {
		return
	}
	a.sums[groupIdx] += a.col.Value(row)
	a.valid[groupIdx] = true
}
func (a *aggFloat64Sum) build() array.Column {
	return array.NewFloat64ColumnOwned(a.alias, a.sums, array.NewBitmapFromBools(a.valid))
}

type aggInt64Mean struct {
	alias string
	col   *array.Int64Column
	sum   []float64
	count []int64
	valid []bool
}

func (a *aggInt64Mean) newGroup() {
	a.sum = append(a.sum, 0)
	a.count = append(a.count, 0)
	a.valid = append(a.valid, false)
}
func (a *aggInt64Mean) observe(groupIdx int, row int) {
	if a.col.IsNull(row) {
		return
	}
	a.sum[groupIdx] += float64(a.col.Value(row))
	a.count[groupIdx]++
	a.valid[groupIdx] = true
}
func (a *aggInt64Mean) build() array.Column {
	out := make([]float64, len(a.sum))
	for i := range out {
		if a.count[i] == 0 {
			out[i] = 0
			continue
		}
		out[i] = a.sum[i] / float64(a.count[i])
	}
	return array.NewFloat64ColumnOwned(a.alias, out, array.NewBitmapFromBools(a.valid))
}

type aggFloat64Mean struct {
	alias string
	col   *array.Float64Column
	sum   []float64
	count []int64
	valid []bool
}

func (a *aggFloat64Mean) newGroup() {
	a.sum = append(a.sum, 0)
	a.count = append(a.count, 0)
	a.valid = append(a.valid, false)
}
func (a *aggFloat64Mean) observe(groupIdx int, row int) {
	if a.col.IsNull(row) {
		return
	}
	a.sum[groupIdx] += a.col.Value(row)
	a.count[groupIdx]++
	a.valid[groupIdx] = true
}
func (a *aggFloat64Mean) build() array.Column {
	out := make([]float64, len(a.sum))
	for i := range out {
		if a.count[i] == 0 {
			out[i] = 0
			continue
		}
		out[i] = a.sum[i] / float64(a.count[i])
	}
	return array.NewFloat64ColumnOwned(a.alias, out, array.NewBitmapFromBools(a.valid))
}

type aggInt64Min struct {
	alias string
	col   *array.Int64Column
	mins  []int64
	valid []bool
}

func (a *aggInt64Min) newGroup() {
	a.mins = append(a.mins, maxInt64)
	a.valid = append(a.valid, false)
}
func (a *aggInt64Min) observe(groupIdx int, row int) {
	if a.col.IsNull(row) {
		return
	}
	v := a.col.Value(row)
	if !a.valid[groupIdx] || v < a.mins[groupIdx] {
		a.mins[groupIdx] = v
		a.valid[groupIdx] = true
	}
}
func (a *aggInt64Min) build() array.Column {
	// Replace sentinel values for invalid groups with 0 to keep output stable.
	for i := range a.mins {
		if !a.valid[i] {
			a.mins[i] = 0
		}
	}
	return array.NewInt64ColumnOwned(a.alias, a.mins, array.NewBitmapFromBools(a.valid))
}

type aggInt64Max struct {
	alias string
	col   *array.Int64Column
	maxs  []int64
	valid []bool
}

func (a *aggInt64Max) newGroup() {
	a.maxs = append(a.maxs, minInt64)
	a.valid = append(a.valid, false)
}
func (a *aggInt64Max) observe(groupIdx int, row int) {
	if a.col.IsNull(row) {
		return
	}
	v := a.col.Value(row)
	if !a.valid[groupIdx] || v > a.maxs[groupIdx] {
		a.maxs[groupIdx] = v
		a.valid[groupIdx] = true
	}
}
func (a *aggInt64Max) build() array.Column {
	for i := range a.maxs {
		if !a.valid[i] {
			a.maxs[i] = 0
		}
	}
	return array.NewInt64ColumnOwned(a.alias, a.maxs, array.NewBitmapFromBools(a.valid))
}

type aggFloat64Min struct {
	alias string
	col   *array.Float64Column
	mins  []float64
	valid []bool
}

func (a *aggFloat64Min) newGroup() {
	a.mins = append(a.mins, math.Inf(1))
	a.valid = append(a.valid, false)
}
func (a *aggFloat64Min) observe(groupIdx int, row int) {
	if a.col.IsNull(row) {
		return
	}
	v := a.col.Value(row)
	if !a.valid[groupIdx] || v < a.mins[groupIdx] {
		a.mins[groupIdx] = v
		a.valid[groupIdx] = true
	}
}
func (a *aggFloat64Min) build() array.Column {
	for i := range a.mins {
		if !a.valid[i] {
			a.mins[i] = 0
		}
	}
	return array.NewFloat64ColumnOwned(a.alias, a.mins, array.NewBitmapFromBools(a.valid))
}

type aggFloat64Max struct {
	alias string
	col   *array.Float64Column
	maxs  []float64
	valid []bool
}

func (a *aggFloat64Max) newGroup() {
	a.maxs = append(a.maxs, math.Inf(-1))
	a.valid = append(a.valid, false)
}
func (a *aggFloat64Max) observe(groupIdx int, row int) {
	if a.col.IsNull(row) {
		return
	}
	v := a.col.Value(row)
	if !a.valid[groupIdx] || v > a.maxs[groupIdx] {
		a.maxs[groupIdx] = v
		a.valid[groupIdx] = true
	}
}
func (a *aggFloat64Max) build() array.Column {
	for i := range a.maxs {
		if !a.valid[i] {
			a.maxs[i] = 0
		}
	}
	return array.NewFloat64ColumnOwned(a.alias, a.maxs, array.NewBitmapFromBools(a.valid))
}
