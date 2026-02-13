package plan

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"grizzly/internal/exec"
	"grizzly/internal/expr"
	csvio "grizzly/internal/io/csv"
	jsonio "grizzly/internal/io/json"
)

type ScanOptions struct {
	Delimiter  rune
	NullValues []string
}

type sourceKind uint8

const (
	sourceCSV sourceKind = iota + 1
	sourceJSON
)

type lazySource struct {
	kind sourceKind
	path string
	csv  ScanOptions
}

type opType uint8

const (
	opSelect opType = iota + 1
	opFilter
	opSort
)

type op struct {
	typeID opType
	cols   []string
	filter expr.Expr
	sortBy string
	desc   bool
}

type LazyFrame struct {
	source lazySource
	ops    []op
}

// Explain returns a human-readable plan.
// It includes any scan pushdowns that will apply during Collect.
func (lf *LazyFrame) Explain() (string, error) {
	optimized := lf.optimize()

	var b strings.Builder
	switch optimized.source.kind {
	case sourceCSV:
		b.WriteString("Source: csv\n")
		b.WriteString("Path: ")
		b.WriteString(optimized.source.path)
		b.WriteByte('\n')
		if optimized.source.csv.Delimiter != 0 {
			b.WriteString("Delimiter: ")
			b.WriteRune(optimized.source.csv.Delimiter)
			b.WriteByte('\n')
		}
		if len(optimized.source.csv.NullValues) > 0 {
			b.WriteString("NullValues: ")
			b.WriteString(strings.Join(optimized.source.csv.NullValues, ","))
			b.WriteByte('\n')
		}

		readPlan, remainingOps, err := optimized.csvReadPlan()
		if err != nil {
			b.WriteString("PlanningError: ")
			b.WriteString(err.Error())
			b.WriteByte('\n')
			return b.String(), err
		}

		b.WriteString("Scan: ")
		if len(readPlan.Projection) > 0 {
			b.WriteString("projection=[")
			keys := make([]string, 0, len(readPlan.Projection))
			for k := range readPlan.Projection {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			b.WriteString(strings.Join(keys, ","))
			b.WriteByte(']')
		} else {
			b.WriteString("projection=* (all)")
		}
		if readPlan.FilterEven != "" {
			b.WriteString(" filter_even=")
			b.WriteString(readPlan.FilterEven)
		}
		b.WriteByte('\n')

		b.WriteString("Ops:\n")
		for _, op := range remainingOps {
			b.WriteString("- ")
			b.WriteString(formatOp(op))
			b.WriteByte('\n')
		}
		return b.String(), nil

	case sourceJSON:
		b.WriteString("Source: json\n")
		b.WriteString("Path: ")
		b.WriteString(optimized.source.path)
		b.WriteByte('\n')
		b.WriteString("Ops:\n")
		for _, op := range optimized.ops {
			b.WriteString("- ")
			b.WriteString(formatOp(op))
			b.WriteByte('\n')
		}
		return b.String(), nil

	default:
		return "", fmt.Errorf("unknown source kind")
	}
}

func formatOp(op op) string {
	switch op.typeID {
	case opSelect:
		return "select(" + strings.Join(op.cols, ",") + ")"
	case opSort:
		dir := "asc"
		if op.desc {
			dir = "desc"
		}
		return "sort(" + op.sortBy + "," + dir + ")"
	case opFilter:
		cols := expr.ExprColumns(op.filter)
		sort.Strings(cols)
		return "filter(cols=[" + strings.Join(cols, ",") + "])"
	default:
		return "op(?)"
	}
}

func ScanCSV(path string, opts ScanOptions) *LazyFrame {
	if len(opts.NullValues) == 0 {
		opts.NullValues = []string{"", "NULL", "null"}
	}
	return &LazyFrame{source: lazySource{kind: sourceCSV, path: path, csv: opts}}
}

func ScanJSON(path string) *LazyFrame {
	return &LazyFrame{source: lazySource{kind: sourceJSON, path: path}}
}

func (lf *LazyFrame) Select(cols ...string) *LazyFrame {
	next := *lf
	next.ops = append(append([]op(nil), lf.ops...), op{typeID: opSelect, cols: cols})
	return &next
}

func (lf *LazyFrame) Filter(e expr.Expr) *LazyFrame {
	next := *lf
	next.ops = append(append([]op(nil), lf.ops...), op{typeID: opFilter, filter: e})
	return &next
}

func (lf *LazyFrame) Sort(col string, desc bool) *LazyFrame {
	next := *lf
	next.ops = append(append([]op(nil), lf.ops...), op{typeID: opSort, sortBy: col, desc: desc})
	return &next
}

func (lf *LazyFrame) Collect() (*exec.DataFrame, error) {
	return lf.CollectContext(context.Background())
}

func (lf *LazyFrame) CollectContext(ctx context.Context) (*exec.DataFrame, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	optimized := lf.optimize()
	var df *exec.DataFrame
	var err error
	if optimized.source.kind == sourceCSV {
		readPlan, remainingOps, err := optimized.csvReadPlan()
		if err != nil {
			return nil, err
		}
		df, err = csvio.Read(ctx, optimized.source.path, optimized.source.csv.Delimiter, optimized.source.csv.NullValues, readPlan)
		if err != nil {
			return nil, err
		}
		for _, op := range remainingOps {
			switch op.typeID {
			case opFilter:
				df, err = df.Filter(op.filter)
			case opSelect:
				df, err = df.Select(op.cols...)
			case opSort:
				df, err = df.SortBy(op.sortBy, op.desc)
			}
			if err != nil {
				return nil, err
			}
		}
		return df, nil
	}

	switch optimized.source.kind {
	case sourceJSON:
		df, err = jsonio.Read(ctx, optimized.source.path)
	default:
		return nil, fmt.Errorf("unknown source kind")
	}
	if err != nil {
		return nil, err
	}
	for _, op := range optimized.ops {
		switch op.typeID {
		case opFilter:
			df, err = df.Filter(op.filter)
		case opSelect:
			df, err = df.Select(op.cols...)
		case opSort:
			df, err = df.SortBy(op.sortBy, op.desc)
		}
		if err != nil {
			return nil, err
		}
	}
	return df, nil
}

func (lf *LazyFrame) optimize() *LazyFrame {
	// Optimizations must preserve sequential semantics.
	// For now, we do not reorder operations (reordering across Select can change
	// error behavior when a filter references a dropped column).
	next := *lf
	next.ops = append([]op(nil), lf.ops...)
	return &next
}

func (lf *LazyFrame) csvReadPlan() (csvio.ReadPlan, []op, error) {
	plan := csvio.ReadPlan{}
	remaining := make([]op, 0, len(lf.ops))

	var visible map[string]struct{}

	firstSelectIdx := -1
	for i := range lf.ops {
		if lf.ops[i].typeID == opSelect {
			firstSelectIdx = i
			break
		}
	}

	scanNeeded := map[string]struct{}{}
	if firstSelectIdx >= 0 {
		for _, c := range lf.ops[firstSelectIdx].cols {
			scanNeeded[c] = struct{}{}
		}
	}

	for i := range lf.ops {
		op := lf.ops[i]
		switch op.typeID {
		case opSelect:
			if visible != nil {
				for _, c := range op.cols {
					if _, ok := visible[c]; !ok {
						return csvio.ReadPlan{}, nil, fmt.Errorf("select unknown column %s", c)
					}
				}
			}
			visible = make(map[string]struct{}, len(op.cols))
			for _, c := range op.cols {
				visible[c] = struct{}{}
			}
			remaining = append(remaining, op)

		case opSort:
			if visible != nil {
				if _, ok := visible[op.sortBy]; !ok {
					return csvio.ReadPlan{}, nil, fmt.Errorf("sort unknown column %s", op.sortBy)
				}
			}
			if firstSelectIdx >= 0 && i < firstSelectIdx {
				scanNeeded[op.sortBy] = struct{}{}
			}
			remaining = append(remaining, op)

		case opFilter:
			cols := expr.ExprColumns(op.filter)
			if visible != nil {
				for _, c := range cols {
					if _, ok := visible[c]; !ok {
						return csvio.ReadPlan{}, nil, fmt.Errorf("filter unknown column %s", c)
					}
				}
			}
			if col, ok := expr.IsEven(op.filter); ok && plan.FilterEven == "" {
				plan.FilterEven = col
				if firstSelectIdx >= 0 && i < firstSelectIdx {
					scanNeeded[col] = struct{}{}
				}
				continue
			}
			// fallback: only collect scan-needed columns for pre-select filters
			if firstSelectIdx >= 0 && i < firstSelectIdx {
				for _, c := range cols {
					scanNeeded[c] = struct{}{}
				}
			}
			remaining = append(remaining, op)
		}
	}

	if firstSelectIdx >= 0 {
		if plan.FilterEven != "" {
			scanNeeded[plan.FilterEven] = struct{}{}
		}
		plan.Projection = scanNeeded
	}
	return plan, remaining, nil
}
