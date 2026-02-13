package grizzly

import "fmt"

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

type planOpType uint8

const (
	opSelect planOpType = iota + 1
	opFilter
	opSort
)

type planOp struct {
	typeID planOpType
	cols   []string
	filter Expr
	sortBy string
	desc   bool
}

type LazyFrame struct {
	source lazySource
	ops    []planOp
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
	next.ops = append(append([]planOp(nil), lf.ops...), planOp{typeID: opSelect, cols: cols})
	return &next
}

func (lf *LazyFrame) Filter(expr Expr) *LazyFrame {
	next := *lf
	next.ops = append(append([]planOp(nil), lf.ops...), planOp{typeID: opFilter, filter: expr})
	return &next
}

func (lf *LazyFrame) Sort(col string, desc bool) *LazyFrame {
	next := *lf
	next.ops = append(append([]planOp(nil), lf.ops...), planOp{typeID: opSort, sortBy: col, desc: desc})
	return &next
}

func (lf *LazyFrame) Collect() (*DataFrame, error) {
	optimized := lf.optimize()
	var df *DataFrame
	var err error
	if optimized.source.kind == sourceCSV {
		readPlan, remainingOps := optimized.csvReadPlan()
		df, err = readCSV(optimized.source.path, optimized.source.csv, readPlan)
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
		df, err = readJSON(optimized.source.path)
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
	filters := make([]planOp, 0, len(lf.ops))
	other := make([]planOp, 0, len(lf.ops))
	for _, op := range lf.ops {
		if op.typeID == opFilter {
			filters = append(filters, op)
		} else {
			other = append(other, op)
		}
	}
	next := *lf
	next.ops = append(filters, other...)
	return &next
}

func (lf *LazyFrame) csvReadPlan() (csvReadPlan, []planOp) {
	plan := csvReadPlan{}
	remaining := make([]planOp, 0, len(lf.ops))

	for i := range lf.ops {
		op := lf.ops[i]
		if op.typeID == opFilter {
			if ev, ok := op.filter.(evenExpr); ok && plan.filterEven == "" {
				plan.filterEven = ev.col
				continue
			}
		}
		remaining = append(remaining, op)
	}

	selected := false
	needed := map[string]struct{}{}
	for i := range remaining {
		op := remaining[i]
		switch op.typeID {
		case opSelect:
			selected = true
			for j := range op.cols {
				needed[op.cols[j]] = struct{}{}
			}
		case opSort:
			needed[op.sortBy] = struct{}{}
		case opFilter:
			for _, c := range exprColumns(op.filter) {
				needed[c] = struct{}{}
			}
		}
	}
	if selected {
		if plan.filterEven != "" {
			needed[plan.filterEven] = struct{}{}
		}
		plan.projection = needed
	}

	return plan, remaining
}
