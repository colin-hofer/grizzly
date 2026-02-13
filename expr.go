package grizzly

import (
	"fmt"
	"strconv"
)

type Expr interface {
	Eval(df *DataFrame) (*BoolColumn, error)
}

type ColRef struct {
	name string
}

func Col(name string) ColRef {
	return ColRef{name: name}
}

func (c ColRef) Eq(v any) Expr {
	return compareExpr{left: c.name, op: "eq", right: v}
}

func (c ColRef) Gt(v any) Expr {
	return compareExpr{left: c.name, op: "gt", right: v}
}

func (c ColRef) Even() Expr {
	return evenExpr{col: c.name}
}

type compareExpr struct {
	left  string
	op    string
	right any
}

func (e compareExpr) Eval(df *DataFrame) (*BoolColumn, error) {
	col, ok := df.Column(e.left)
	if !ok {
		return nil, fmt.Errorf("unknown column %s", e.left)
	}
	vals := make([]bool, col.Len())
	valid := make([]bool, col.Len())
	for i := range vals {
		if col.IsNull(i) {
			continue
		}
		valid[i] = true
		switch c := col.(type) {
		case *Int64Column:
			r, ok := literalToInt64(e.right)
			if !ok {
				return nil, fmt.Errorf("cannot compare int64 column with literal")
			}
			if e.op == "eq" {
				vals[i] = c.Value(i) == r
			} else {
				vals[i] = c.Value(i) > r
			}
		case *Float64Column:
			r, ok := literalToFloat64(e.right)
			if !ok {
				return nil, fmt.Errorf("cannot compare float64 column with literal")
			}
			if e.op == "eq" {
				vals[i] = c.Value(i) == r
			} else {
				vals[i] = c.Value(i) > r
			}
		case *Utf8Column:
			r := fmt.Sprint(e.right)
			if e.op == "eq" {
				vals[i] = c.Value(i) == r
			} else {
				vals[i] = c.Value(i) > r
			}
		case *BoolColumn:
			r, ok := e.right.(bool)
			if !ok {
				return nil, fmt.Errorf("bool comparison requires bool literal")
			}
			if e.op == "eq" {
				vals[i] = c.Value(i) == r
			} else {
				vals[i] = c.Value(i) && !r
			}
		default:
			return nil, fmt.Errorf("unsupported compare column type")
		}
	}
	return NewBoolColumn("_mask", vals, valid), nil
}

type evenExpr struct {
	col string
}

func (e evenExpr) Eval(df *DataFrame) (*BoolColumn, error) {
	col, ok := df.Column(e.col)
	if !ok {
		return nil, fmt.Errorf("unknown column %s", e.col)
	}
	vals := make([]bool, col.Len())
	valid := make([]bool, col.Len())
	for i := range vals {
		if col.IsNull(i) {
			continue
		}
		valid[i] = true
		switch c := col.(type) {
		case *Int64Column:
			vals[i] = c.Value(i)%2 == 0
		case *Float64Column:
			vals[i] = int64(c.Value(i))%2 == 0
		case *Utf8Column:
			vals[i] = len(c.Value(i))%2 == 0
		case *BoolColumn:
			vals[i] = !c.Value(i)
		default:
			vals[i] = false
		}
	}
	return NewBoolColumn("_mask", vals, valid), nil
}

type logicalExpr struct {
	left  Expr
	right Expr
	op    string
}

func And(a, b Expr) Expr { return logicalExpr{left: a, right: b, op: "and"} }
func Or(a, b Expr) Expr  { return logicalExpr{left: a, right: b, op: "or"} }

func (e logicalExpr) Eval(df *DataFrame) (*BoolColumn, error) {
	la, err := e.left.Eval(df)
	if err != nil {
		return nil, err
	}
	lb, err := e.right.Eval(df)
	if err != nil {
		return nil, err
	}
	if la.Len() != lb.Len() {
		return nil, fmt.Errorf("logical mask length mismatch")
	}
	out := make([]bool, la.Len())
	valid := make([]bool, la.Len())
	for i := range out {
		if la.IsNull(i) || lb.IsNull(i) {
			continue
		}
		valid[i] = true
		if e.op == "and" {
			out[i] = la.Value(i) && lb.Value(i)
		} else {
			out[i] = la.Value(i) || lb.Value(i)
		}
	}
	return NewBoolColumn("_mask", out, valid), nil
}

func literalToInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case float64:
		return int64(x), true
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func literalToFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case string:
		n, err := strconv.ParseFloat(x, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func exprColumns(e Expr) []string {
	switch x := e.(type) {
	case compareExpr:
		return []string{x.left}
	case evenExpr:
		return []string{x.col}
	case logicalExpr:
		l := exprColumns(x.left)
		r := exprColumns(x.right)
		out := make([]string, 0, len(l)+len(r))
		seen := map[string]struct{}{}
		for i := range l {
			if _, ok := seen[l[i]]; ok {
				continue
			}
			seen[l[i]] = struct{}{}
			out = append(out, l[i])
		}
		for i := range r {
			if _, ok := seen[r[i]]; ok {
				continue
			}
			seen[r[i]] = struct{}{}
			out = append(out, r[i])
		}
		return out
	default:
		return nil
	}
}
