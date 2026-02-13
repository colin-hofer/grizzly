package expr

import (
	"fmt"
	"strconv"

	"grizzly/internal/array"
)

// Frame is the minimal interface required by expression evaluation.
// It avoids depending on concrete dataframe implementations to keep packages acyclic.
type Frame interface {
	Height() int
	Column(name string) (array.Column, bool)
}

// Mask is the boolean result of evaluating an expression.
// Data is used by filter kernels directly.
// Valid encodes NULLs; invalid entries should be treated as false by filters.
type Mask struct {
	Data  []bool
	Valid []bool
}

type Expr interface {
	Eval(f Frame) (Mask, error)
}

type ColRef struct {
	Name string
}

func Col(name string) ColRef { return ColRef{Name: name} }

func (c ColRef) Eq(v any) Expr   { return compareExpr{left: c.Name, op: cmpEq, right: v} }
func (c ColRef) Neq(v any) Expr  { return compareExpr{left: c.Name, op: cmpNeq, right: v} }
func (c ColRef) Lt(v any) Expr   { return compareExpr{left: c.Name, op: cmpLt, right: v} }
func (c ColRef) Lte(v any) Expr  { return compareExpr{left: c.Name, op: cmpLte, right: v} }
func (c ColRef) Gt(v any) Expr   { return compareExpr{left: c.Name, op: cmpGt, right: v} }
func (c ColRef) Gte(v any) Expr  { return compareExpr{left: c.Name, op: cmpGte, right: v} }
func (c ColRef) Even() Expr      { return evenExpr{col: c.Name} }
func (c ColRef) IsNull() Expr    { return isNullExpr{col: c.Name, negate: false} }
func (c ColRef) IsNotNull() Expr { return isNullExpr{col: c.Name, negate: true} }
func (c ColRef) In(vals ...any) Expr {
	return inExpr{col: c.Name, vals: append([]any(nil), vals...)}
}

func Not(e Expr) Expr { return notExpr{child: e} }

type compareExpr struct {
	left  string
	op    cmpOp
	right any
}

type inExpr struct {
	col  string
	vals []any
}

func (e inExpr) Eval(f Frame) (Mask, error) {
	col, ok := f.Column(e.col)
	if !ok {
		return Mask{}, fmt.Errorf("unknown column %s", e.col)
	}
	vals := make([]bool, col.Len())
	valid := make([]bool, col.Len())
	if len(e.vals) == 0 {
		// Empty set: always false.
		for i := range vals {
			if col.IsNull(i) {
				continue
			}
			valid[i] = true
			vals[i] = false
		}
		return Mask{Data: vals, Valid: valid}, nil
	}

	switch c := col.(type) {
	case *array.Int64Column:
		set := make(map[int64]struct{}, len(e.vals))
		for i := range e.vals {
			v, ok := literalToInt64(e.vals[i])
			if !ok {
				return Mask{}, fmt.Errorf("cannot build int64 set")
			}
			set[v] = struct{}{}
		}
		for i := range vals {
			if c.IsNull(i) {
				continue
			}
			valid[i] = true
			_, ok := set[c.Value(i)]
			vals[i] = ok
		}
	case *array.BoolColumn:
		set := make(map[bool]struct{}, len(e.vals))
		for i := range e.vals {
			v, ok := e.vals[i].(bool)
			if !ok {
				return Mask{}, fmt.Errorf("cannot build bool set")
			}
			set[v] = struct{}{}
		}
		for i := range vals {
			if c.IsNull(i) {
				continue
			}
			valid[i] = true
			_, ok := set[c.Value(i)]
			vals[i] = ok
		}
	case *array.Utf8Column:
		set := make(map[string]struct{}, len(e.vals))
		for i := range e.vals {
			set[fmt.Sprint(e.vals[i])] = struct{}{}
		}
		for i := range vals {
			if c.IsNull(i) {
				continue
			}
			valid[i] = true
			_, ok := set[c.Value(i)]
			vals[i] = ok
		}
	default:
		return Mask{}, fmt.Errorf("unsupported in column type")
	}
	return Mask{Data: vals, Valid: valid}, nil
}

type cmpOp uint8

const (
	cmpInvalid cmpOp = iota
	cmpEq
	cmpNeq
	cmpLt
	cmpLte
	cmpGt
	cmpGte
)

func (e compareExpr) Eval(f Frame) (Mask, error) {
	col, ok := f.Column(e.left)
	if !ok {
		return Mask{}, fmt.Errorf("unknown column %s", e.left)
	}
	vals := make([]bool, col.Len())
	valid := make([]bool, col.Len())
	switch c := col.(type) {
	case *array.Int64Column:
		r, ok := literalToInt64(e.right)
		if !ok {
			return Mask{}, fmt.Errorf("cannot compare int64 column with literal")
		}
		for i := range vals {
			if c.IsNull(i) {
				continue
			}
			valid[i] = true
			vals[i] = cmpInt64(e.op, c.Value(i), r)
		}
	case *array.Float64Column:
		r, ok := literalToFloat64(e.right)
		if !ok {
			return Mask{}, fmt.Errorf("cannot compare float64 column with literal")
		}
		for i := range vals {
			if c.IsNull(i) {
				continue
			}
			valid[i] = true
			vals[i] = cmpFloat64(e.op, c.Value(i), r)
		}
	case *array.Utf8Column:
		lit := []byte(fmt.Sprint(e.right))
		for i := range vals {
			if c.IsNull(i) {
				continue
			}
			valid[i] = true
			cmpv := c.CompareLiteral(i, lit)
			vals[i] = cmpInt(e.op, cmpv)
		}
	case *array.BoolColumn:
		r, ok := e.right.(bool)
		if !ok {
			return Mask{}, fmt.Errorf("bool comparison requires bool literal")
		}
		for i := range vals {
			if c.IsNull(i) {
				continue
			}
			valid[i] = true
			vals[i] = cmpBool(e.op, c.Value(i), r)
		}
	default:
		return Mask{}, fmt.Errorf("unsupported compare column type")
	}
	return Mask{Data: vals, Valid: valid}, nil
}

func cmpInt(op cmpOp, ord int) bool {
	switch op {
	case cmpEq:
		return ord == 0
	case cmpNeq:
		return ord != 0
	case cmpLt:
		return ord < 0
	case cmpLte:
		return ord <= 0
	case cmpGt:
		return ord > 0
	case cmpGte:
		return ord >= 0
	default:
		return false
	}
}

func cmpInt64(op cmpOp, a, b int64) bool {
	switch op {
	case cmpEq:
		return a == b
	case cmpNeq:
		return a != b
	case cmpLt:
		return a < b
	case cmpLte:
		return a <= b
	case cmpGt:
		return a > b
	case cmpGte:
		return a >= b
	default:
		return false
	}
}

func cmpFloat64(op cmpOp, a, b float64) bool {
	switch op {
	case cmpEq:
		return a == b
	case cmpNeq:
		return a != b
	case cmpLt:
		return a < b
	case cmpLte:
		return a <= b
	case cmpGt:
		return a > b
	case cmpGte:
		return a >= b
	default:
		return false
	}
}

func cmpBool(op cmpOp, a, b bool) bool {
	switch op {
	case cmpEq:
		return a == b
	case cmpNeq:
		return a != b
	default:
		// Ordering comparisons are not defined for bool in this engine yet.
		return false
	}
}

type evenExpr struct {
	col string
}

func (e evenExpr) Eval(f Frame) (Mask, error) {
	col, ok := f.Column(e.col)
	if !ok {
		return Mask{}, fmt.Errorf("unknown column %s", e.col)
	}
	vals := make([]bool, col.Len())
	valid := make([]bool, col.Len())
	for i := range vals {
		if col.IsNull(i) {
			continue
		}
		valid[i] = true
		switch c := col.(type) {
		case *array.Int64Column:
			vals[i] = c.Value(i)%2 == 0
		case *array.Float64Column:
			vals[i] = int64(c.Value(i))%2 == 0
		case *array.Utf8Column:
			vals[i] = c.ValueLen(i)%2 == 0
		case *array.BoolColumn:
			vals[i] = !c.Value(i)
		default:
			vals[i] = false
		}
	}
	return Mask{Data: vals, Valid: valid}, nil
}

type isNullExpr struct {
	col    string
	negate bool
}

func (e isNullExpr) Eval(f Frame) (Mask, error) {
	col, ok := f.Column(e.col)
	if !ok {
		return Mask{}, fmt.Errorf("unknown column %s", e.col)
	}
	vals := make([]bool, col.Len())
	valid := make([]bool, col.Len())
	for i := range vals {
		valid[i] = true
		isNull := col.IsNull(i)
		if e.negate {
			vals[i] = !isNull
		} else {
			vals[i] = isNull
		}
	}
	return Mask{Data: vals, Valid: valid}, nil
}

type notExpr struct {
	child Expr
}

func (e notExpr) Eval(f Frame) (Mask, error) {
	m, err := e.child.Eval(f)
	if err != nil {
		return Mask{}, err
	}
	out := make([]bool, len(m.Data))
	valid := make([]bool, len(m.Data))
	for i := range out {
		if i < len(m.Valid) && m.Valid[i] {
			valid[i] = true
			out[i] = !m.Data[i]
		}
	}
	return Mask{Data: out, Valid: valid}, nil
}

type logicalExpr struct {
	left  Expr
	right Expr
	op    string
}

func And(a, b Expr) Expr { return logicalExpr{left: a, right: b, op: "and"} }
func Or(a, b Expr) Expr  { return logicalExpr{left: a, right: b, op: "or"} }

func (e logicalExpr) Eval(f Frame) (Mask, error) {
	la, err := e.left.Eval(f)
	if err != nil {
		return Mask{}, err
	}
	lb, err := e.right.Eval(f)
	if err != nil {
		return Mask{}, err
	}
	if len(la.Data) != len(lb.Data) {
		return Mask{}, fmt.Errorf("logical mask length mismatch")
	}
	out := make([]bool, len(la.Data))
	valid := make([]bool, len(la.Data))
	for i := range out {
		if i >= len(la.Valid) || i >= len(lb.Valid) {
			continue
		}
		if !la.Valid[i] || !lb.Valid[i] {
			continue
		}
		valid[i] = true
		if e.op == "and" {
			out[i] = la.Data[i] && lb.Data[i]
		} else {
			out[i] = la.Data[i] || lb.Data[i]
		}
	}
	return Mask{Data: out, Valid: valid}, nil
}

func ExprColumns(e Expr) []string {
	switch x := e.(type) {
	case compareExpr:
		return []string{x.left}
	case inExpr:
		return []string{x.col}
	case evenExpr:
		return []string{x.col}
	case isNullExpr:
		return []string{x.col}
	case notExpr:
		return ExprColumns(x.child)
	case logicalExpr:
		l := ExprColumns(x.left)
		r := ExprColumns(x.right)
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

func IsEven(e Expr) (string, bool) {
	x, ok := e.(evenExpr)
	if !ok {
		return "", false
	}
	return x.col, true
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
