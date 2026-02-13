package grizzly

import (
	"context"
	"fmt"

	"grizzly/internal/array"
	"grizzly/internal/exec"
	"grizzly/internal/expr"
	"grizzly/internal/plan"
)

// This package is the public API surface.
//
// Implementation lives in internal packages:
// - internal/array: columns, bitmaps, schema
// - internal/expr: expression tree + evaluation
// - internal/plan: lazy plans + optimizer rules
// - internal/exec: kernels and dataframe execution

type Kind = array.Kind
type TimeUnit = array.TimeUnit
type DataType = array.DataType

const (
	KindInvalid  = array.KindInvalid
	KindInt      = array.KindInt
	KindUInt     = array.KindUInt
	KindFloat    = array.KindFloat
	KindBool     = array.KindBool
	KindUtf8     = array.KindUtf8
	KindBinary   = array.KindBinary
	KindDate     = array.KindDate
	KindDatetime = array.KindDatetime
	KindDuration = array.KindDuration
)

const (
	TimeUnitInvalid = array.TimeUnitInvalid
	TimeUnitNS      = array.TimeUnitNS
	TimeUnitUS      = array.TimeUnitUS
	TimeUnitMS      = array.TimeUnitMS
)

func Int(bits uint8) DataType                    { return array.Int(bits) }
func UInt(bits uint8) DataType                   { return array.UInt(bits) }
func Float(bits uint8) DataType                  { return array.Float(bits) }
func Bool() DataType                             { return array.Bool() }
func Utf8() DataType                             { return array.Utf8() }
func Binary() DataType                           { return array.Binary() }
func Date() DataType                             { return array.Date() }
func Datetime(unit TimeUnit, tz string) DataType { return array.Datetime(unit, tz) }
func Duration(unit TimeUnit) DataType            { return array.Duration(unit) }

type Field struct {
	Name string
	Type DataType
}

type Schema struct {
	Fields []Field
}

func (s Schema) Lookup(name string) (Field, bool) {
	for i := range s.Fields {
		if s.Fields[i].Name == name {
			return s.Fields[i], true
		}
	}
	return Field{}, false
}

type Series struct {
	col array.Column
}

// Column is any value that can provide an internal immutable column.
//
// This includes Series values taken from a DataFrame as well as concrete
// column wrappers created via NewInt64Column/NewUtf8Column/etc.
type Column interface {
	internalColumn() array.Column
}

func (s Series) internalColumn() array.Column { return s.col }

func (s Series) Name() string    { return s.col.Name() }
func (s Series) DType() DataType { return s.col.DType() }
func (s Series) Len() int        { return s.col.Len() }
func (s Series) IsNull(i int) bool {
	return s.col.IsNull(i)
}

// ValueString returns a printable representation for a value.
// It returns an empty string for NULL.
func (s Series) ValueString(i int) string { return s.col.ValueString(i) }

func (s Series) Int64() (*Int64Column, bool) {
	c, ok := s.col.(*array.Int64Column)
	if !ok {
		return nil, false
	}
	return &Int64Column{col: c}, true
}
func (s Series) Float64() (*Float64Column, bool) {
	c, ok := s.col.(*array.Float64Column)
	if !ok {
		return nil, false
	}
	return &Float64Column{col: c}, true
}
func (s Series) Bool() (*BoolColumn, bool) {
	c, ok := s.col.(*array.BoolColumn)
	if !ok {
		return nil, false
	}
	return &BoolColumn{col: c}, true
}
func (s Series) Utf8() (*Utf8Column, bool) {
	c, ok := s.col.(*array.Utf8Column)
	if !ok {
		return nil, false
	}
	return &Utf8Column{col: c}, true
}

// Concrete column wrappers.
//
// These intentionally do not expose internal backing storage.
type Int64Column struct{ col *array.Int64Column }
type Float64Column struct{ col *array.Float64Column }
type BoolColumn struct{ col *array.BoolColumn }
type Utf8Column struct{ col *array.Utf8Column }

func (c *Int64Column) internalColumn() array.Column   { return c.col }
func (c *Float64Column) internalColumn() array.Column { return c.col }
func (c *BoolColumn) internalColumn() array.Column    { return c.col }
func (c *Utf8Column) internalColumn() array.Column    { return c.col }

func (c *Int64Column) Name() string      { return c.col.Name() }
func (c *Int64Column) DType() DataType   { return c.col.DType() }
func (c *Int64Column) Len() int          { return c.col.Len() }
func (c *Int64Column) IsNull(i int) bool { return c.col.IsNull(i) }
func (c *Int64Column) Value(i int) int64 { return c.col.Value(i) }
func (c *Int64Column) Values() []int64   { return c.col.Values() }

func (c *Float64Column) Name() string        { return c.col.Name() }
func (c *Float64Column) DType() DataType     { return c.col.DType() }
func (c *Float64Column) Len() int            { return c.col.Len() }
func (c *Float64Column) IsNull(i int) bool   { return c.col.IsNull(i) }
func (c *Float64Column) Value(i int) float64 { return c.col.Value(i) }
func (c *Float64Column) Values() []float64   { return c.col.Values() }

func (c *BoolColumn) Name() string      { return c.col.Name() }
func (c *BoolColumn) DType() DataType   { return c.col.DType() }
func (c *BoolColumn) Len() int          { return c.col.Len() }
func (c *BoolColumn) IsNull(i int) bool { return c.col.IsNull(i) }
func (c *BoolColumn) Value(i int) bool  { return c.col.Value(i) }
func (c *BoolColumn) Values() []bool    { return c.col.Values() }

func (c *Utf8Column) Name() string       { return c.col.Name() }
func (c *Utf8Column) DType() DataType    { return c.col.DType() }
func (c *Utf8Column) Len() int           { return c.col.Len() }
func (c *Utf8Column) IsNull(i int) bool  { return c.col.IsNull(i) }
func (c *Utf8Column) Value(i int) string { return c.col.Value(i) }
func (c *Utf8Column) Values() []string   { return c.col.Values() }

func NewInt64Column(name string, data []int64, valid []bool) (*Int64Column, error) {
	c, err := array.NewInt64Column(name, data, valid)
	if err != nil {
		return nil, err
	}
	return &Int64Column{col: c}, nil
}
func NewFloat64Column(name string, data []float64, valid []bool) (*Float64Column, error) {
	c, err := array.NewFloat64Column(name, data, valid)
	if err != nil {
		return nil, err
	}
	return &Float64Column{col: c}, nil
}
func NewBoolColumn(name string, data []bool, valid []bool) (*BoolColumn, error) {
	c, err := array.NewBoolColumn(name, data, valid)
	if err != nil {
		return nil, err
	}
	return &BoolColumn{col: c}, nil
}
func NewUtf8Column(name string, data []string, valid []bool) (*Utf8Column, error) {
	c, err := array.NewUtf8Column(name, data, valid)
	if err != nil {
		return nil, err
	}
	return &Utf8Column{col: c}, nil
}

func MustNewInt64Column(name string, data []int64, valid []bool) *Int64Column {
	c, err := NewInt64Column(name, data, valid)
	if err != nil {
		panic(err)
	}
	return c
}
func MustNewFloat64Column(name string, data []float64, valid []bool) *Float64Column {
	c, err := NewFloat64Column(name, data, valid)
	if err != nil {
		panic(err)
	}
	return c
}
func MustNewBoolColumn(name string, data []bool, valid []bool) *BoolColumn {
	c, err := NewBoolColumn(name, data, valid)
	if err != nil {
		panic(err)
	}
	return c
}
func MustNewUtf8Column(name string, data []string, valid []bool) *Utf8Column {
	c, err := NewUtf8Column(name, data, valid)
	if err != nil {
		panic(err)
	}
	return c
}

type DataFrame struct {
	df *exec.DataFrame
}

func NewDataFrame(cols ...Column) (*DataFrame, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("at least one column required")
	}
	internal := make([]array.Column, len(cols))
	for i := range cols {
		if cols[i] == nil {
			return nil, fmt.Errorf("nil column")
		}
		internal[i] = cols[i].internalColumn()
		if internal[i] == nil {
			return nil, fmt.Errorf("nil column")
		}
	}
	df, err := exec.NewDataFrame(internal...)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: df}, nil
}

func (df *DataFrame) Height() int { return df.df.Height() }
func (df *DataFrame) Width() int  { return df.df.Width() }
func (df *DataFrame) Schema() Schema {
	s := df.df.Schema()
	out := make([]Field, len(s.Fields))
	for i := range s.Fields {
		out[i] = Field{Name: s.Fields[i].Name, Type: s.Fields[i].Type}
	}
	return Schema{Fields: out}
}

func (df *DataFrame) Columns() []Series {
	cols := df.df.Columns()
	out := make([]Series, len(cols))
	for i := range cols {
		out[i] = Series{col: cols[i]}
	}
	return out
}

func (df *DataFrame) Column(name string) (Series, bool) {
	c, ok := df.df.Column(name)
	if !ok {
		return Series{}, false
	}
	return Series{col: c}, true
}

func (df *DataFrame) Select(names ...string) (*DataFrame, error) {
	out, err := df.df.Select(names...)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: out}, nil
}

func (df *DataFrame) Filter(e Expr) (*DataFrame, error) {
	if e == nil {
		return nil, fmt.Errorf("nil expression")
	}
	out, err := df.df.Filter(e.internal())
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: out}, nil
}

func (df *DataFrame) Slice(offset, length int) (*DataFrame, error) {
	out, err := df.df.Slice(offset, length)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: out}, nil
}

func (df *DataFrame) Head(n int) (*DataFrame, error) {
	out, err := df.df.Head(n)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: out}, nil
}

func (df *DataFrame) Tail(n int) (*DataFrame, error) {
	out, err := df.df.Tail(n)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: out}, nil
}

func (df *DataFrame) Limit(n int) (*DataFrame, error) {
	out, err := df.df.Limit(n)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: out}, nil
}

func (df *DataFrame) Drop(names ...string) (*DataFrame, error) {
	out, err := df.df.Drop(names...)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: out}, nil
}

func (df *DataFrame) Rename(oldName, newName string) (*DataFrame, error) {
	out, err := df.df.Rename(oldName, newName)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: out}, nil
}

func (df *DataFrame) WithColumns(cols ...Column) (*DataFrame, error) {
	if len(cols) == 0 {
		return df, nil
	}
	internal := make([]array.Column, len(cols))
	for i := range cols {
		if cols[i] == nil {
			return nil, fmt.Errorf("nil column")
		}
		internal[i] = cols[i].internalColumn()
		if internal[i] == nil {
			return nil, fmt.Errorf("nil column")
		}
	}
	out, err := df.df.WithColumns(internal...)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: out}, nil
}

type GroupBy struct {
	gb *exec.GroupBy
}

type AggFunc = exec.AggFunc

const (
	AggInvalid = exec.AggInvalid
	AggCount   = exec.AggCount
	AggSum     = exec.AggSum
	AggMean    = exec.AggMean
	AggMin     = exec.AggMin
	AggMax     = exec.AggMax
)

type Agg struct {
	Col   string
	Func  AggFunc
	Alias string
}

func (a Agg) As(alias string) Agg {
	a.Alias = alias
	return a
}

func Count() Agg          { return Agg{Func: AggCount, Alias: "count"} }
func Sum(col string) Agg  { return Agg{Func: AggSum, Col: col} }
func Mean(col string) Agg { return Agg{Func: AggMean, Col: col} }
func Min(col string) Agg  { return Agg{Func: AggMin, Col: col} }
func Max(col string) Agg  { return Agg{Func: AggMax, Col: col} }

func (df *DataFrame) GroupBy(keys ...string) (*GroupBy, error) {
	gb, err := df.df.GroupBy(keys...)
	if err != nil {
		return nil, err
	}
	return &GroupBy{gb: gb}, nil
}

func (g *GroupBy) Count() (*DataFrame, error) {
	df, err := g.gb.Count()
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: df}, nil
}

func (g *GroupBy) Agg(specs ...Agg) (*DataFrame, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("agg requires at least one spec")
	}
	internal := make([]exec.AggSpec, len(specs))
	for i := range specs {
		internal[i] = exec.AggSpec{Col: specs[i].Col, Func: specs[i].Func, Alias: specs[i].Alias}
	}
	df, err := g.gb.Agg(internal...)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: df}, nil
}

func (df *DataFrame) SortBy(column string, desc bool) (*DataFrame, error) {
	out, err := df.df.SortBy(column, desc)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: out}, nil
}

func (df *DataFrame) ProjectionChecksum(maxCols int) string {
	return df.df.ProjectionChecksum(maxCols)
}

func (df *DataFrame) MarshalRowsJSON() ([]byte, error) {
	return df.df.MarshalRowsJSON()
}

type ScanOptions = plan.ScanOptions

type LazyFrame struct {
	lf *plan.LazyFrame
}

func ScanCSV(path string, opts ScanOptions) *LazyFrame {
	return &LazyFrame{lf: plan.ScanCSV(path, opts)}
}
func ScanJSON(path string) *LazyFrame { return &LazyFrame{lf: plan.ScanJSON(path)} }

func (lf *LazyFrame) Select(cols ...string) *LazyFrame {
	return &LazyFrame{lf: lf.lf.Select(cols...)}
}
func (lf *LazyFrame) Filter(e Expr) *LazyFrame {
	return &LazyFrame{lf: lf.lf.Filter(e.internal())}
}
func (lf *LazyFrame) Sort(col string, desc bool) *LazyFrame {
	return &LazyFrame{lf: lf.lf.Sort(col, desc)}
}
func (lf *LazyFrame) Collect() (*DataFrame, error) {
	return lf.CollectContext(context.Background())
}

func (lf *LazyFrame) CollectContext(ctx context.Context) (*DataFrame, error) {
	df, err := lf.lf.CollectContext(ctx)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: df}, nil
}

func (lf *LazyFrame) Explain() (string, error) {
	return lf.lf.Explain()
}

// Expression wrappers

type Expr interface {
	internal() expr.Expr
}

type wrappedExpr struct{ e expr.Expr }

func (w wrappedExpr) internal() expr.Expr { return w.e }

type ColRef struct{ name string }

func Col(name string) ColRef { return ColRef{name: name} }

func (c ColRef) Eq(v any) Expr  { return wrappedExpr{e: expr.Col(c.name).Eq(v)} }
func (c ColRef) Neq(v any) Expr { return wrappedExpr{e: expr.Col(c.name).Neq(v)} }
func (c ColRef) Lt(v any) Expr  { return wrappedExpr{e: expr.Col(c.name).Lt(v)} }
func (c ColRef) Lte(v any) Expr { return wrappedExpr{e: expr.Col(c.name).Lte(v)} }
func (c ColRef) Gt(v any) Expr  { return wrappedExpr{e: expr.Col(c.name).Gt(v)} }
func (c ColRef) Gte(v any) Expr { return wrappedExpr{e: expr.Col(c.name).Gte(v)} }
func (c ColRef) Even() Expr     { return wrappedExpr{e: expr.Col(c.name).Even()} }
func (c ColRef) IsNull() Expr   { return wrappedExpr{e: expr.Col(c.name).IsNull()} }
func (c ColRef) IsNotNull() Expr {
	return wrappedExpr{e: expr.Col(c.name).IsNotNull()}
}
func (c ColRef) In(vals ...any) Expr { return wrappedExpr{e: expr.Col(c.name).In(vals...)} }

func Not(e Expr) Expr { return wrappedExpr{e: expr.Not(e.internal())} }

func And(a, b Expr) Expr { return wrappedExpr{e: expr.And(a.internal(), b.internal())} }
func Or(a, b Expr) Expr  { return wrappedExpr{e: expr.Or(a.internal(), b.internal())} }
