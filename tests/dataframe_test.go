package tests

import (
	"strings"
	"testing"

	g "grizzly"
)

func TestDataFrameConvenienceOps(t *testing.T) {
	a := g.MustNewInt64Column("a", []int64{1, 2, 3}, nil)
	b := g.MustNewUtf8Column("b", []string{"x", "y", "z"}, nil)
	df, err := g.NewDataFrame(a, b)
	if err != nil {
		t.Fatalf("new dataframe: %v", err)
	}

	head, err := df.Head(2)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.Height() != 2 {
		t.Fatalf("expected head height 2 got %d", head.Height())
	}

	tail, err := df.Tail(1)
	if err != nil {
		t.Fatalf("tail: %v", err)
	}
	if tail.Height() != 1 {
		t.Fatalf("expected tail height 1 got %d", tail.Height())
	}
	s, ok := tail.Column("a")
	if !ok {
		t.Fatalf("missing column a")
	}
	ic, ok := s.Int64()
	if !ok {
		t.Fatalf("expected int64 column")
	}
	if ic.Value(0) != 3 {
		t.Fatalf("expected tail a=3 got %d", ic.Value(0))
	}

	slice, err := df.Slice(1, 2)
	if err != nil {
		t.Fatalf("slice: %v", err)
	}
	s2, ok := slice.Column("a")
	if !ok {
		t.Fatalf("missing column a")
	}
	ic2, ok := s2.Int64()
	if !ok {
		t.Fatalf("expected int64 column")
	}
	if ic2.Value(0) != 2 || ic2.Value(1) != 3 {
		t.Fatalf("unexpected slice values")
	}

	dropped, err := df.Drop("b")
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if dropped.Width() != 1 {
		t.Fatalf("expected width 1 got %d", dropped.Width())
	}
	if _, ok := dropped.Column("b"); ok {
		t.Fatalf("expected column b to be dropped")
	}

	renamed, err := df.Rename("a", "x")
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, ok := renamed.Column("a"); ok {
		t.Fatalf("expected old name to be absent")
	}
	if _, ok := renamed.Column("x"); !ok {
		t.Fatalf("expected renamed column x")
	}

	replaced, err := df.WithColumns(g.MustNewInt64Column("a", []int64{9, 9, 9}, nil))
	if err != nil {
		t.Fatalf("with columns replace: %v", err)
	}
	s3, _ := replaced.Column("a")
	if ic3, ok := s3.Int64(); !ok || ic3.Value(0) != 9 {
		t.Fatalf("expected replaced a=9")
	}

	appended, err := df.WithColumns(g.MustNewBoolColumn("c", []bool{true, false, true}, nil))
	if err != nil {
		t.Fatalf("with columns append: %v", err)
	}
	if appended.Width() != 3 {
		t.Fatalf("expected width 3 got %d", appended.Width())
	}
}

func TestGroupByCountInt64Key(t *testing.T) {
	k, err := g.NewInt64Column("k", []int64{1, 1, 2, 0}, []bool{true, true, true, false})
	if err != nil {
		t.Fatalf("new column: %v", err)
	}
	v := g.MustNewUtf8Column("v", []string{"a", "b", "c", "d"}, nil)
	df, err := g.NewDataFrame(k, v)
	if err != nil {
		t.Fatalf("new dataframe: %v", err)
	}

	gb, err := df.GroupBy("k")
	if err != nil {
		t.Fatalf("groupby: %v", err)
	}
	out, err := gb.Count()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if out.Height() != 3 {
		t.Fatalf("expected 3 groups got %d", out.Height())
	}

	ks, ok := out.Column("k")
	if !ok {
		t.Fatalf("missing k")
	}
	ki, ok := ks.Int64()
	if !ok {
		t.Fatalf("expected int64 k")
	}
	cs, ok := out.Column("count")
	if !ok {
		t.Fatalf("missing count")
	}
	ci, ok := cs.Int64()
	if !ok {
		t.Fatalf("expected int64 count")
	}

	if ki.IsNull(0) || ki.Value(0) != 1 || ci.Value(0) != 2 {
		t.Fatalf("unexpected group 0")
	}
	if ki.IsNull(1) || ki.Value(1) != 2 || ci.Value(1) != 1 {
		t.Fatalf("unexpected group 1")
	}
	if !ki.IsNull(2) || ci.Value(2) != 1 {
		t.Fatalf("unexpected group 2")
	}
}

func TestGroupByAggsInt64(t *testing.T) {
	k := g.MustNewInt64Column("k", []int64{1, 1, 2, 2, 2}, nil)
	v, err := g.NewInt64Column("v", []int64{10, 0, 1, 0, 3}, []bool{true, false, true, false, true})
	if err != nil {
		t.Fatalf("new v: %v", err)
	}
	df, err := g.NewDataFrame(k, v)
	if err != nil {
		t.Fatalf("new dataframe: %v", err)
	}
	gb, err := df.GroupBy("k")
	if err != nil {
		t.Fatalf("groupby: %v", err)
	}
	out, err := gb.Agg(
		g.Count(),
		g.Sum("v").As("v_sum"),
		g.Mean("v").As("v_mean"),
		g.Min("v").As("v_min"),
		g.Max("v").As("v_max"),
	)
	if err != nil {
		t.Fatalf("agg: %v", err)
	}
	if out.Height() != 2 {
		t.Fatalf("expected 2 groups got %d", out.Height())
	}

	ks, _ := out.Column("k")
	ki, _ := ks.Int64()
	if ki.Value(0) != 1 || ki.Value(1) != 2 {
		t.Fatalf("unexpected group keys")
	}

	cs, _ := out.Column("count")
	ci, _ := cs.Int64()
	if ci.Value(0) != 2 || ci.Value(1) != 3 {
		t.Fatalf("unexpected counts")
	}

	ss, _ := out.Column("v_sum")
	si, _ := ss.Int64()
	if si.Value(0) != 10 || si.Value(1) != 4 {
		t.Fatalf("unexpected sums")
	}

	ms, _ := out.Column("v_mean")
	mi, ok := ms.Float64()
	if !ok {
		t.Fatalf("expected float64 mean")
	}
	if mi.Value(0) != 10 || mi.Value(1) != 2 {
		t.Fatalf("unexpected means")
	}

	mins, _ := out.Column("v_min")
	mini, _ := mins.Int64()
	if mini.Value(0) != 10 || mini.Value(1) != 1 {
		t.Fatalf("unexpected mins")
	}

	maxs, _ := out.Column("v_max")
	maxi, _ := maxs.Int64()
	if maxi.Value(0) != 10 || maxi.Value(1) != 3 {
		t.Fatalf("unexpected maxs")
	}
}

func TestGroupByAggsUtf8Key(t *testing.T) {
	k := g.MustNewUtf8Column("k", []string{"a", "a", "b", "b"}, nil)
	v := g.MustNewFloat64Column("v", []float64{1.0, 2.0, 3.0, 0}, []bool{true, true, true, false})
	df, err := g.NewDataFrame(k, v)
	if err != nil {
		t.Fatalf("new dataframe: %v", err)
	}
	gb, err := df.GroupBy("k")
	if err != nil {
		t.Fatalf("groupby: %v", err)
	}
	out, err := gb.Agg(g.Count(), g.Mean("v").As("m"), g.Min("v").As("mn"), g.Max("v").As("mx"))
	if err != nil {
		t.Fatalf("agg: %v", err)
	}
	if out.Height() != 2 {
		t.Fatalf("expected 2 groups got %d", out.Height())
	}
	ks, _ := out.Column("k")
	kc, ok := ks.Utf8()
	if !ok {
		t.Fatalf("expected utf8 key output")
	}
	if kc.Value(0) != "a" || kc.Value(1) != "b" {
		t.Fatalf("unexpected key order")
	}

	ms, _ := out.Column("m")
	mc, ok := ms.Float64()
	if !ok {
		t.Fatalf("expected float64 mean")
	}
	if mc.Value(0) != 1.5 || mc.Value(1) != 3 {
		t.Fatalf("unexpected means")
	}
}

func TestExprIn(t *testing.T) {
	a := g.MustNewInt64Column("a", []int64{1, 2, 3, 4}, nil)
	df, err := g.NewDataFrame(a)
	if err != nil {
		t.Fatalf("new dataframe: %v", err)
	}
	out, err := df.Filter(g.Col("a").In(1, 3, 99))
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if out.Height() != 2 {
		t.Fatalf("expected 2 rows got %d", out.Height())
	}
	s, _ := out.Column("a")
	ic, ok := s.Int64()
	if !ok {
		t.Fatalf("expected int64")
	}
	if ic.Value(0) != 1 || ic.Value(1) != 3 {
		t.Fatalf("unexpected filtered values")
	}
}

func TestDataFrameStringPreview(t *testing.T) {
	a := g.MustNewInt64Column("a", []int64{1, 2, 3, 4, 5, 6, 7}, nil)
	b := g.MustNewUtf8Column("b", []string{"x", "", "z", "w", "u", "v", "t"}, nil)
	df, err := g.NewDataFrame(a, b)
	if err != nil {
		t.Fatalf("new dataframe: %v", err)
	}

	s := df.String()
	if !strings.Contains(s, "DataFrame shape: (7, 2)") {
		t.Fatalf("missing shape in output: %s", s)
	}
	if !strings.Contains(s, "| idx ") {
		t.Fatalf("missing header row")
	}
	if !strings.Contains(s, "| 0 ") || !strings.Contains(s, "| 6 ") {
		t.Fatalf("missing expected indices")
	}
	if !strings.Contains(s, "\"\"") {
		t.Fatalf("expected empty utf8 to be printed as \"\"")
	}
}
