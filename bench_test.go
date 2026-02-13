package grizzly

import (
	"strconv"
	"strings"
	"testing"
)

func makeBenchFrame(b *testing.B, n int) *Frame {
	b.Helper()
	ids := make([]int, n)
	names := make([]string, n)
	scores := make([]float64, n)
	valid := make([]bool, n)
	for i := 0; i < n; i++ {
		ids[i] = n - i
		names[i] = "name_" + strconv.Itoa(i%1000)
		scores[i] = float64(i%100) / 3.0
		valid[i] = i%10 != 0
	}
	scoreCol, err := NewNullableColumn("score", scores, valid)
	if err != nil {
		b.Fatalf("NewNullableColumn failed: %v", err)
	}
	df, err := NewFrame(
		NewColumn("id", ids),
		NewColumn("name", names),
		scoreCol,
	)
	if err != nil {
		b.Fatalf("NewFrame failed: %v", err)
	}
	return df
}

func makeBenchCSV(rows int) string {
	var sb strings.Builder
	sb.WriteString("id,name,score\n")
	for i := 0; i < rows; i++ {
		sb.WriteString(strconv.Itoa(i + 1))
		sb.WriteByte(',')
		sb.WriteString("name_")
		sb.WriteString(strconv.Itoa(i % 500))
		sb.WriteByte(',')
		if i%9 == 0 {
			sb.WriteString("NULL")
		} else {
			sb.WriteString(strconv.Itoa((i % 100) + 1))
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

func BenchmarkFrameSort(b *testing.B) {
	base := makeBenchFrame(b, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mat, err := base.Materialize()
		if err != nil {
			b.Fatal(err)
		}
		if err := mat.SortBy(SortKey{Name: "name"}, SortKey{Name: "id"}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrameFilterMaterialize(b *testing.B) {
	base := makeBenchFrame(b, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := base.Filter(func(r Row) bool {
			v, ok := r.Value("score")
			return ok && v.(float64) > 10
		})
		if _, err := out.Materialize(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCSVReadInfer(b *testing.B) {
	data := makeBenchCSV(50000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := UnmarshalCSV(data, CSVReadOptions{NullValue: "NULL"}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCSVReadSchema(b *testing.B) {
	data := makeBenchCSV(50000)
	opts := CSVReadOptions{
		NullValue: "NULL",
		Schema: map[string]DType{
			"id":    DTypeInt64,
			"name":  DTypeString,
			"score": DTypeFloat64,
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := UnmarshalCSV(data, opts); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONMarshal(b *testing.B) {
	df := makeBenchFrame(b, 50000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := MarshalFrameJSON(df); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONUnmarshal(b *testing.B) {
	df := makeBenchFrame(b, 50000)
	blob, err := MarshalFrameJSON(df)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := UnmarshalFrameJSON(blob); err != nil {
			b.Fatal(err)
		}
	}
}
