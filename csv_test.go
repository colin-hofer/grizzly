package grizzly

import (
	"strings"
	"testing"
)

func TestCSVWriteReadRoundTrip(t *testing.T) {
	age, err := NewNullableColumn("age", []int{10, 0, 30}, []bool{true, false, true})
	if err != nil {
		t.Fatalf("NewNullableColumn failed: %v", err)
	}
	df, err := NewFrame(
		NewColumn("id", []int{1, 2, 3}),
		NewColumn("name", []string{"a", "b", "c"}),
		age,
	)
	if err != nil {
		t.Fatalf("NewFrame failed: %v", err)
	}

	csvText, err := df.MarshalCSV(CSVWriteOptions{NullValue: "NULL"})
	if err != nil {
		t.Fatalf("MarshalCSV failed: %v", err)
	}

	decoded, err := UnmarshalCSV(csvText, CSVReadOptions{NullValue: "NULL"})
	if err != nil {
		t.Fatalf("UnmarshalCSV failed: %v", err)
	}

	if decoded.NumRows() != 3 || decoded.NumCols() != 3 {
		t.Fatalf("decoded shape got (%d,%d), want (3,3)", decoded.NumRows(), decoded.NumCols())
	}
}

func TestCSVReadErrors(t *testing.T) {
	if _, err := UnmarshalCSV("", CSVReadOptions{}); err == nil {
		t.Fatalf("expected error for empty csv")
	}

	dupHeader := "a,a\n1,2\n"
	if _, err := UnmarshalCSV(dupHeader, CSVReadOptions{}); err == nil {
		t.Fatalf("expected error for duplicate header")
	}

	badWidth := "a,b\n1\n"
	if _, err := UnmarshalCSV(badWidth, CSVReadOptions{}); err == nil {
		t.Fatalf("expected error for mismatched row width")
	}
}

func TestCSVSchemaFastPathAndNoHeader(t *testing.T) {
	in := "1|alice\n2|bob\n"
	df, err := UnmarshalCSV(in, CSVReadOptions{
		NoHeader:  true,
		Comma:     '|',
		NullValue: "",
		Schema: map[string]DType{
			"col_1": DTypeInt64,
			"col_2": DTypeString,
		},
	})
	if err != nil {
		t.Fatalf("UnmarshalCSV failed: %v", err)
	}
	if df.NumRows() != 2 || df.NumCols() != 2 {
		t.Fatalf("shape got (%d,%d), want (2,2)", df.NumRows(), df.NumCols())
	}

	out, err := df.MarshalCSV(CSVWriteOptions{Comma: '|', NoHeader: true})
	if err != nil {
		t.Fatalf("MarshalCSV failed: %v", err)
	}
	if strings.TrimSpace(out) == "" {
		t.Fatalf("expected non-empty csv output")
	}
}
