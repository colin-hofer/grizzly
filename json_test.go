package grizzly

import (
	"encoding/json"
	"testing"
)

func TestJSONRoundTrip(t *testing.T) {
	age, err := NewNullableColumn("age", []int{21, 0, 40}, []bool{true, false, true})
	if err != nil {
		t.Fatalf("NewNullableColumn failed: %v", err)
	}
	df, err := NewFrame(
		NewColumn("id", []int{1, 2, 3}),
		NewColumn("name", []string{"amy", "bob", "cara"}),
		age,
	)
	if err != nil {
		t.Fatalf("NewFrame failed: %v", err)
	}

	b, err := MarshalFrameJSON(df)
	if err != nil {
		t.Fatalf("MarshalFrameJSON failed: %v", err)
	}
	decoded, err := UnmarshalFrameJSON(b)
	if err != nil {
		t.Fatalf("UnmarshalFrameJSON failed: %v", err)
	}
	if decoded.NumRows() != 3 || decoded.NumCols() != 3 {
		t.Fatalf("decoded shape got (%d,%d), want (3,3)", decoded.NumRows(), decoded.NumCols())
	}
}

func TestJSONNullFrame(t *testing.T) {
	var f Frame
	if err := json.Unmarshal([]byte("null"), &f); err != nil {
		t.Fatalf("unmarshal null failed: %v", err)
	}

	b, err := json.Marshal((*Frame)(nil))
	if err != nil {
		t.Fatalf("marshal nil failed: %v", err)
	}
	if string(b) != "null" {
		t.Fatalf("got %q, want null", string(b))
	}
}

func TestJSONUnknownDTypeFallsBackToAny(t *testing.T) {
	raw := `{"columns":[{"name":"meta","dtype":"example.person","data":[{"id":1}]}]}`
	df, err := UnmarshalFrameJSON([]byte(raw))
	if err != nil {
		t.Fatalf("UnmarshalFrameJSON failed: %v", err)
	}
	v, ok := Row{f: df, row: 0}.Value("meta")
	if !ok {
		t.Fatalf("expected meta value")
	}
	if _, ok := v.(map[string]any); !ok {
		t.Fatalf("expected map[string]any fallback, got %T", v)
	}
}
