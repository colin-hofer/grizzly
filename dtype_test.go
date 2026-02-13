package grizzly

import "testing"

func TestNormalizeAndParseDType(t *testing.T) {
	if got := normalizeDType("  INT64 "); got != DTypeInt64 {
		t.Fatalf("normalize got %q, want %q", got, DTypeInt64)
	}
	if _, err := ParseDType(" "); err == nil {
		t.Fatalf("expected parse error for empty dtype")
	}
	if got, err := ParseDType("string"); err != nil || got != DTypeString {
		t.Fatalf("ParseDType got (%q,%v), want (%q,nil)", got, err, DTypeString)
	}
}

func TestDTypeFromValue(t *testing.T) {
	if got := dtypeFromValue(1); got != DTypeInt {
		t.Fatalf("int dtype got %q", got)
	}
	if got := dtypeFromValue("x"); got != DTypeString {
		t.Fatalf("string dtype got %q", got)
	}
	type custom struct{ A int }
	if got := dtypeFromValue(custom{}); got != DTypeUnknown {
		t.Fatalf("custom dtype got %q, want unknown", got)
	}
}
