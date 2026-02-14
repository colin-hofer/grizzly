package tests

import (
	"testing"

	g "grizzly"
)

func TestFromStructsBasic(t *testing.T) {
	type Row struct {
		A int64
		B *string
		C bool `grizzly:"flag"`
		D string
	}
	b1 := "x"
	rows := []Row{{A: 1, B: &b1, C: true, D: "hello"}, {A: 2, B: nil, C: false, D: ""}}
	df, err := g.FromStructs(rows)
	if err != nil {
		t.Fatalf("from structs: %v", err)
	}
	if df.Height() != 2 || df.Width() != 4 {
		t.Fatalf("unexpected shape")
	}
	if _, ok := df.Column("C"); ok {
		t.Fatalf("expected tag rename to hide C")
	}
	if _, ok := df.Column("flag"); !ok {
		t.Fatalf("expected renamed column flag")
	}

	bs, _ := df.Column("B")
	bc, ok := bs.Utf8()
	if !ok {
		t.Fatalf("expected utf8")
	}
	if bc.IsNull(0) || bc.Value(0) != "x" {
		t.Fatalf("unexpected b row0")
	}
	if !bc.IsNull(1) {
		t.Fatalf("expected null in row1")
	}
}

func TestFromStructsJSONTags(t *testing.T) {
	type Row struct {
		UserID int64  `json:"user_id"`
		Name   string `json:"name"`
	}
	df, err := g.FromStructs([]Row{{UserID: 7, Name: "a"}}, g.FromStructsUseJSONTags())
	if err != nil {
		t.Fatalf("from structs: %v", err)
	}
	if _, ok := df.Column("user_id"); !ok {
		t.Fatalf("expected json tag name")
	}
}
