package array

import "fmt"

// WithName returns a new column value that shares the underlying buffers
// with col, but reports the provided name.
//
// This is safe because columns are immutable by contract.
func WithName(col Column, name string) (Column, error) {
	if col == nil {
		return nil, fmt.Errorf("nil column")
	}
	if name == "" {
		return nil, fmt.Errorf("empty column name")
	}
	switch c := col.(type) {
	case *Int64Column:
		out := *c
		out.name = name
		return &out, nil
	case *Float64Column:
		out := *c
		out.name = name
		return &out, nil
	case *BoolColumn:
		out := *c
		out.name = name
		return &out, nil
	case *Utf8Column:
		out := *c
		out.name = name
		return &out, nil
	default:
		return nil, fmt.Errorf("unsupported column type for rename")
	}
}
