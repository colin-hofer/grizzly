package array

import "fmt"

// Field describes one named column.
type Field struct {
	Name string
	Type DataType
}

// Schema is an ordered set of fields.
type Schema struct {
	Fields []Field
	index  map[string]int
}

func NewSchema(fields []Field) (Schema, error) {
	idx := make(map[string]int, len(fields))
	for i := range fields {
		if fields[i].Name == "" {
			return Schema{}, fmt.Errorf("field name required")
		}
		if _, ok := idx[fields[i].Name]; ok {
			return Schema{}, fmt.Errorf("duplicate field %s", fields[i].Name)
		}
		idx[fields[i].Name] = i
	}
	return Schema{Fields: fields, index: idx}, nil
}

func (s Schema) Lookup(name string) (Field, bool) {
	i, ok := s.index[name]
	if !ok {
		return Field{}, false
	}
	return s.Fields[i], true
}
