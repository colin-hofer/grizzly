package grizzly

type Row struct {
	f   *Frame
	row int
}

func (r Row) Value(col string) (any, bool) {
	c, ok := r.f.columns[col]
	if !ok {
		return nil, false
	}
	if !c.ValidAt(r.row) {
		return nil, false
	}
	return c.ValueAt(r.row), true
}
