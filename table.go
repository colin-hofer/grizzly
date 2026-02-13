package grizzly

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"grizzly/internal/array"
)

type TableOptions struct {
	Head int
	Tail int

	MaxCols       int
	MaxColWidth   int
	MaxTableWidth int

	// StatsRows controls how many rows are scanned for basic stats.
	// If StatsRows <= 0, stats are disabled.
	StatsRows int
}

func DefaultTableOptions() TableOptions {
	return TableOptions{
		Head:          5,
		Tail:          5,
		MaxCols:       12,
		MaxColWidth:   20,
		MaxTableWidth: 120,
		StatsRows:     10_000,
	}
}

// String formats the DataFrame as a readable ASCII table.
// It intentionally prints only a preview (head/tail) for large dataframes.
func (df *DataFrame) String() string {
	return df.Table(DefaultTableOptions())
}

func (df *DataFrame) Table(opts TableOptions) string {
	if df == nil || df.df == nil {
		return "<nil dataframe>"
	}

	nrows := df.Height()
	ncols := df.Width()

	cols := df.df.Columns()
	if opts.MaxCols <= 0 {
		opts.MaxCols = len(cols)
	}
	showCols := cols
	if len(showCols) > opts.MaxCols {
		showCols = showCols[:opts.MaxCols]
	}

	if opts.Head < 0 {
		opts.Head = 0
	}
	if opts.Tail < 0 {
		opts.Tail = 0
	}

	rowIdx := previewRowIndices(nrows, opts.Head, opts.Tail)

	// Determine width budget.
	maxColWidth := opts.MaxColWidth
	if maxColWidth <= 0 {
		maxColWidth = 20
	}
	maxTableWidth := opts.MaxTableWidth
	if maxTableWidth <= 0 {
		maxTableWidth = 120
	}

	idxWidth := 3
	if nrows > 0 {
		maxIdx := 0
		for _, r := range rowIdx {
			if r >= 0 && r > maxIdx {
				maxIdx = r
			}
		}
		idxWidth = max(idxWidth, len(strconv.Itoa(maxIdx)))
	}

	// If the table would exceed the max width, reduce per-column width.
	if len(showCols) > 0 {
		sepCount := 1 + 1 + len(showCols) // left border + idx separator + col seps
		fixed := sepCount + idxWidth
		avail := maxTableWidth - fixed
		if avail > 0 {
			per := avail / len(showCols)
			if per < maxColWidth {
				maxColWidth = max(6, per)
			}
		}
	}

	widths := make([]int, 1+len(showCols))
	widths[0] = idxWidth
	for j := range showCols {
		w := len(showCols[j].Name())
		if w > maxColWidth {
			w = maxColWidth
		}
		widths[1+j] = w
	}

	// Fit widths to the preview content.
	for _, r := range rowIdx {
		if r < 0 {
			widths[0] = max(widths[0], 3)
			for j := range showCols {
				widths[1+j] = max(widths[1+j], 3)
			}
			continue
		}
		widths[0] = max(widths[0], len(strconv.Itoa(r)))
		for j := range showCols {
			cell := cellString(showCols[j], r)
			cell = truncate(cell, maxColWidth)
			widths[1+j] = max(widths[1+j], len(cell))
		}
	}

	var b strings.Builder
	b.WriteString("DataFrame")
	b.WriteString(fmt.Sprintf(" shape: (%d, %d)", nrows, ncols))
	b.WriteByte('\n')
	if len(showCols) != ncols {
		b.WriteString(fmt.Sprintf("Preview columns: %d of %d\n", len(showCols), ncols))
	}
	b.WriteString(renderStats(showCols, nrows, opts.StatsRows))
	b.WriteString(renderTable(showCols, widths, rowIdx))
	return b.String()
}

func previewRowIndices(nrows, head, tail int) []int {
	if nrows <= 0 {
		return nil
	}
	if head+tail <= 0 {
		return nil
	}
	if head+tail >= nrows {
		idx := make([]int, nrows)
		for i := range idx {
			idx[i] = i
		}
		return idx
	}
	idx := make([]int, 0, head+tail+1)
	for i := 0; i < head && i < nrows; i++ {
		idx = append(idx, i)
	}
	// -1 indicates ellipsis row.
	idx = append(idx, -1)
	start := nrows - tail
	if start < head {
		start = head
	}
	for i := start; i < nrows; i++ {
		idx = append(idx, i)
	}
	return idx
}

func renderTable(cols []array.Column, widths []int, rowIdx []int) string {
	var b strings.Builder
	if len(cols) == 0 {
		b.WriteString("(no columns)\n")
		return b.String()
	}

	// Borders
	border := func() {
		b.WriteByte('+')
		for i := range widths {
			b.WriteString(strings.Repeat("-", widths[i]+2))
			b.WriteByte('+')
		}
		b.WriteByte('\n')
	}

	border()
	// Header
	b.WriteByte('|')
	b.WriteByte(' ')
	b.WriteString(pad(truncate("idx", widths[0]), widths[0]))
	b.WriteByte(' ')
	b.WriteByte('|')
	for j := range cols {
		b.WriteByte(' ')
		b.WriteString(pad(truncate(cols[j].Name(), widths[1+j]), widths[1+j]))
		b.WriteByte(' ')
		b.WriteByte('|')
	}
	b.WriteByte('\n')
	border()

	// Rows
	for _, r := range rowIdx {
		b.WriteByte('|')
		b.WriteByte(' ')
		if r < 0 {
			b.WriteString(pad("...", widths[0]))
		} else {
			b.WriteString(pad(strconv.Itoa(r), widths[0]))
		}
		b.WriteByte(' ')
		b.WriteByte('|')
		for j := range cols {
			b.WriteByte(' ')
			if r < 0 {
				b.WriteString(pad("...", widths[1+j]))
			} else {
				cell := truncate(cellString(cols[j], r), widths[1+j])
				b.WriteString(pad(cell, widths[1+j]))
			}
			b.WriteByte(' ')
			b.WriteByte('|')
		}
		b.WriteByte('\n')
	}
	border()
	return b.String()
}

func cellString(col array.Column, row int) string {
	if col.IsNull(row) {
		return "null"
	}
	s := col.ValueString(row)
	if s == "" {
		if _, ok := col.(*array.Utf8Column); ok {
			return "\"\""
		}
	}
	return s
}

func renderStats(cols []array.Column, nrows int, statsRows int) string {
	if statsRows <= 0 {
		return ""
	}
	limit := statsRows
	if limit > nrows {
		limit = nrows
	}
	if limit <= 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("Columns")
	if limit != nrows {
		b.WriteString(fmt.Sprintf(" (stats: first %d rows)", limit))
	}
	b.WriteString(":\n")

	for i := range cols {
		c := cols[i]
		dt := c.DType().String()
		st := statsForColumn(c, limit)
		b.WriteString("- ")
		b.WriteString(c.Name())
		b.WriteString(": ")
		b.WriteString(dt)
		b.WriteString("  ")
		b.WriteString(st)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.String()
}

func statsForColumn(col array.Column, n int) string {
	// All stats are computed over first n rows.
	nulls := 0
	nonNull := 0

	switch c := col.(type) {
	case *array.Int64Column:
		min := int64(0)
		max := int64(0)
		sum := float64(0)
		set := false
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				nulls++
				continue
			}
			nonNull++
			v := c.Value(i)
			sum += float64(v)
			if !set {
				min, max = v, v
				set = true
				continue
			}
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		if nonNull == 0 {
			return fmt.Sprintf("nulls=%d/%d", nulls, n)
		}
		mean := sum / float64(nonNull)
		return fmt.Sprintf("nulls=%d/%d min=%d max=%d mean=%.4g", nulls, n, min, max, mean)

	case *array.Float64Column:
		min := 0.0
		max := 0.0
		sum := 0.0
		set := false
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				nulls++
				continue
			}
			nonNull++
			v := c.Value(i)
			sum += v
			if !set {
				min, max = v, v
				set = true
				continue
			}
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		if nonNull == 0 {
			return fmt.Sprintf("nulls=%d/%d", nulls, n)
		}
		mean := sum / float64(nonNull)
		// Avoid printing -0.
		if min == 0 {
			min = math.Abs(min)
		}
		if max == 0 {
			max = math.Abs(max)
		}
		return fmt.Sprintf("nulls=%d/%d min=%.4g max=%.4g mean=%.4g", nulls, n, min, max, mean)

	case *array.BoolColumn:
		trues := 0
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				nulls++
				continue
			}
			nonNull++
			if c.Value(i) {
				trues++
			}
		}
		return fmt.Sprintf("nulls=%d/%d true=%d/%d", nulls, n, trues, nonNull)

	case *array.Utf8Column:
		minLen := 0
		maxLen := 0
		set := false
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				nulls++
				continue
			}
			nonNull++
			l := c.ValueLen(i)
			if !set {
				minLen, maxLen = l, l
				set = true
				continue
			}
			if l < minLen {
				minLen = l
			}
			if l > maxLen {
				maxLen = l
			}
		}
		if nonNull == 0 {
			return fmt.Sprintf("nulls=%d/%d", nulls, n)
		}
		return fmt.Sprintf("nulls=%d/%d len=[%d,%d]", nulls, n, minLen, maxLen)

	default:
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				nulls++
				continue
			}
			nonNull++
		}
		return fmt.Sprintf("nulls=%d/%d", nulls, n)
	}
}

func pad(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func truncate(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if len(s) <= width {
		return s
	}
	if width <= 3 {
		return s[:width]
	}
	return s[:width-3] + "..."
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
