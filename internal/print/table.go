package print

import (
	"fmt"
	"grizzly/internal/array"
	"grizzly/internal/exec"
	"math"
	"os"
	"strconv"
	"strings"
)

type tableCol struct {
	name string
	col  array.Column
}

func tableOverhead(idxWidth int, nDataCols int) int {
	// The rendered table has:
	// - 1 idx column + nDataCols columns
	// - each cell adds 2 spaces and 1 separator => width+3
	// - plus a leading separator char
	// Total line width = 1 + 3*m + sum(widths) where m = 1 + nDataCols.
	m := 1 + nDataCols
	return 1 + 3*m + idxWidth
}

func tableLineWidth(widths []int) int {
	sum := 0
	for i := range widths {
		sum += widths[i]
	}
	return 1 + 3*len(widths) + sum
}

func enforceTableWidth(widths []int, maxWidth int) {
	if maxWidth <= 0 {
		return
	}
	if len(widths) == 0 {
		return
	}
	minCell := 4
	// Keep shrinking the widest non-index column until we fit.
	for tableLineWidth(widths) > maxWidth {
		best := -1
		bestW := 0
		for i := 1; i < len(widths); i++ {
			if widths[i] > bestW && widths[i] > minCell {
				best = i
				bestW = widths[i]
			}
		}
		if best < 0 {
			return
		}
		widths[best]--
	}
}

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
		MaxTableWidth: 0, // auto
		StatsRows:     10_000,
	}
}

func Table(df *exec.DataFrame, opts TableOptions) string {
	if df == nil {
		return "<nil dataframe>"
	}

	nrows := df.Height()
	ncols := df.Width()

	cols := df.Columns()

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
		maxTableWidth = detectStdoutWidth(120)
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

	show := selectTableCols(cols, maxTableWidth, idxWidth, maxColWidth, opts.MaxCols)

	// Cap per-column width to fit the table budget.
	if len(show) > 0 {
		overhead := tableOverhead(idxWidth, len(show))
		avail := maxTableWidth - overhead
		if avail > 0 {
			per := avail / len(show)
			if per < maxColWidth {
				maxColWidth = max(4, per)
			}
		}
	}

	widths := make([]int, 1+len(show))
	widths[0] = idxWidth
	for j := range show {
		w := len(show[j].name)
		if w > maxColWidth {
			w = maxColWidth
		}
		widths[1+j] = w
	}

	// Fit widths to the preview content.
	for _, r := range rowIdx {
		if r < 0 {
			widths[0] = max(widths[0], 3)
			for j := range show {
				widths[1+j] = max(widths[1+j], 3)
			}
			continue
		}
		widths[0] = max(widths[0], len(strconv.Itoa(r)))
		for j := range show {
			cell := cellString(show[j], r)
			cell = truncate(cell, maxColWidth)
			widths[1+j] = max(widths[1+j], len(cell))
		}
	}

	// Final hard clamp: shrink columns until the table fits.
	enforceTableWidth(widths, maxTableWidth)

	var b strings.Builder
	b.WriteString("DataFrame")
	b.WriteString(fmt.Sprintf(" shape: (%d, %d)", nrows, ncols))
	b.WriteByte('\n')
	if countRealCols(show) != ncols {
		b.WriteString(fmt.Sprintf("Preview columns: %d of %d\n", countRealCols(show), ncols))
	}
	b.WriteString(renderStats(show, nrows, opts.StatsRows, maxTableWidth))
	b.WriteString(renderTable(show, widths, rowIdx))
	return b.String()
}

func detectStdoutWidth(fallback int) int {
	if w, ok := stdoutTerminalWidth(); ok {
		// Be conservative: many terminals will wrap if we hit exactly the limit.
		if w >= 40 {
			return w - 1
		}
	}
	if v := strings.TrimSpace(os.Getenv("COLUMNS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 40 {
			return n
		}
	}
	return fallback
}

func countRealCols(cols []tableCol) int {
	n := 0
	for i := range cols {
		if cols[i].col != nil {
			n++
		}
	}
	return n
}

func selectTableCols(cols []array.Column, maxTableWidth, idxWidth, maxColWidth, maxCols int) []tableCol {
	if len(cols) == 0 {
		return nil
	}
	if maxCols <= 0 || maxCols > len(cols) {
		maxCols = len(cols)
	}
	minColWidth := 6
	if maxColWidth < minColWidth {
		minColWidth = maxColWidth
	}
	if minColWidth < 4 {
		minColWidth = 4
	}

	// Best-effort: keep as many columns as can fit at min width.
	// Total line width is: overhead + sum(widths of shown data cols).
	fits := func(n int) bool {
		if n <= 0 {
			return true
		}
		overhead := tableOverhead(idxWidth, n)
		need := overhead + n*minColWidth
		return need <= maxTableWidth
	}

	// If we can't fit even one real column plus idx, force one.
	if !fits(1) {
		return []tableCol{{name: cols[0].Name(), col: cols[0]}}
	}

	// Start with maxCols, reduce until it fits.
	n := maxCols
	for n > 1 && !fits(n) {
		n--
	}
	if n >= len(cols) {
		out := make([]tableCol, len(cols))
		for i := range cols {
			out[i] = tableCol{name: cols[i].Name(), col: cols[i]}
		}
		return out
	}

	// If we truncated columns, attempt Polars-like elision: left ... right.
	// Use one slot for "...".
	if n <= 2 {
		out := make([]tableCol, n)
		for i := 0; i < n; i++ {
			out[i] = tableCol{name: cols[i].Name(), col: cols[i]}
		}
		return out
	}

	left := (n - 1) / 2
	right := n - 1 - left
	if left < 1 {
		left = 1
	}
	if right < 1 {
		right = 1
	}
	if left+right+1 != n {
		right = n - 1 - left
	}

	out := make([]tableCol, 0, n)
	for i := 0; i < left; i++ {
		out = append(out, tableCol{name: cols[i].Name(), col: cols[i]})
	}
	out = append(out, tableCol{name: "...", col: nil})
	for i := len(cols) - right; i < len(cols); i++ {
		out = append(out, tableCol{name: cols[i].Name(), col: cols[i]})
	}
	return out
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

func renderTable(cols []tableCol, widths []int, rowIdx []int) string {
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
		b.WriteString(pad(truncate(cols[j].name, widths[1+j]), widths[1+j]))
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

func cellString(col tableCol, row int) string {
	if col.col == nil {
		return "..."
	}
	if col.col.IsNull(row) {
		return "null"
	}
	s := col.col.ValueString(row)
	if s == "" {
		if _, ok := col.col.(*array.Utf8Column); ok {
			return "\"\""
		}
	}
	return s
}

func renderStats(cols []tableCol, nrows int, statsRows int, maxWidth int) string {
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
		if cols[i].col == nil {
			continue
		}
		c := cols[i].col
		dt := c.DType().String()
		st := statsForColumn(c, limit)
		line := "- " + c.Name() + ": " + dt + "  " + st
		if maxWidth > 0 {
			line = truncate(line, maxWidth)
		}
		b.WriteString(line)
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
