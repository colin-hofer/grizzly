# grizzly

`grizzly` is a clean-slate, performance-first dataframe engine for Go with a lazy query API inspired by polars.

## Core Design

- Typed columnar memory (`int64`, `float64`, `bool`, `utf8`) with validity bitmaps
- Generic column and builder internals to keep implementation compact without runtime interface overhead in hot loops
- Expression-based filtering (`Col("x").Gt(...)`, `Col("id").Even()`) instead of row callbacks
- Lazy query plans with optimization passes (filter reordering, CSV filter pushdown for supported predicates, projection pushdown when `Select` is present)
- Deterministic projection checksums for correctness verification
- CSV + JSON scanners as pluggable sources

## Performance Notes

- CSV ingestion uses single-pass typed builders after a bounded schema sample window
- CSV scan uses chunked parallel parsing after schema sampling
- `Filter` and `Take` use exact-size allocations to reduce GC pressure
- Sort uses type-specialized kernels and parallel stable merge-sort for large frames
- JSON serialization writes rows directly to an output buffer, avoiding map-heavy intermediate structures
- UTF-8 columns are stored as offset+byte buffers, reducing string-object overhead
- Expression literals are parsed once per kernel and evaluated with typed loops
- JSON record ingestion normalizes and sorts keys for deterministic column ordering

## Quick Example

```go
package main

import (
	"fmt"

	"grizzly"
)

func main() {
	df, err := grizzly.
		ScanCSV("data/customers-500000.csv", grizzly.ScanOptions{}).
		Filter(grizzly.Col("Index").Even()).
		Sort("Customer Id", false).
		Select("Index", "Customer Id", "Email").
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("rows", df.Height(), "checksum", df.ProjectionChecksum(3))
}
```

## Direction

This version intentionally drops legacy API compatibility to focus on a high-performance architecture and provide a strong foundation for:

- chunked columns
- parallel scan/filter/sort kernels
- expression fusion and predicate pushdown
- SIMD-oriented execution paths
