package grizzly

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const csvTypeSampleRows = 8192

type csvReadPlan struct {
	projection map[string]struct{}
	filterEven string
}

type typedBuilder interface {
	Append(raw string, row int) error
	Build(name string) Column
}

type genericBuilder[T any] struct {
	data      []T
	valid     bitmapBuilder
	nulls     map[string]struct{}
	parse     func(string) (T, error)
	construct func(name string, data []T, valid bitmap) Column
}

func (b *genericBuilder[T]) Append(raw string, row int) error {
	var zero T
	b.data = append(b.data, zero)
	if _, ok := b.nulls[raw]; ok {
		b.valid.Append(false)
		return nil
	}
	v, err := b.parse(raw)
	if err != nil {
		return fmt.Errorf("row %d parse value: %w", row, err)
	}
	b.data[len(b.data)-1] = v
	b.valid.Append(true)
	return nil
}

func (b *genericBuilder[T]) Build(name string) Column {
	return b.construct(name, b.data, b.valid.Build())
}

func readCSV(path string, opts ScanOptions, plan csvReadPlan) (*DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.ReuseRecord = true
	if opts.Delimiter != 0 {
		r.Comma = opts.Delimiter
	}

	header, err := r.Read()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("empty csv")
		}
		return nil, err
	}
	if len(header) == 0 {
		return nil, fmt.Errorf("empty csv header")
	}
	header = append([]string(nil), header...)

	nulls := make(map[string]struct{}, len(opts.NullValues))
	for i := range opts.NullValues {
		nulls[opts.NullValues[i]] = struct{}{}
	}

	headerIdx := make(map[string]int, len(header))
	for i := range header {
		headerIdx[header[i]] = i
	}

	included := make([]int, 0, len(header))
	includedNames := make([]string, 0, len(header))
	if len(plan.projection) == 0 {
		for i := range header {
			included = append(included, i)
			includedNames = append(includedNames, header[i])
		}
	} else {
		for i := range header {
			if _, ok := plan.projection[header[i]]; ok {
				included = append(included, i)
				includedNames = append(includedNames, header[i])
			}
		}
		if len(included) == 0 {
			return nil, fmt.Errorf("projection selected no columns")
		}
	}

	filterIdx := -1
	if plan.filterEven != "" {
		i, ok := headerIdx[plan.filterEven]
		if !ok {
			return nil, fmt.Errorf("unknown filter column %s", plan.filterEven)
		}
		filterIdx = i
	}

	samples := make([][]string, len(included))
	for i := range samples {
		samples[i] = make([]string, 0, csvTypeSampleRows)
	}
	records := make([][]string, 0, csvTypeSampleRows)
	for len(records) < csvTypeSampleRows {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) != len(header) {
			return nil, fmt.Errorf("csv row has %d columns expected %d", len(rec), len(header))
		}
		if filterIdx >= 0 && !rawEven(rec[filterIdx], nulls) {
			continue
		}
		projected := make([]string, len(included))
		for i, src := range included {
			projected[i] = rec[src]
			samples[i] = append(samples[i], rec[src])
		}
		records = append(records, projected)
	}

	builders := make([]typedBuilder, len(included))
	for i := range included {
		builders[i] = newBuilder(inferType(samples[i], nulls), nulls)
	}

	row := 1
	for i := range records {
		for j := range records[i] {
			if err := builders[j].Append(records[i][j], row); err != nil {
				return nil, err
			}
		}
		row++
	}

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) != len(header) {
			return nil, fmt.Errorf("csv row has %d columns expected %d", len(rec), len(header))
		}
		if filterIdx >= 0 && !rawEven(rec[filterIdx], nulls) {
			continue
		}
		for i, src := range included {
			if err := builders[i].Append(rec[src], row); err != nil {
				return nil, err
			}
		}
		row++
	}

	cols := make([]Column, len(included))
	for i := range cols {
		cols[i] = builders[i].Build(includedNames[i])
	}
	return NewDataFrame(cols...)
}

func newBuilder(dtype DType, nulls map[string]struct{}) typedBuilder {
	switch dtype {
	case DTypeInt64:
		return &genericBuilder[int64]{
			data:  make([]int64, 0, 16384),
			nulls: nulls,
			parse: func(raw string) (int64, error) {
				return strconv.ParseInt(raw, 10, 64)
			},
			construct: func(name string, data []int64, valid bitmap) Column {
				return newInt64ColumnOwned(name, data, valid)
			},
		}
	case DTypeFloat64:
		return &genericBuilder[float64]{
			data:  make([]float64, 0, 16384),
			nulls: nulls,
			parse: func(raw string) (float64, error) {
				return strconv.ParseFloat(raw, 64)
			},
			construct: func(name string, data []float64, valid bitmap) Column {
				return newFloat64ColumnOwned(name, data, valid)
			},
		}
	case DTypeBool:
		return &genericBuilder[bool]{
			data:  make([]bool, 0, 16384),
			nulls: nulls,
			parse: func(raw string) (bool, error) {
				return strconv.ParseBool(strings.ToLower(raw))
			},
			construct: func(name string, data []bool, valid bitmap) Column {
				return newBoolColumnOwned(name, data, valid)
			},
		}
	default:
		return &genericBuilder[string]{
			data:  make([]string, 0, 16384),
			nulls: nulls,
			parse: func(raw string) (string, error) {
				return raw, nil
			},
			construct: func(name string, data []string, valid bitmap) Column {
				return newUtf8ColumnOwned(name, data, valid)
			},
		}
	}
}

func inferType(values []string, nullSet map[string]struct{}) DType {
	allInt := true
	allFloat := true
	allBool := true
	for i := range values {
		if _, ok := nullSet[values[i]]; ok {
			continue
		}
		if _, err := strconv.ParseInt(values[i], 10, 64); err != nil {
			allInt = false
		}
		if _, err := strconv.ParseFloat(values[i], 64); err != nil {
			allFloat = false
		}
		if _, err := strconv.ParseBool(strings.ToLower(values[i])); err != nil {
			allBool = false
		}
		if !allInt && !allFloat && !allBool {
			return DTypeUtf8
		}
	}
	if allInt {
		return DTypeInt64
	}
	if allFloat {
		return DTypeFloat64
	}
	if allBool {
		return DTypeBool
	}
	return DTypeUtf8
}

func rawEven(raw string, nulls map[string]struct{}) bool {
	if _, ok := nulls[raw]; ok {
		return false
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return n%2 == 0
	}
	return len(raw)%2 == 0
}
