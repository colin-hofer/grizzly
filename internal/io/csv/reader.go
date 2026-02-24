package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strconv"
	"sync"

	"os"

	"grizzly/internal/array"
	"grizzly/internal/exec"
)

const typeSampleRows = 8192
const chunkRows = 4096

type ReadPlan struct {
	Projection map[string]struct{}
	FilterEven string
}

type NullMatcher struct {
	single     string
	smallVals  [4]string
	smallCount uint8
	set        map[string]struct{}
}

func NewNullMatcher(values []string) NullMatcher {
	if len(values) == 0 {
		return NullMatcher{single: ""}
	}
	if len(values) == 1 {
		return NullMatcher{single: values[0]}
	}
	if len(values) <= 4 {
		uniq := make([]string, 0, len(values))
		seen := make(map[string]struct{}, len(values))
		for i := range values {
			if _, ok := seen[values[i]]; ok {
				continue
			}
			seen[values[i]] = struct{}{}
			uniq = append(uniq, values[i])
		}
		if len(uniq) == 1 {
			return NullMatcher{single: uniq[0]}
		}
		m := NullMatcher{smallCount: uint8(len(uniq))}
		for i := range uniq {
			m.smallVals[i] = uniq[i]
		}
		return m
	}
	set := make(map[string]struct{}, len(values))
	for i := range values {
		set[values[i]] = struct{}{}
	}
	if len(set) == 1 {
		for k := range set {
			return NullMatcher{single: k}
		}
	}
	return NullMatcher{set: set}
}

func (m NullMatcher) IsNull(raw string) bool {
	if m.set != nil {
		_, ok := m.set[raw]
		return ok
	}
	switch m.smallCount {
	case 0:
		return raw == m.single
	case 1:
		return raw == m.smallVals[0]
	case 2:
		return raw == m.smallVals[0] || raw == m.smallVals[1]
	case 3:
		return raw == m.smallVals[0] || raw == m.smallVals[1] || raw == m.smallVals[2]
	default:
		return raw == m.smallVals[0] || raw == m.smallVals[1] || raw == m.smallVals[2] || raw == m.smallVals[3]
	}
}

type typedBuilder interface {
	Append(raw string, row int) error
	Build(name string) array.Column
	AppendBuilder(other typedBuilder) error
}

type reservableBuilder interface {
	Reserve(addRows int, addBytes int)
}

type genericBuilder[T any] struct {
	data      []T
	valid     array.BitmapBuilder
	nulls     NullMatcher
	parse     func(string) (T, error)
	construct func(name string, data []T, valid array.Bitmap) array.Column
}

func (b *genericBuilder[T]) Append(raw string, row int) error {
	var zero T
	b.data = append(b.data, zero)
	if b.nulls.IsNull(raw) {
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

func (b *genericBuilder[T]) Build(name string) array.Column {
	return b.construct(name, b.data, b.valid.Build())
}

func (b *genericBuilder[T]) AppendBuilder(other typedBuilder) error {
	o, ok := other.(*genericBuilder[T])
	if !ok {
		return fmt.Errorf("builder type mismatch")
	}
	b.data = append(b.data, o.data...)
	b.valid.AppendFrom(&o.valid)
	return nil
}

func (b *genericBuilder[T]) Reserve(addRows int, _ int) {
	if addRows <= 0 {
		return
	}
	b.data = slices.Grow(b.data, addRows)
	b.valid.Reserve(addRows)
}

type utf8Builder struct {
	offsets []int32
	bytes   []byte
	valid   array.BitmapBuilder
	nulls   NullMatcher
}

func (b *utf8Builder) Append(raw string, _ int) error {
	if b.nulls.IsNull(raw) {
		b.valid.Append(false)
		b.offsets = append(b.offsets, int32(len(b.bytes)))
		return nil
	}
	b.valid.Append(true)
	b.bytes = append(b.bytes, raw...)
	b.offsets = append(b.offsets, int32(len(b.bytes)))
	return nil
}

func (b *utf8Builder) Build(name string) array.Column {
	return array.NewUtf8ColumnOwned(name, b.offsets, b.bytes, b.valid.Build())
}

func (b *utf8Builder) AppendBuilder(other typedBuilder) error {
	o, ok := other.(*utf8Builder)
	if !ok {
		return fmt.Errorf("builder type mismatch")
	}
	base := int32(len(b.bytes))
	b.bytes = append(b.bytes, o.bytes...)
	for i := 1; i < len(o.offsets); i++ {
		b.offsets = append(b.offsets, o.offsets[i]+base)
	}
	b.valid.AppendFrom(&o.valid)
	return nil
}

func (b *utf8Builder) Reserve(addRows int, addBytes int) {
	if addRows > 0 {
		b.offsets = slices.Grow(b.offsets, addRows)
		b.valid.Reserve(addRows)
	}
	if addBytes > 0 {
		b.bytes = slices.Grow(b.bytes, addBytes)
	}
}

func Read(ctx context.Context, path string, delimiter rune, nullValues []string, plan ReadPlan) (*exec.DataFrame, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Avoid paying per-row cancellation checks when the context cannot be canceled
	// (e.g. context.Background()).
	cancellable := ctx.Done() != nil
	if cancellable {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.ReuseRecord = true
	if delimiter != 0 {
		r.Comma = delimiter
	}

	header, err := r.Read()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("empty csv")
		}
		return nil, err
	}
	if cancellable {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if len(header) == 0 {
		return nil, fmt.Errorf("empty csv header")
	}
	header = append([]string(nil), header...)

	nulls := NewNullMatcher(nullValues)

	headerIdx := make(map[string]int, len(header))
	for i := range header {
		headerIdx[header[i]] = i
	}

	included := make([]int, 0, len(header))
	includedNames := make([]string, 0, len(header))
	if len(plan.Projection) == 0 {
		for i := range header {
			included = append(included, i)
			includedNames = append(includedNames, header[i])
		}
	} else {
		for i := range header {
			if _, ok := plan.Projection[header[i]]; ok {
				included = append(included, i)
				includedNames = append(includedNames, header[i])
			}
		}
		if len(included) == 0 {
			return nil, fmt.Errorf("projection selected no columns")
		}
	}

	filterIdx := -1
	if plan.FilterEven != "" {
		i, ok := headerIdx[plan.FilterEven]
		if !ok {
			return nil, fmt.Errorf("unknown filter column %s", plan.FilterEven)
		}
		filterIdx = i
	}

	samples := make([][]string, len(included))
	for i := range samples {
		samples[i] = make([]string, 0, typeSampleRows)
	}
	seedValues := make([]string, 0, typeSampleRows*len(included))
	seedRows := 0
	const ctxCheckMask = 1024 - 1
	iter := 0
	for seedRows < typeSampleRows {
		iter++
		if cancellable && (iter&ctxCheckMask) == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
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
			raw := rec[src]
			samples[i] = append(samples[i], raw)
			seedValues = append(seedValues, raw)
		}
		seedRows++
	}

	dtypes := make([]array.DataType, len(included))
	for i := range included {
		dtypes[i] = inferType(samples[i], nulls)
	}

	reserveRows := seedRows + chunkRows
	builders := make([]typedBuilder, len(included))
	for i := range included {
		builders[i] = newBuilder(dtypes[i], nulls, reserveRows)
	}

	row := 1
	ncols := len(included)
	for i := 0; i < seedRows; i++ {
		if cancellable && (i&ctxCheckMask) == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		base := i * ncols
		for j := 0; j < ncols; j++ {
			if err := builders[j].Append(seedValues[base+j], row); err != nil {
				return nil, err
			}
		}
		row++
	}

	if err := parseRemainingParallel(ctx, r, header, included, filterIdx, nulls, dtypes, builders, row); err != nil {
		return nil, err
	}

	cols := make([]array.Column, len(included))
	for i := range cols {
		cols[i] = builders[i].Build(includedNames[i])
	}
	return exec.NewDataFrame(cols...)
}

type parseJob struct {
	index int
	rows  []string
	nrows int
}

type builderPools struct {
	intPool   sync.Pool
	floatPool sync.Pool
	boolPool  sync.Pool
	utf8Pool  sync.Pool
}

func newBuilderPools() *builderPools {
	p := &builderPools{}
	p.intPool.New = func() any {
		return &genericBuilder[int64]{
			parse: func(raw string) (int64, error) { return strconv.ParseInt(raw, 10, 64) },
			construct: func(name string, data []int64, valid array.Bitmap) array.Column {
				return array.NewInt64ColumnOwned(name, data, valid)
			},
		}
	}
	p.floatPool.New = func() any {
		return &genericBuilder[float64]{
			parse: func(raw string) (float64, error) { return strconv.ParseFloat(raw, 64) },
			construct: func(name string, data []float64, valid array.Bitmap) array.Column {
				return array.NewFloat64ColumnOwned(name, data, valid)
			},
		}
	}
	p.boolPool.New = func() any {
		return &genericBuilder[bool]{
			parse: parseBoolASCII,
			construct: func(name string, data []bool, valid array.Bitmap) array.Column {
				return array.NewBoolColumnOwned(name, data, valid)
			},
		}
	}
	p.utf8Pool.New = func() any {
		return &utf8Builder{}
	}
	return p
}

func (p *builderPools) acquire(dtype array.DataType, nulls NullMatcher, rowsCap int, bytesCap int) typedBuilder {
	if rowsCap < 16 {
		rowsCap = 16
	}
	switch dtype.Kind {
	case array.KindInt:
		b := p.intPool.Get().(*genericBuilder[int64])
		resetGenericBuilder(b, nulls, rowsCap)
		return b
	case array.KindFloat:
		b := p.floatPool.Get().(*genericBuilder[float64])
		resetGenericBuilder(b, nulls, rowsCap)
		return b
	case array.KindBool:
		b := p.boolPool.Get().(*genericBuilder[bool])
		resetGenericBuilder(b, nulls, rowsCap)
		return b
	default:
		b := p.utf8Pool.Get().(*utf8Builder)
		resetUTF8Builder(b, nulls, rowsCap, bytesCap)
		return b
	}
}

func (p *builderPools) release(dtype array.DataType, b typedBuilder) {
	switch dtype.Kind {
	case array.KindInt:
		if gb, ok := b.(*genericBuilder[int64]); ok {
			p.intPool.Put(gb)
		}
	case array.KindFloat:
		if gb, ok := b.(*genericBuilder[float64]); ok {
			p.floatPool.Put(gb)
		}
	case array.KindBool:
		if gb, ok := b.(*genericBuilder[bool]); ok {
			p.boolPool.Put(gb)
		}
	default:
		if ub, ok := b.(*utf8Builder); ok {
			p.utf8Pool.Put(ub)
		}
	}
}

func resetGenericBuilder[T any](b *genericBuilder[T], nulls NullMatcher, rowsCap int) {
	b.nulls = nulls
	if cap(b.data) < rowsCap {
		b.data = make([]T, 0, rowsCap)
	} else {
		b.data = b.data[:0]
	}
	b.valid = array.BitmapBuilder{}
}

func resetUTF8Builder(b *utf8Builder, nulls NullMatcher, rowsCap int, bytesCap int) {
	b.nulls = nulls
	b.valid = array.BitmapBuilder{}
	if cap(b.offsets) < rowsCap+1 {
		b.offsets = make([]int32, 1, rowsCap+1)
	} else {
		b.offsets = b.offsets[:1]
		b.offsets[0] = 0
	}
	if bytesCap < 256 {
		bytesCap = 256
	}
	if cap(b.bytes) < bytesCap {
		b.bytes = make([]byte, 0, bytesCap)
	} else {
		b.bytes = b.bytes[:0]
	}
}

func parseRemainingParallel(ctx context.Context, r *csv.Reader, header []string, included []int, filterIdx int, nulls NullMatcher, dtypes []array.DataType, builders []typedBuilder, startRow int) error {
	cancellable := ctx != nil && ctx.Done() != nil
	const ctxCheckMask = 1024 - 1
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		return parseRemainingSequential(ctx, r, header, included, filterIdx, nulls, builders, startRow)
	}

	chunkValues := make([]string, 0, chunkRows*len(included))
	chunkNRows := 0
	chunkIndex := 0
	maxBatchJobs := workers * 8
	if maxBatchJobs < 8 {
		maxBatchJobs = 8
	}
	batch := make([]parseJob, 0, maxBatchJobs)
	bpools := newBuilderPools()
	ncols := len(included)
	chunkPool := sync.Pool{New: func() any {
		return make([]string, 0, chunkRows*ncols)
	}}

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if cancellable {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		results, err := parseBatch(ctx, batch, dtypes, nulls, startRow, bpools)
		if err != nil {
			return err
		}
		reserveForMerge(builders, results)
		for _, res := range results {
			for j := range builders {
				if err := builders[j].AppendBuilder(res[j]); err != nil {
					return err
				}
				bpools.release(dtypes[j], res[j])
			}
		}
		for i := range batch {
			if cap(batch[i].rows) > chunkRows*ncols*4 {
				continue
			}
			chunkPool.Put(batch[i].rows[:0])
		}
		batch = batch[:0]
		return nil
	}

	flushChunk := func() {
		if chunkNRows == 0 {
			return
		}
		batch = append(batch, parseJob{index: chunkIndex, rows: chunkValues, nrows: chunkNRows})
		chunkIndex++
		chunkValues = chunkPool.Get().([]string)
		chunkValues = chunkValues[:0]
		chunkNRows = 0
	}
	iter := 0
	for {
		iter++
		if cancellable && (iter&ctxCheckMask) == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(rec) != len(header) {
			return fmt.Errorf("csv row has %d columns expected %d", len(rec), len(header))
		}
		if filterIdx >= 0 && !rawEven(rec[filterIdx], nulls) {
			continue
		}
		for _, src := range included {
			chunkValues = append(chunkValues, rec[src])
		}
		chunkNRows++
		if chunkNRows >= chunkRows {
			flushChunk()
			if len(batch) >= maxBatchJobs {
				if err := flushBatch(); err != nil {
					return err
				}
			}
		}
	}
	flushChunk()
	if err := flushBatch(); err != nil {
		return err
	}
	return nil
}

func parseBatch(ctx context.Context, batch []parseJob, dtypes []array.DataType, nulls NullMatcher, startRow int, bpools *builderPools) ([][]typedBuilder, error) {
	cancellable := ctx != nil && ctx.Done() != nil
	if cancellable {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	results := make([][]typedBuilder, len(batch))
	utf8Cols := make([]int, 0, len(dtypes))
	for i := range dtypes {
		if dtypes[i].Kind == array.KindUtf8 {
			utf8Cols = append(utf8Cols, i)
		}
	}
	errCh := make(chan error, len(batch))
	var wg sync.WaitGroup
	for i := range batch {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			const ctxCheckMask = 1024 - 1
			job := batch[i]
			local := make([]typedBuilder, len(dtypes))
			rowsCap := job.nrows
			utf8Bytes := make([]int, len(dtypes))
			if len(utf8Cols) > 0 {
				ncols := len(dtypes)
				for r := 0; r < job.nrows; r++ {
					base := r * ncols
					for _, c := range utf8Cols {
						utf8Bytes[c] += len(job.rows[base+c])
					}
				}
			}
			for j := range dtypes {
				local[j] = bpools.acquire(dtypes[j], nulls, rowsCap, utf8Bytes[j])
			}
			row := startRow + job.index*chunkRows
			ncols := len(dtypes)
			for r := 0; r < job.nrows; r++ {
				if cancellable && (r&ctxCheckMask) == 0 {
					if err := ctx.Err(); err != nil {
						errCh <- err
						return
					}
				}
				base := r * ncols
				for c := 0; c < ncols; c++ {
					if err := local[c].Append(job.rows[base+c], row+r); err != nil {
						errCh <- err
						return
					}
				}
			}
			results[i] = local
		}(i)
	}
	wg.Wait()
	close(errCh)
	if err, ok := <-errCh; ok {
		return nil, err
	}
	return results, nil
}

func reserveForMerge(dst []typedBuilder, batch [][]typedBuilder) {
	rows := make([]int, len(dst))
	bytes := make([]int, len(dst))
	for i := range batch {
		for j := range batch[i] {
			switch b := batch[i][j].(type) {
			case *utf8Builder:
				rows[j] += len(b.offsets) - 1
				bytes[j] += len(b.bytes)
			case *genericBuilder[int64]:
				rows[j] += len(b.data)
			case *genericBuilder[float64]:
				rows[j] += len(b.data)
			case *genericBuilder[bool]:
				rows[j] += len(b.data)
			}
		}
	}
	for j := range dst {
		if rb, ok := dst[j].(reservableBuilder); ok {
			rb.Reserve(rows[j], bytes[j])
		}
	}
}

func parseRemainingSequential(ctx context.Context, r *csv.Reader, header []string, included []int, filterIdx int, nulls NullMatcher, builders []typedBuilder, row int) error {
	cancellable := ctx != nil && ctx.Done() != nil
	const ctxCheckMask = 1024 - 1
	iter := 0
	for {
		iter++
		if cancellable && (iter&ctxCheckMask) == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(rec) != len(header) {
			return fmt.Errorf("csv row has %d columns expected %d", len(rec), len(header))
		}
		if filterIdx >= 0 && !rawEven(rec[filterIdx], nulls) {
			continue
		}
		for i, src := range included {
			if err := builders[i].Append(rec[src], row); err != nil {
				return err
			}
		}
		row++
	}
	return nil
}

func newBuilder(dtype array.DataType, nulls NullMatcher, rowsCap int) typedBuilder {
	if rowsCap < 16 {
		rowsCap = 16
	}
	switch dtype.Kind {
	case array.KindInt:
		if dtype.Bits != 64 {
			// For now, IO inference always maps ints to int64.
			// Wider type support will be introduced via explicit Cast and FromStructs.
		}
		return &genericBuilder[int64]{
			data:  make([]int64, 0, rowsCap),
			nulls: nulls,
			parse: func(raw string) (int64, error) { return strconv.ParseInt(raw, 10, 64) },
			construct: func(name string, data []int64, valid array.Bitmap) array.Column {
				return array.NewInt64ColumnOwned(name, data, valid)
			},
		}
	case array.KindFloat:
		return &genericBuilder[float64]{
			data:  make([]float64, 0, rowsCap),
			nulls: nulls,
			parse: func(raw string) (float64, error) { return strconv.ParseFloat(raw, 64) },
			construct: func(name string, data []float64, valid array.Bitmap) array.Column {
				return array.NewFloat64ColumnOwned(name, data, valid)
			},
		}
	case array.KindBool:
		return &genericBuilder[bool]{
			data:  make([]bool, 0, rowsCap),
			nulls: nulls,
			parse: parseBoolASCII,
			construct: func(name string, data []bool, valid array.Bitmap) array.Column {
				return array.NewBoolColumnOwned(name, data, valid)
			},
		}
	default:
		byteCap := rowsCap * 16
		if byteCap < 256 {
			byteCap = 256
		}
		return &utf8Builder{offsets: make([]int32, 1, rowsCap+1), bytes: make([]byte, 0, byteCap), nulls: nulls}
	}
}

func inferType(values []string, nulls NullMatcher) array.DataType {
	allInt := true
	allFloat := true
	allBool := true
	for i := range values {
		if nulls.IsNull(values[i]) {
			continue
		}
		if _, err := strconv.ParseInt(values[i], 10, 64); err != nil {
			allInt = false
		}
		if _, err := strconv.ParseFloat(values[i], 64); err != nil {
			allFloat = false
		}
		if _, err := parseBoolASCII(values[i]); err != nil {
			allBool = false
		}
		if !allInt && !allFloat && !allBool {
			return array.Utf8()
		}
	}
	if allInt {
		return array.Int(64)
	}
	if allFloat {
		return array.Float(64)
	}
	if allBool {
		return array.Bool()
	}
	return array.Utf8()
}

func rawEven(raw string, nulls NullMatcher) bool {
	if nulls.IsNull(raw) {
		return false
	}
	if even, ok := fastIntEven(raw); ok {
		return even
	}
	return len(raw)%2 == 0
}

func fastIntEven(raw string) (bool, bool) {
	if raw == "" {
		return false, false
	}
	i := 0
	if raw[0] == '-' || raw[0] == '+' {
		if len(raw) == 1 {
			return false, false
		}
		i = 1
	}
	for ; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return false, false
		}
	}
	last := raw[len(raw)-1]
	return (last-'0')%2 == 0, true
}

func parseBoolASCII(raw string) (bool, error) {
	if len(raw) == 4 {
		if (raw[0] == 't' || raw[0] == 'T') &&
			(raw[1] == 'r' || raw[1] == 'R') &&
			(raw[2] == 'u' || raw[2] == 'U') &&
			(raw[3] == 'e' || raw[3] == 'E') {
			return true, nil
		}
	}
	if len(raw) == 5 {
		if (raw[0] == 'f' || raw[0] == 'F') &&
			(raw[1] == 'a' || raw[1] == 'A') &&
			(raw[2] == 'l' || raw[2] == 'L') &&
			(raw[3] == 's' || raw[3] == 'S') &&
			(raw[4] == 'e' || raw[4] == 'E') {
			return false, nil
		}
	}
	return strconv.ParseBool(raw)
}
