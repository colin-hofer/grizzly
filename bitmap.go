package grizzly

type bitmap struct {
	bits []uint64
}

type bitmapBuilder struct {
	bits []uint64
	n    int
}

func newBitmap(n int, defaultValid bool) bitmap {
	b := bitmap{bits: make([]uint64, (n+63)/64)}
	if defaultValid {
		for i := range b.bits {
			b.bits[i] = ^uint64(0)
		}
		if n%64 != 0 && len(b.bits) > 0 {
			b.bits[len(b.bits)-1] = (uint64(1) << uint(n%64)) - 1
		}
	}
	return b
}

func (b bitmap) get(i int) bool {
	return (b.bits[i/64]>>(uint(i%64)))&1 == 1
}

func (b bitmap) set(i int) {
	b.bits[i/64] |= uint64(1) << uint(i%64)
}

func newBitmapFromBools(mask []bool) bitmap {
	b := bitmap{bits: make([]uint64, (len(mask)+63)/64)}
	for i := range mask {
		if mask[i] {
			b.set(i)
		}
	}
	return b
}

func (b *bitmapBuilder) Append(valid bool) {
	if b.n%64 == 0 {
		b.bits = append(b.bits, 0)
	}
	if valid {
		b.bits[b.n/64] |= uint64(1) << uint(b.n%64)
	}
	b.n++
}

func (b *bitmapBuilder) Build() bitmap {
	return bitmap{bits: b.bits}
}

func (b *bitmapBuilder) Len() int {
	return b.n
}

func (b *bitmapBuilder) Get(i int) bool {
	return (b.bits[i/64]>>(uint(i%64)))&1 == 1
}

func (b *bitmapBuilder) AppendFrom(other *bitmapBuilder) {
	if other.n == 0 {
		return
	}
	if b.n%64 == 0 {
		full := other.n / 64
		if full > 0 {
			b.bits = append(b.bits, other.bits[:full]...)
			b.n += full * 64
		}
		for i := full * 64; i < other.n; i++ {
			b.Append(other.Get(i))
		}
		return
	}
	for i := 0; i < other.n; i++ {
		b.Append(other.Get(i))
	}
}

func (b *bitmapBuilder) Reserve(additionalBits int) {
	if additionalBits <= 0 {
		return
	}
	needBits := b.n + additionalBits
	needWords := (needBits + 63) / 64
	if cap(b.bits) >= needWords {
		return
	}
	next := make([]uint64, len(b.bits), needWords)
	copy(next, b.bits)
	b.bits = next
}
