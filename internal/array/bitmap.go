package array

type Bitmap struct {
	bits []uint64
}

func NewBitmap(n int, defaultValid bool) Bitmap {
	b := Bitmap{bits: make([]uint64, (n+63)/64)}
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

func NewBitmapFromBools(mask []bool) Bitmap {
	b := Bitmap{bits: make([]uint64, (len(mask)+63)/64)}
	for i := range mask {
		if mask[i] {
			b.Set(i)
		}
	}
	return b
}

func (b Bitmap) Get(i int) bool {
	return (b.bits[i/64]>>(uint(i%64)))&1 == 1
}

func (b Bitmap) Set(i int) {
	b.bits[i/64] |= uint64(1) << uint(i%64)
}

type BitmapBuilder struct {
	bits []uint64
	n    int
}

func (b *BitmapBuilder) Append(valid bool) {
	if b.n%64 == 0 {
		b.bits = append(b.bits, 0)
	}
	if valid {
		b.bits[b.n/64] |= uint64(1) << uint(b.n%64)
	}
	b.n++
}

func (b *BitmapBuilder) Len() int { return b.n }

func (b *BitmapBuilder) Get(i int) bool {
	return (b.bits[i/64]>>(uint(i%64)))&1 == 1
}

func (b *BitmapBuilder) Reserve(additionalBits int) {
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

func (b *BitmapBuilder) AppendFrom(other *BitmapBuilder) {
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

func (b *BitmapBuilder) Build() Bitmap {
	return Bitmap{bits: b.bits}
}
