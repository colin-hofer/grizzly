package csv

import (
	"strconv"
	"testing"
)

func FuzzFastIntEven(f *testing.F) {
	seeds := []string{"0", "1", "2", "10", "-2", "+42", "999", "-999", "", "01", "1a"}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		even, ok := fastIntEven(s)
		if !ok {
			return
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			// fastIntEven only returns ok for digit-only (with optional sign) strings.
			t.Fatalf("fastIntEven ok but ParseInt failed for %q", s)
		}
		if even != (n%2 == 0) {
			t.Fatalf("parity mismatch for %q", s)
		}
	})
}
