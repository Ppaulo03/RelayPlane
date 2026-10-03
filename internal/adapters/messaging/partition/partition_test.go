package partition

import (
	"fmt"
	"testing"
)

func TestOfIsStableAndBounded(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("inst_%d", i)
		p := Of(k, 32)
		if p < 0 || p >= 32 {
			t.Fatalf("partition %d out of range", p)
		}
		if Of(k, 32) != p {
			t.Fatal("partition must be deterministic")
		}
	}
	if Of("x", 1) != 0 || Of("x", 0) != 0 {
		t.Fatal("degenerate partition counts map to 0")
	}
}

func TestOfSpreadsKeys(t *testing.T) {
	counts := make([]int, 16)
	for i := 0; i < 16000; i++ {
		counts[Of(fmt.Sprintf("inst_%d", i), 16)]++
	}
	for p, c := range counts {
		if c < 700 || c > 1300 {
			t.Errorf("partition %d badly skewed: %d", p, c)
		}
	}
}
