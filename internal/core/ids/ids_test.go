package ids

import (
	"strings"
	"testing"
	"time"
)

func TestNewIsPrefixedUniqueAndSortable(t *testing.T) {
	a := NewAt("inst", time.UnixMilli(1_000_000))
	b := NewAt("inst", time.UnixMilli(1_000_001))
	if !strings.HasPrefix(a, "inst_") || len(a) != len("inst_")+26 {
		t.Fatalf("bad shape %q", a)
	}
	if !(a < b) {
		t.Fatalf("ids must sort by time: %q !< %q", a, b)
	}
	seen := map[string]bool{}
	for i := 0; i < 5000; i++ {
		id := New("x")
		if seen[id] {
			t.Fatal("duplicate id")
		}
		seen[id] = true
	}
}
