// Package partition maps ordering keys to a fixed number of partitions.
package partition

import "hash/fnv"

// Of returns hash(key) % n using FNV-1a. Commands with the same key always map
// to the same partition, which is what preserves per-instance ordering when a
// partition is consumed by a single worker at a time.
func Of(key string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}
