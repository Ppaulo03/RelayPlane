package postgres

import (
	"sort"
	"strings"

	"github.com/relayplane/relayplane/internal/core/subscription"
)

// prefixCols turns "a,b,c" into "x.a,x.b,x.c" for statements that join several tables.
func prefixCols(alias, cols string) string {
	parts := strings.Split(strings.ReplaceAll(cols, "\n", ""), ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ",")
}

func sortDeliveries(ds []subscription.Delivery) {
	sort.Slice(ds, func(i, j int) bool {
		if !ds[i].CreatedAt.Equal(ds[j].CreatedAt) {
			return ds[i].CreatedAt.Before(ds[j].CreatedAt)
		}
		return ds[i].ID < ds[j].ID
	})
}
