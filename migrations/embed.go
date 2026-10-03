// Package migrations embeds the SQL schema migrations.
package migrations

import "embed"

// FS holds the *.sql files, applied in lexical order.
//
//go:embed *.sql
var FS embed.FS
