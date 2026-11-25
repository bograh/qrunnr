// Package migrations embeds the Postgres schema migrations.
package migrations

import "embed"

// FS holds the ordered *.sql migration files.
//
//go:embed *.sql
var FS embed.FS
