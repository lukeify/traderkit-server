package backfill

import (
	"time"

	"github.com/jackc/pgx/v5"
)

type Provider interface {
	Backfill(from time.Time, to time.Time, predicate func(string) bool) (pgx.CopyFromSource, error)
	SetMetrics(metrics *Metrics)
}
