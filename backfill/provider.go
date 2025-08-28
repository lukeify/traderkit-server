package backfill

import (
	"time"

	"github.com/jackc/pgx/v5"
)

type Provider interface {
	Backfill(backfillFrom time.Time, predicate func(string) bool) (pgx.CopyFromSource, error)
	SetMetrics(metrics *Metrics)
}
