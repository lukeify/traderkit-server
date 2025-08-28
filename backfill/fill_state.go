package backfill

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"time"
	"traderkit-server/utils"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// fillState provides a representation of the fill state of the `bars` table. `FilledBefore` represents the latest
// timestamp where all bars before have been definitely filled, or if:
//
// 1. No bars exist in the table,
// 2. The `FilledBefore` field resolves to a time before the retention period,
//
// `FilledBefore `ill be set to the start of the retention period for the table. `UnfilledAfter` represents the latest
// timestamp where some filling has taken place. Inserts after this timestamp can be done using `COPY FROM` for
// additional performance. `UnfilledAfter` is `nil` if no bars exist in the table.
type fillState struct {
	FilledBefore  *time.Time
	UnfilledAfter *time.Time
}

// backfillFrom takes a `pgxpool.Pool` and queries `the` bars table for the earliest and latest timestamps that
// represent respectively:
//
// 1. The timestamp where bars before it have been definitely filled (and definitely exist), and
// 2. Bars after the second timestamp that have never been filled (and do not exist yet).
//
// If a `pgx.ErrNoRows` is returned, then `FilledBefore`, will be set to the start of the retention period and
// `UnfilledAfter` is set as `nil`.
func (fs *fillState) backfillFrom(db *pgxpool.Pool) time.Time {
	lastRetained := fs.defaultLastRetainedTimestamp()

	rows, _ := db.Query(
		context.Background(),
		`SELECT MIN(max_ts) AS earliest, MAX(max_ts) AS latest FROM (
			SELECT MAX(ts) AS max_ts FROM bars GROUP BY s_id
		) as max_bar`,
	)

	row, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[fillState])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			fs.FilledBefore = &lastRetained
		} else {
			log.Fatalf("Error collecting fill state: %v\n", err)
		}
	}
	*fs = row

	// If no filled range is present (i.e. `fs.FilledBefore` is `nil`), or the `FilledBefore` timestamp is before
	// the `lastRetained` then the database is completely empty and backfilling shall start from the specified retention
	// period. This is enforced by setting `FilledBefore` to the `lastRetained` timestamp.
	if fs.FilledBefore == nil || fs.FilledBefore.Before(lastRetained) {
		fs.FilledBefore = &lastRetained
	}

	// If the UnfilledAfter timestamp is also before `lastRetained`, it should be bounded to `lastRetained` as well.
	if fs.UnfilledAfter != nil && fs.UnfilledAfter.Before(lastRetained) {
		fs.UnfilledAfter = &lastRetained
	}

	return *fs.FilledBefore
}

// mayBeFilled will return `true` if the given `time.Time` is between `FilledBefore` and `UnfilledAfter` inclusive, but
// only if `UnfilledAfter` is not `nil`.
func (fs *fillState) mayBeFilled(t time.Time) bool {
	return t.Compare(*fs.FilledBefore) >= 0 && fs.UnfilledAfter != nil && t.Compare(*fs.UnfilledAfter) <= 0
}

// defaultLastRetainedTimestamp will return either the last retained day as specified by the `RETENTION_PERIOD_DAYS`
// environment variable, or a default of 14 days if the variable is not set or invalid.
func (fs *fillState) defaultLastRetainedTimestamp() time.Time {
	// Determine what date we must backfill from.
	n, err := strconv.Atoi(os.Getenv("RETENTION_PERIOD_DAYS"))
	if err != nil || n < 0 || n > 255 {
		n = 14
	}
	return utils.LastRetainedDay(time.Now(), uint8(n))
}
