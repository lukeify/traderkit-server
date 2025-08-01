package ohlcv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"traderkit-server/utils"
)

type Ingestion struct {
	db       *pgxpool.Pool
	provider IngestionProvider
}

type IngestionProvider interface {
	BackfilledData(symbols []string, ingestFrom time.Time) (pgx.CopyFromSource, error)
}

func NewIngestor(db *pgxpool.Pool, provider IngestionProvider) *Ingestion {
	return &Ingestion{
		db:       db,
		provider: provider,
	}
}

func (oi *Ingestion) Backfill(symbols []string) {
	// Compute the most recent ingestion date, based off the most recent bar timestamp for the provided symbol.
	mostRecentIngestionTime, err := oi.earliestIngestion()
	if err != nil {
		fmt.Printf("Could not determine the most recent ingestion: %#v", err)
		os.Exit(1)
	}

	// If no most recent ingestion time was found (usually the database is empty), then we need to backfill from the
	// start of the specified retention period.
	if mostRecentIngestionTime.IsZero() {
		// Determine what date we must backfill from.
		n, err := strconv.Atoi(os.Getenv("RETENTION_PERIOD_DAYS"))
		if err != nil || n < 0 || n > 255 {
			n = 14
		}
		mostRecentIngestionTime = utils.LastRetainedDay(time.Now(), uint8(n))
	}

	iter, err := oi.provider.BackfilledData(symbols, mostRecentIngestionTime)
	if err != nil {
		fmt.Printf("Could not generate backfill iterator: %#v", err)
		os.Exit(1)
	}

	_, err = oi.db.CopyFrom(
		context.Background(),
		pgx.Identifier{"bars"},
		[]string{"s_id", "ts", "o", "h", "l", "c", "v", "txns"},
		iter,
	)
	if err != nil {
		panic(fmt.Sprintf("Error calling pgx.CopyFrom: %#v\n", err))
	}
}

// earliestIngestion gets the earliest bar timestamp for all symbols in the database. If no such timestamp exists,
// then a zero `time.Time` value is returned and `err` will be `nil`.
func (oi *Ingestion) earliestIngestion() (time.Time, error) {
	var ts time.Time
	err := oi.db.QueryRow(
		context.Background(),
		`SELECT MIN(max_ts) AS global_last_ingested FROM (
			SELECT MAX(ts) AS max_ts FROM bars GROUP BY s_id
		) as max_bar`,
	).Scan(&ts)
	if err != nil && errors.Is(err, pgx.ErrNoRows) {
		return ts, nil
	}
	return ts, err
}
