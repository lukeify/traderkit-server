package ohlcv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"traderkit-server/utils"
)

type Ingestion struct {
	db       *pgx.Conn
	provider IngestionProvider
}

type IngestionProvider interface {
	BackfilledData(symbols []string, ingestFrom time.Time) (pgx.CopyFromSource, error)
}

func NewIngestor(db *pgx.Conn, provider IngestionProvider) *Ingestion {
	return &Ingestion{
		db:       db,
		provider: provider,
	}
}

func (oi *Ingestion) Backfill(symbols []string) {
	// Compute the most recent ingestion date, based off the most recent bar timestamp for the provided symbol.
	mostRecentIngestionTime, err := oi.mostRecentIngestion(symbols[0])
	if err != nil {
		fmt.Printf("Could not determine the most recent ingestion: %#v", err)
		os.Exit(1)
	}

	// If no most recent ingestion time was found (usually the database is empty), then we need to backfill from the
	// start of the specified retention period.
	if mostRecentIngestionTime.IsZero() {
		// Determine what date we must backfill from.
		n, err := strconv.Atoi(os.Getenv("RETENTION_PERIOD_DAYS"))
		if err != nil {
			n = 14
		}
		mostRecentIngestionTime = utils.LastRetainedDay(time.Now(), n)
	}

	iter, err := oi.provider.BackfilledData(symbols, mostRecentIngestionTime)
	if err != nil {
		fmt.Printf("Could not generate backfill iterator: %#v", err)
		os.Exit(1)
	}

	_, err = oi.db.CopyFrom(
		context.Background(),
		pgx.Identifier{"bars"},
		[]string{"symbol_id", "ts", "o", "h", "l", "c", "v", "txns"},
		iter,
	)
	if err != nil {
		panic("Error calling pgx.CopyFrom")
	}
}

// mostRecentIngestion gets the most recent bar timestamp for the provided symbol and returns it. If no such bar is
// found, then a zero `time.Time` value is returned and `err` will be `nil`.
func (oi *Ingestion) mostRecentIngestion(symbol string) (time.Time, error) {
	var ts time.Time
	err := oi.db.QueryRow(
		context.Background(),
		"SELECT ts FROM bars WHERE symbol_id = $1 ORDER BY ts DESC LIMIT 1",
		symbol,
	).Scan(&ts)
	if err != nil && errors.Is(err, pgx.ErrNoRows) {
		return ts, nil
	}
	return ts, err
}
