package ohlcv

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"traderkit-server/utils/progress_printer"
)

type Ingestion struct {
	db       *pgxpool.Pool
	metrics  *Metrics
	provider IngestionProvider
}

// TODO: Optionally provide the ability to backfill only on specific symbols.

type IngestionProvider interface {
	Backfill(ingestFrom time.Time) (pgx.CopyFromSource, error)
	SetMetrics(metrics *Metrics)
}

func NewIngestor(db *pgxpool.Pool, provider IngestionProvider) *Ingestion {
	m := Metrics{}
	provider.SetMetrics(&m)
	return &Ingestion{
		db:       db,
		metrics:  &m,
		provider: provider,
	}
}

// Backfill ingests bar data using the provided `IngestionProvider` as a source of data until the database is up to
// date with a full set of OHLCV bar data for all symbols.
//
// The data is routed into the database using either `COPY FROM` or `UPSERT` ergonomics, depending on whether the bar
// falls within a range of timestamps that may have already been ingested into the database. The former will be used
// when it is known that no data exists, while the latter will be used when it is known that some data may already
// exist in the database, and an `ON CONFLICT` clause is necessary.
//
// If the database is entirely empty, then `partiallyFilledRange` will return a struct with no time bounds, and
// backfilling will begin from the start of the defined retention period using `COPY FROM`. If the struct contains a
// valid range, then the backfill will begin from the starting bound of the range, using `UPSERT` ergonomics, and then
// `COPY FROM` following the end of the range.
func (oi *Ingestion) Backfill() error {
	pp := progress_printer.NewProgressPrinter(os.Stdout)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		pp.Complete("Done")
	}()
	oi.metrics.StartPrinting(ctx, pp)

	// Compute the fill state of the database, and when to begin ingesting data from for backfilling.
	fs := FillState{}
	iter, err := oi.provider.Backfill(fs.IngestFrom(oi.db))
	if err != nil {
		return err
	}

	copyFromCh := make(chan []any, 1000)
	upsertCh := make(chan []any, 1000)
	errCh := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(3)

	// Router goroutine. This reads from the iterator, and routes values to the appropriate insertion channel based on
	// whether the bar falls within the range of timestamps where we already have data.
	go func() {
		upsertCount := 0
		copyFromCount := 0

		defer wg.Done()
		defer close(copyFromCh)
		defer close(upsertCh)

		// If the bar timestamp is within the range of already ingested data (inclusive), then an upsert is
		// required because it cannot be guaranteed that the bar is not already in the database. A type assertion
		// is used to convert the `string` time to a `time.Time` instance.
		// @see https://go.dev/tour/methods/15
		for iter.Next() {
			values, err := iter.Values()
			if err != nil {
				errCh <- err
			}

			if fs.MayBeFilled(values[1].(time.Time)) {
				upsertCount++
				upsertCh <- values
			} else {
				copyFromCount++
				copyFromCh <- values
			}
		}
	}()

	go func() {
		defer wg.Done()
		err := oi.processViaCopyFrom(copyFromCh)
		if err != nil {
			errCh <- fmt.Errorf("could not process via COPY FROM: %#v", err)
		}

	}()

	go func() {
		defer wg.Done()
		err := oi.processViaUpsert(upsertCh)
		if err != nil {
			errCh <- fmt.Errorf("could not process via INSERT: %#v", err)
		}
	}()

	wg.Wait()

	// Check if the error channel has accumulated any errors. If there is an error, return it to the calling function.
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (oi *Ingestion) processViaCopyFrom(dataCh <-chan []any) error {
	// TODO: Document the `channelCopyFromSourceIter` struct, and print the number of rows copied.
	_, err := oi.db.CopyFrom(
		context.Background(),
		pgx.Identifier{"bars"},
		[]string{"s_id", "ts", "o", "h", "l", "c", "v", "txns"},
		&channelCopyFromSourceIter{dataCh: dataCh},
	)
	return err
}

// processViaUpsert processes bars within the range of timestamps where data may already have been ingested, and thus
// ON CONFLICT handling is necessary.
func (oi *Ingestion) processViaUpsert(dataCh <-chan []any) error {
	const batchSize = 1000
	batch := make([][]any, 0, batchSize)

	for {
		values, ok := <-dataCh
		// The channel is closed, perform the final insertion
		if !ok {
			if len(batch) > 0 {
				err := oi.executeUpsert(batch)
				return err
			}
			// TODO: It's assumed there is no error here.
			return nil
		}

		batch = append(batch, values)
		// The batch is now larger than the batch size, perform an insertion and flush the batch.
		if len(batch) >= batchSize {
			err := oi.executeUpsert(batch)
			if err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
}

// executeUpsert performs a `INSERT INTO ... ON CONFLICT` query for rows that either might need to be updated or cannot
// be guaranteed to not exist (`COPY FROM` requires rows to not exist in the database).
func (oi *Ingestion) executeUpsert(rows [][]any) error {
	if len(rows) == 0 {
		// TODO: Should having no rows to upsert be considered an error?
		return nil
	}

	// Track all parameters to be inserted in the query. There are 8 parameters per row, so the capacity of the slice
	// should be the number of rows multiplied by 8.
	params := make([]any, 0, len(rows)*8)

	var sb strings.Builder
	sb.WriteString(`INSERT INTO bars (s_id, ts, o, h, l, c, v, txns) VALUES `)

	for i, row := range rows {
		if i > 0 {
			sb.WriteString(`, `)
		}
		sb.WriteString(`(`)
		for j := 0; j < len(row); j++ {
			if j > 0 {
				sb.WriteString(`, `)
			}
			sb.WriteString(fmt.Sprintf("$%d", i*8+j+1))
			params = append(params, row[j])
		}
		sb.WriteString(`)`)
	}
	sb.WriteString(` ON CONFLICT (s_id, ts) DO UPDATE SET o = EXCLUDED.o, h = EXCLUDED.h, l = EXCLUDED.l, c = EXCLUDED.c, v = EXCLUDED.v, txns = EXCLUDED.txns`)

	// TODO: Capture newly inserted rows, versus conflicted rows.
	_, err := oi.db.Exec(context.Background(), sb.String(), params...)
	return err
}

type channelCopyFromSourceIter struct {
	dataCh <-chan []any
	values []any
	err    error
}

func (c *channelCopyFromSourceIter) Next() bool {
	values, ok := <-c.dataCh
	if !ok {
		return false
	}
	c.values = values
	return true
}

func (c *channelCopyFromSourceIter) Values() ([]any, error) {
	return c.values, nil
}

func (c *channelCopyFromSourceIter) Err() error {
	return c.err
}
