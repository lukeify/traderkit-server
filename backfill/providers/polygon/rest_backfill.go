package polygon

import (
	"time"
)

type restBackfill struct {
	parent       *backfillIterator
	tickerSource tickerSource
	aggPool      *aggregatePool
	entry        *aggregateEntry
}

// startup begins two goroutines:
// 1. Calls `tickerSource.accumulate` which will retrieve the tickers and insert them into a channel to retrieve
// aggregate data for. This accumulation process can be customised depending on what struct that conforms to the
// `tickerSource` interface is used. For example, the default uses a `restTickerSource`.
// 2. Calls `aggPool.startWorkers` which retrieves aggregates for the accumulated tickers and pushes them to a channel
// to then be popped off for insertion into the database.
// TODO: `tickerSource` and `aggregatePool` could possibly be merged into an `AggregateSource`.
func (rb *restBackfill) startup() {
	go rb.tickerSource.accumulate(rb.parent.predicate)
	go rb.aggPool.startWorkers(rb.parent.backfillFrom, rb.parent.backfillTo)
}

func (rb *restBackfill) Next() bool {
	// Pluck from the aggregates channel.
	entry, ok := <-rb.aggPool.aggregates
	if !ok {
		return false
	}
	rb.entry = entry
	return true
}

// Values takes both the `ticker` value from the struct and the current `row` (of type `models.Agg`) and returns a
// slice that can be inserted into the database.
func (rb *restBackfill) Values() ([]any, error) {
	sId := rb.entry.Ticker
	rb.parent.metrics.IngestRow(sId)

	return []any{
		sId,
		time.Time(rb.entry.Agg.Timestamp),
		rb.entry.Agg.Open,
		rb.entry.Agg.High,
		rb.entry.Agg.Low,
		rb.entry.Agg.Close,
		rb.entry.Agg.Volume,
		rb.entry.Agg.Transactions,
	}, nil
}
