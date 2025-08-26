package polygon

import (
	"time"
)

type restBackfill struct {
	parent       *backfillIterator
	tickerSource TickerSource
	aggPool      *AggregatePool
	entry        *AggregateEntry
}

// Startup begins two goroutines:
// 1. Calls `tickerSource.Accumulate` which will retrieve the tickers and insert them into a channel to retrieve
// aggregate data for. This accumulation process can be customised depending on what struct that conforms to the
// `TickerSource` interface is used. For example, the default uses a `RestTickerSource`.
// 2. Calls `aggPool.StartWorkers` which retrieves aggregates for the accumulated tickers and pushes them to a channel
// to then be popped off for insertion into the database.
// TODO: `TickerSource` and `AggregatePool` could possibly be merged into an `AggregateSource`.
func (rb *restBackfill) Startup() {
	go rb.tickerSource.Accumulate(rb.parent.predicate)
	go rb.aggPool.StartWorkers(rb.parent.ingestFrom, time.Now())
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

// TODO: Figure out how to tell if we're out of tickers in the tickerIter.
// TODO: We could also fetch the next ticker from the REST API while we're ingesting the current ticker.
