package polygon

import (
	"context"
	"fmt"
	"time"

	"github.com/polygon-io/client-go/rest/iter"
	"github.com/polygon-io/client-go/rest/models"
)

type restBackfill struct {
	parent       *backfillIterator
	tickerSource TickerSource
	aggIter      *iter.Iter[models.Agg]
	ticker       string
	row          models.Agg
}

func (rb *restBackfill) Next() bool {
	// A list of aggregates for a current ticker has already been fetched, we can read directly from the iterator.
	if rb.aggIter != nil && rb.aggIter.Next() {
		rb.row = rb.aggIter.Item()
		return true
	}
	err := rb.instantiateAggIterator()
	fmt.Printf("error: %v\n", err)
	if err != nil {
		println("Returning false from restBackfill.Next()")
		return false
	}
	return rb.Next()
}

// Values takes both the `ticker` value from the struct and the current `row` (of type `models.Agg`) and returns a
// slice that can be inserted into the database.
func (rb *restBackfill) Values() ([]any, error) {
	sId := rb.ticker
	rb.parent.metrics.IngestRow(sId)

	return []any{
		sId,
		time.Time(rb.row.Timestamp),
		rb.row.Open,
		rb.row.High,
		rb.row.Low,
		rb.row.Close,
		rb.row.Volume,
		rb.row.Transactions,
	}, nil
}

func (rb *restBackfill) instantiateAggIterator() error {
	// TODO: Figure out how to tell if we're out of tickers in the tickerIter.
	// TODO: We could also fetch the next ticker from the REST API while we're ingesting the current ticker.
	return rb.tickerSource.Read(context.Background(), func(ticker string) {
		println("instantiateAggIterator for ticker:", ticker)
		// Assign the ticker to a stateful field on the struct, because when `Values` is called, the `models.Aggs` `row`
		// property has the `Ticker` field set to an empty string, which may be a bug.
		rb.ticker = ticker
		rb.aggIter = rb.parent.client.ListAggs(
			context.Background(),
			&models.ListAggsParams{
				Ticker:     ticker,
				Multiplier: 1,
				Timespan:   models.Minute,
				From:       models.Millis(rb.parent.ingestFrom),
				To:         models.Millis(time.Now()),
			},
		)
	})
}
