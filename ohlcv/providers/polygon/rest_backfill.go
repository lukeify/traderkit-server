package polygon

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/polygon-io/client-go/rest/iter"
	"github.com/polygon-io/client-go/rest/models"
)

type restBackfill struct {
	parent          *backfillIterator
	tickerFetchIter *RestTickerFetchIterator
	aggFetchIter    *iter.Iter[models.Agg]
	ticker          string
	row             models.Agg
}

func (rb *restBackfill) Next() bool {
	// TODO: How do we check we're waiting until the tickerFetchIter has a value ready?
	if rb.tickerFetchIter != nil {
		// A list of aggregates for a current ticker has already been fetched, we can read directly from the iterator.
		if rb.aggFetchIter != nil && rb.aggFetchIter.Next() {
			rb.row = rb.aggFetchIter.Item()
			fmt.Printf("%#v\n", rb.row)
			return true
		}
		println("aggFetchIter is nil, or has no next value")
		err := rb.instantiateAggIterator()
		fmt.Printf("eror: %v\n", err)
		if err != nil {
			println("Returning false from rest_backfill.Next()")
			return false
		}
		return rb.Next()
	}
	println("false called")
	return false
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

func (rb *restBackfill) IsCold() bool {
	return rb.tickerFetchIter == nil
}

func (rb *restBackfill) WarmUp() {
	rb.tickerFetchIter = NewRestTickerFetchIterator(rb.parent.client)
	go func() {
		i := 0
		tickerIter := rb.parent.client.ListTickers(
			context.Background(),
			models.ListTickersParams{}.WithMarket(models.AssetStocks).WithLimit(1000),
		)
		for tickerIter.Next() {
			// TODO: Improve selection logic for tickers.
			i++
			if i == 10 {
				break
			}
			rb.tickerFetchIter.Push(tickerIter.Item().Ticker)
		}
		if tickerIter.Err() != nil {
			log.Fatal(tickerIter.Err())
		}
		rb.tickerFetchIter.Close()
		println("Done fetching tickers")
	}()
}

func (rb *restBackfill) instantiateAggIterator() error {
	// TODO: Figure out how to tell if we're out of tickers in the tickerFetchIter.
	// TODO: How do we check we're waiting until the tickerFetchIter has a value ready?
	// TODO: We could also fetch the next ticker from the REST API while we're ingesting the current ticker.
	return rb.tickerFetchIter.Read(context.Background(), func(ticker string) {
		println("instantiateAggIterator for ticker:", ticker)
		// Assign the ticker to a stateful field on the struct, because when `Values` is called, the `models.Aggs` `row`
		// property has the `Ticker` field set to an empty string, which may be a bug.
		rb.ticker = ticker
		rb.aggFetchIter = rb.parent.client.ListAggs(
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
