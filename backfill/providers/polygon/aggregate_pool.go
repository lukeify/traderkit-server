package polygon

import (
	"context"
	"sync"
	"time"

	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
)

// aggregateEntry contains a single aggregate data point. This is necessary because the `Ticker` field of `models.Agg`
// is not populated by the Polygon API (this is likely a bug).
type aggregateEntry struct {
	Ticker string
	Agg    models.Agg
}

type aggregatePool struct {
	client     *polygon.Client
	source     tickerSource
	aggregates chan *aggregateEntry
}

func newAggregatePool(client *polygon.Client, source tickerSource, bufferSize int) *aggregatePool {
	return &aggregatePool{
		client:     client,
		source:     source,
		aggregates: make(chan *aggregateEntry, bufferSize),
	}
}

func (pool *aggregatePool) startWorkers(from time.Time, to time.Time) {
	// TODO: Determine most optimal goroutine worker count
	workerCount := 16

	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for ticker := range pool.source.getChannel() {
				aggIter := pool.client.ListAggs(
					context.Background(),
					&models.ListAggsParams{
						Ticker:     ticker,
						Multiplier: 1,
						Timespan:   models.Minute,
						From:       models.Millis(from),
						To:         models.Millis(to),
					},
				)

				for aggIter.Next() {
					pool.aggregates <- &aggregateEntry{
						Ticker: ticker,
						Agg:    aggIter.Item(),
					}
				}
			}
		}()
	}

	wg.Wait()
	close(pool.aggregates)
}
