package polygon

import (
	"context"
	"sync"
	"time"

	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
)

type AggregateEntry struct {
	Ticker string
	Agg    models.Agg
}

type AggregatePool struct {
	client     *polygon.Client
	source     TickerSource
	aggregates chan *AggregateEntry
}

func NewAggregatePool(client *polygon.Client, source TickerSource, bufferSize int) *AggregatePool {
	return &AggregatePool{
		client:     client,
		source:     source,
		aggregates: make(chan *AggregateEntry, bufferSize),
	}
}

func (pool *AggregatePool) StartWorkers(from time.Time, to time.Time) {
	// TODO: Determine most optimal goroutine worker count
	workerCount := 16

	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for ticker := range pool.source.Channel() {
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
					pool.aggregates <- &AggregateEntry{
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
