package polygon

import (
	"context"
	"fmt"
	"log"

	"github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/iter"
	"github.com/polygon-io/client-go/rest/models"
)

type TickerSource interface {
	Accumulate()
	Read(ctx context.Context, fn func(string)) error
}

type RestTickerSource struct {
	client  *polygon.Client
	iter    *iter.Iter[models.Ticker]
	channel chan string
}

func NewRestTickerSource(client *polygon.Client) TickerSource {
	// TODO: Is this buffer size appropriate? Currently it holds all U.S. equity tickers with space to spare.
	return &RestTickerSource{
		client:  client,
		channel: make(chan string, 20000),
		iter: client.ListTickers(
			context.Background(),
			models.ListTickersParams{}.WithMarket(models.AssetStocks).WithLimit(1000),
		),
	}
}

func (rts *RestTickerSource) Accumulate() {
	for rts.iter.Next() {
		// TODO: Improve selection logic for tickers when we only want to select from a certain number of tickers.
		rts.channel <- rts.iter.Item().Ticker
	}
	if rts.iter.Err() != nil {
		log.Fatal(rts.iter.Err())
	}
	close(rts.channel)
}

// Read uses the functionality of `select` (https://go.dev/ref/spec#Select_statements) to wait until the `tfi.tickers`
// channel has data available to read from (this is when the ticker fetchign retri
func (rts *RestTickerSource) Read(ctx context.Context, fn func(string)) error {
	// TODO: This used to have a `for` loop around it? Maybe it's not needed.
	select {
	case ticker, ok := <-rts.channel:
		if !ok {
			// TODO: Describe under what scenario this would happen.
			// This happens when the channel is closed.
			return fmt.Errorf("tfi.tickers not ok")
		}
		fn(ticker)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("ctx.Done")
	}
}

type MapTickerSource struct {
	tickers map[string]struct{}
}

func (mts *MapTickerSource) Accumulate() {
	// No-op for MapTickerSource, as tickers are already provided in the map.
}

func (mts *MapTickerSource) Read(_ context.Context, fn func(string)) error {
	if len(mts.tickers) == 0 {
		return fmt.Errorf("no tickers available in MapTickerSource")
	}

	for k := range mts.tickers {
		fn(k)
		delete(mts.tickers, k)
		break
	}

	return nil
}
