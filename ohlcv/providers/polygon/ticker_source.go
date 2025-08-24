package polygon

import (
	"context"
	"log"

	"github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/iter"
	"github.com/polygon-io/client-go/rest/models"
)

// TODO: Accumulate should take a filter function to filter out unwanted tickers.

type TickerSource interface {
	Accumulate()
	Channel() chan string
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
		ticker := rts.iter.Item().Ticker
		// TODO: implement filtering
		rts.channel <- ticker
	}
	if rts.iter.Err() != nil {
		log.Fatal(rts.iter.Err())
	}
	close(rts.channel)
}

func (rts *RestTickerSource) Channel() chan string {
	return rts.channel
}

type MapTickerSource struct {
	tickers map[string]struct{}
	channel chan string
}

func (mts *MapTickerSource) Accumulate() {
	for k, _ := range mts.tickers {
		mts.channel <- k
	}
}

func (mts *MapTickerSource) Channel() chan string {
	return mts.channel
}
