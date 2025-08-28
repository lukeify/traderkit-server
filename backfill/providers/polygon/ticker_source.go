package polygon

import (
	"context"
	"log"

	"github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/iter"
	"github.com/polygon-io/client-go/rest/models"
)

type tickerSource interface {
	accumulate(predicate func(string) bool)
	getChannel() chan string
}

type restTickerSource struct {
	client  *polygon.Client
	iter    *iter.Iter[models.Ticker]
	channel chan string
}

func newRestTickerSource(client *polygon.Client) tickerSource {
	// TODO: Is this buffer size appropriate? Currently it holds all U.S. equity tickers with space to spare.
	return &restTickerSource{
		client:  client,
		channel: make(chan string, 20000),
		iter: client.ListTickers(
			context.Background(),
			models.ListTickersParams{}.WithMarket(models.AssetStocks).WithLimit(1000),
		),
	}
}

func (rts *restTickerSource) accumulate(predicate func(string) bool) {
	for rts.iter.Next() {
		ticker := rts.iter.Item().Ticker
		if predicate(ticker) {
			rts.channel <- ticker
		}
	}
	if rts.iter.Err() != nil {
		log.Fatal(rts.iter.Err())
	}
	close(rts.channel)
}

func (rts *restTickerSource) getChannel() chan string {
	return rts.channel
}

type mapTickerSource struct {
	tickers map[string]struct{}
	channel chan string
}

func (mts *mapTickerSource) accumulate(predicate func(string) bool) {
	for k, _ := range mts.tickers {
		if predicate(k) {
			mts.channel <- k
		}
	}
}

func (mts *mapTickerSource) getChannel() chan string {
	return mts.channel
}
