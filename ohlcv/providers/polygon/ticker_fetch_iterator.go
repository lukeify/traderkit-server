package polygon

import (
	"context"
	"fmt"

	"github.com/polygon-io/client-go/rest"
)

type RestTickerFetchIterator struct {
	client  *polygon.Client
	tickers chan string
}

func NewRestTickerFetchIterator(client *polygon.Client) *RestTickerFetchIterator {
	// TODO: Is this buffer size appropriate? Currently it holds all U.S. equity tickers with space to spare.
	return &RestTickerFetchIterator{
		client:  client,
		tickers: make(chan string, 20000),
	}
}

func (rtfi *RestTickerFetchIterator) Close() {
	close(rtfi.tickers)
}

func (rtfi *RestTickerFetchIterator) Push(ticker string) {
	rtfi.tickers <- ticker
}

// Read uses the functionality of `select` (https://go.dev/ref/spec#Select_statements) to wait until the `tfi.tickers`
// channel has data available to read from (this is when the ticker fetchign retri
func (rtfi *RestTickerFetchIterator) Read(ctx context.Context, fn func(string)) error {
	// TODO: This used to have a `for` loop around it? Maybe it's not needed.
	select {
	case ticker, ok := <-rtfi.tickers:
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
