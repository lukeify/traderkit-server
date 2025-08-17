package polygon

import (
	"context"
	"fmt"

	"github.com/polygon-io/client-go/rest"
)

type TickerIterator struct {
	client  *polygon.Client
	tickers chan string
}

func NewTickerIterator(client *polygon.Client) *TickerIterator {
	// TODO: Is this buffer size appropriate? Currently it holds all U.S. equity tickers with space to spare.
	return &TickerIterator{
		client:  client,
		tickers: make(chan string, 20000),
	}
}

func (ti *TickerIterator) Close() {
	close(ti.tickers)
}

func (ti *TickerIterator) Push(ticker string) {
	ti.tickers <- ticker
}

// Read uses the functionality of `select` (https://go.dev/ref/spec#Select_statements) to wait until the `tfi.tickers`
// channel has data available to read from (this is when the ticker fetchign retri
func (ti *TickerIterator) Read(ctx context.Context, fn func(string)) error {
	// TODO: This used to have a `for` loop around it? Maybe it's not needed.
	select {
	case ticker, ok := <-ti.tickers:
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
