package ohlcv

import "time"

type IngestionProvider interface {
	RetrieveBackfilledData(symbols []string, ingestFrom time.Time)
}
