package polygon

import (
	"fmt"
	"time"

	"traderkit-server/backfill"

	"github.com/polygon-io/client-go/rest"
)

type backfillIterator struct {
	client       *polygon.Client
	metrics      *backfill.Metrics
	backfillFrom time.Time
	predicate    func(string) bool
	source       backfillSource
	flatFiles    *flatFilesBackfill
	rest         *restBackfill
	err          error
}

func (bi *backfillIterator) MarkSourceCompleted(updatedBackfillFrom time.Time) {
	bi.backfillFrom = updatedBackfillFrom
	if bi.source == FlatFiles {
		bi.rest.startup()
		bi.source = RestAPI
	}
}

// Next prepares the next row of data to be read for backfilling. Data is ready sequentially from the Polygon's
// flatfiles corresponding to the `backfillFrom` date, iterating through each file until no more flatfiles exist.
// Following this, the iterator switches to reading from the REST API for un-backfilled data that is not available in a
// flatfile yet (a flatfile for the yesterday's data is not published until 11AM ET the following day).
//
// If the backfill has not begun, then `bi.gz` will be `nil`, and opening a flatfile corresponding to the `backfillFrom`
// date will be attempted.
func (bi *backfillIterator) Next() bool {
	switch bi.source {
	case FlatFiles:
		return bi.flatFiles.Next()
	case RestAPI:
		return bi.rest.Next()
	}
	return false
}

func (bi *backfillIterator) Values() ([]any, error) {
	switch bi.source {
	case FlatFiles:
		return bi.flatFiles.Values()
	case RestAPI:
		return bi.rest.Values()
	default:
		return nil, fmt.Errorf("unrecognized source type: %v", bi.source)
	}
}

func (bi *backfillIterator) Err() error {
	// TODO: Find out how to use this method.
	return bi.err
}

type backfillSource int

const (
	FlatFiles backfillSource = iota
	RestAPI
)
