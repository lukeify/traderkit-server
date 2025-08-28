package polygon

import (
	"log"
	"os"
	"time"

	"traderkit-server/backfill"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/polygon-io/client-go/rest"
)

// Backfill conforms to the `Provider` interface.
type Backfill struct {
	metrics *backfill.Metrics
	client  *polygon.Client
}

// New returns a new polygon `Backfill` instance. Before returning, it increases the timeout of the HTTP client for
// `polygon` to avoid https://github.com/polygon-io/client-go/issues/508
func New() *Backfill {
	pc := polygon.New(os.Getenv("POLYGON_API_KEY"))
	pc.Client.HTTP.SetTimeout(60 * time.Second)

	return &Backfill{client: pc}
}

func (b *Backfill) Backfill(backfillFrom time.Time, predicate func(string) bool) (pgx.CopyFromSource, error) {
	// TODO: Support being agnostic about the flat file source, so we don't always need to retrieve from Polygon, b.e.
	//  we could retrieve from a local CSV file.
	m, err := minio.New(
		"files.polygon.io",
		&minio.Options{
			Creds: credentials.NewStaticV4(
				os.Getenv("POLYGON_FLAT_FILES_ACCESS_KEY_ID"),
				os.Getenv("POLYGON_FLAT_FILES_SECRET_ACCESS_KEY"),
				"",
			),
			Secure: true,
		})
	if err != nil {
		log.Fatalf("Error instantiating MinIO client: %v\n", err)
	}

	bi := &backfillIterator{
		client:       b.client,
		metrics:      b.metrics,
		backfillFrom: backfillFrom,
		predicate:    predicate,
		source:       FlatFiles,
	}

	bi.flatFiles = &flatFilesBackfill{parent: bi, minio: m}

	tickerSource := newRestTickerSource(b.client)
	// TODO: Determine if this is the right buffer size for capturing aggregates
	aggPool := newAggregatePool(b.client, tickerSource, 1000)

	bi.rest = &restBackfill{
		parent:       bi,
		tickerSource: tickerSource,
		aggPool:      aggPool,
	}

	return bi, nil
}

func (b *Backfill) SetMetrics(m *backfill.Metrics) {
	b.metrics = m
}
