package polygon

import (
	"log"
	"os"
	"time"

	"traderkit-server/ohlcv"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/polygon-io/client-go/rest"
)

// Ingestion conforms to the `IngestionProvider` interface.
type Ingestion struct {
	metrics *ohlcv.Metrics
	client  *polygon.Client
}

func New() *Ingestion {
	// Increase the timeout of the HTTP client for `polygon` to avoid https://github.com/polygon-io/client-go/issues/508
	pc := polygon.New(os.Getenv("POLYGON_API_KEY"))
	pc.Client.HTTP.SetTimeout(60 * time.Second)

	return &Ingestion{client: pc}
}

func (i *Ingestion) Backfill(ingestFrom time.Time, tickers map[string]struct{}) (pgx.CopyFromSource, error) {
	// TODO: Support being agnostic about the flat file source, so we don't always need to retrieve from Polygon, i.e.
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

	bf := &backfillIterator{
		client:     i.client,
		metrics:    i.metrics,
		ingestFrom: ingestFrom,
		tickers:    tickers,
		source:     FlatFiles,
	}

	bf.flatFiles = &flatFilesBackfill{parent: bf, minio: m}
	bf.rest = &restBackfill{parent: bf}

	return bf, nil
}

func (i *Ingestion) SetMetrics(m *ohlcv.Metrics) {
	i.metrics = m
}
