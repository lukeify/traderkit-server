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
	m      *ohlcv.Metrics
	client *polygon.Client
}

func New() *Ingestion {
	return &Ingestion{
		client: polygon.New(os.Getenv("POLYGON_API_KEY")),
	}
}

func (i *Ingestion) Backfill(ingestFrom time.Time) (pgx.CopyFromSource, error) {
	// TODO: Support being agnostic about the flat file source, so we don't always need to retrieve from Polygon, i.e.
	//  we could retrieve from a local CSV file.
	// TODO: Once flat files are exhausted, switch to REST API for backfilling.
	s3, err := minio.New(
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

	return &backfillIterator{
		m:          i.m,
		s3:         s3,
		ingestFrom: ingestFrom,
	}, nil
}

func (i *Ingestion) SetMetrics(m *ohlcv.Metrics) {
	i.m = m
}
