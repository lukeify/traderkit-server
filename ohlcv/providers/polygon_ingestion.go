package providers

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"time"

	"traderkit-server/utils/progress_printer"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/polygon-io/client-go/rest"
)

// PolygonIngestion conforms to the `IngestionProvider` interface.
type PolygonIngestion struct {
	client *polygon.Client
}

func New() *PolygonIngestion {
	return &PolygonIngestion{
		client: polygon.New(os.Getenv("POLYGON_API_KEY")),
	}
}

func (pi *PolygonIngestion) BackfilledData(symbols []string, ingestFrom time.Time) (pgx.CopyFromSource, error) {
	// TODO: Support picking up backfilling from a partially backfilled polygon flat file.
	// TODO: Support backfilling over multiple flat files.
	// TODO: Once flat files are exhausted, switch to REST API for backfilling.

	mc, err := minio.New(
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
		panic(fmt.Sprintf("Error instantiating MinIO client: %v\n", err))
	}

	return &polygonBackfillIter{
		pp:         progress_printer.NewProgressPrinter(os.Stdout),
		s3:         mc,
		ingestFrom: ingestFrom,
		metrics:    backfillMetrics{},
	}, nil
}

type polygonBackfillIter struct {
	pp         *progress_printer.ProgressPrinter
	s3         *minio.Client
	ingestFrom time.Time
	gz         *gzip.Reader
	csv        *csv.Reader
	row        []string
	err        error
	metrics    backfillMetrics
}

// Next prepares the next row of data to be read for backfilling. Data is ready sequentially from the flat files
// corresponding to the `ingestFrom` date, iterating until no more flat files exist. Following this, the iterator
// switches to reading from the REST API for un-backfilled data that is not available in a flat file yet.
//
// If this is the first row, it will instantiate a
// gzip & csv reader for the flat file corresponding to the `ingestFrom` date. A
func (pbs *polygonBackfillIter) Next() bool {
	if pbs.gz == nil && pbs.csv == nil {

		pbs.metrics.setFileName(pbs.toFlatFileName(pbs.ingestFrom))

		obj, err := pbs.s3.GetObject(
			context.Background(),
			"flatfiles",
			pbs.metrics.fileName,
			minio.GetObjectOptions{},
		)
		if err != nil {
			panic("s3.GetObject error: " + err.Error())
			// TODO: Handle error—this might be where the file is not found because it does not exist for date?
		}
		pbs.gz, err = gzip.NewReader(obj)
		if err != nil {
			panic(fmt.Sprintf("gzip.NewReader error: %#v", err))
		}

		pbs.csv = csv.NewReader(pbs.gz)
		// Read and ignore the header
		_, err = pbs.csv.Read()
		if err != nil {
			panic(fmt.Sprintf("csv.Read() header row error: %#v\n", err))
			return false
		}
	}

	var err error
	pbs.row, err = pbs.csv.Read()
	// End of file reached, progress towards next file, or begin making REST API calls.
	if err == io.EOF {
		pbs.pp.Complete("Ingestion complete.")
		return false
	}
	if err != nil {
		panic(fmt.Sprintf("Row read error %#v\n", err))
		return false
	}
	return true
}

func (pbs *polygonBackfillIter) Values() ([]any, error) {
	// Parse the CSV row into the expected values provided by polygon.
	// Extract ticker symbol
	sId := pbs.row[0]
	pbs.metrics.ingesting(sId)
	pbs.pp.Update(pbs.metrics.get())

	// Parse numeric values
	v, _ := strconv.ParseUint(pbs.row[1], 10, 32)
	o, _ := strconv.ParseFloat(pbs.row[2], 32)
	c, _ := strconv.ParseFloat(pbs.row[3], 32)
	h, _ := strconv.ParseFloat(pbs.row[4], 32)
	l, _ := strconv.ParseFloat(pbs.row[5], 32)

	// Parse timestamp (nanoseconds since epoch)
	windowStartNs, _ := strconv.ParseUint(pbs.row[6], 10, 64)
	ts := time.Unix(0, int64(windowStartNs))

	// Parse the transaction count
	txns, _ := strconv.ParseUint(pbs.row[7], 10, 32)

	// Return values in order matching the DB columns.
	return []any{sId, ts, o, h, l, c, v, txns}, nil
}

func (pbs *polygonBackfillIter) Err() error {
	return pbs.err
}

// Polygon's flat file naming structure is YYYY-MM-DD, accessible as a gzipped CSV file. The directory this flat file
// is placed under is the` minute_aggs_v1` directory, with year and month subdirectories.
func (pbs *polygonBackfillIter) toFlatFileName(t time.Time) string {
	return path.Join(
		"us_stocks_sip",
		"minute_aggs_v1",
		t.Format("2006"),
		t.Format("01"),
		t.Format("2006-01-02")+".csv.gz",
	)
}

type backfillMetrics struct {
	fileName string
	ticker   string
	barCount int
}

func (bm *backfillMetrics) setFileName(name string) {
	bm.fileName = name
}

func (bm *backfillMetrics) ingesting(ticker string) {
	bm.ticker = ticker
	bm.barCount++
}

func (bm *backfillMetrics) get() string {
	return fmt.Sprintf("[%s] %d bars ingested (current ticker: %s)", bm.fileName, bm.barCount, bm.ticker)
}
