package providers

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
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

func (pi *PolygonIngestion) BackfilledData(ingestFrom time.Time) (pgx.CopyFromSource, error) {
	// TODO: Support being agnostic about the flat file source, so we don't always need to retrieve from Polygon, i.e.
	//  we could retrieve from a local CSV file.
	// TODO: Support picking up backfilling from a partially backfilled polygon flat file.
	// TODO: Once flat files are exhausted, switch to REST API for backfilling.
	// TODO: Support not backfilling data that has already been backfilled.

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
	obj        *minio.Object
	gz         *gzip.Reader
	csv        *csv.Reader
	row        []string
	err        error
	metrics    backfillMetrics
}

// Next prepares the next row of data to be read for backfilling. Data is ready sequentially from the Polygon's
// flatfiles corresponding to the `ingestFrom` date, iterating through each file until no more flatfiles exist.
// Following this, the iterator switches to reading from the REST API for un-backfilled data that is not available in a
// flatfile yet (a flatfile for the yesterday's data is not published until 11AM ET the following day).
//
// If the backfill has not begun, then `pbs.gz` will be `nil`, and opening a flatfile corresponding to the `ingestFrom`
// date will be attempted.
func (pbs *polygonBackfillIter) Next() bool {
	if pbs.gz == nil {
		// TODO: This is a roundabout way of having openFlatFile have access to the file name. It should be passed in
		//   as a param.
		pbs.metrics.setFileName(pbs.toFlatFileName(pbs.ingestFrom))
		err := pbs.openFlatFile()
		if err != nil {
			// TODO: This is assumed to be that the next flat file does not exist, switch to ingesting from the
			//   REST API.
			return false
		}
	}

	err := pbs.readFromFlatFile()
	if err == io.EOF {
		pbs.closeFlatFile()
		return pbs.Next()
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
func (pbi *polygonBackfillIter) toFlatFileName(t time.Time) string {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		panic(err)
	}

	t = t.In(loc)

	return path.Join(
		"us_stocks_sip",
		"minute_aggs_v1",
		t.Format("2006"),
		t.Format("01"),
		t.Format("2006-01-02")+".csv.gz",
	)
}

// openFlatFile will open the flatfile that corresponds to the `ingestFrom` date currently stored in the struct.
func (pbs *polygonBackfillIter) openFlatFile() error {
	var err error
	pbs.obj, err = pbs.s3.GetObject(
		context.Background(),
		"flatfiles",
		pbs.metrics.fileName,
		minio.GetObjectOptions{},
	)
	if err != nil {
		panic(fmt.Sprintf("[Error] pbs.s3.GetObject() error: %v\n", err))
	}

	// If the flatfile does not exist on the server (such as because it hasn't been uploaded yet), this is where the
	// error will be encountered—calling minio.GetObject() merely instantiates an object instance, it doesn't fetch it.
	pbs.gz, err = gzip.NewReader(pbs.obj)
	if err != nil {
		// TODO: Close pbs.obj here.
		var minioErr minio.ErrorResponse
		if errors.As(err, &minioErr) && (minioErr.StatusCode == 403 || minioErr.StatusCode == 404) {
			return err
		} else {
			panic(fmt.Sprintf("[Error] gzip.NewReader() error: %v\n", err))
		}
	}

	pbs.csv = csv.NewReader(pbs.gz)
	// Read the first row to ignore the header.
	_, err = pbs.csv.Read()
	if err != nil {
		panic(fmt.Sprintf("[Error] csv.Read() error reading header row: %#v\n", err))
	}

	return nil
}

func (pbs *polygonBackfillIter) readFromFlatFile() error {
	// TODO: Read forwards to the ingestFrom time, discarding anything before that, which is the contract which
	//  specifies where the backfill should start from.
	var err error
	pbs.row, err = pbs.csv.Read()

	if err == io.EOF {
		// TODO: Write a comment to the progress printer.
		// pbs.pp.Complete("Ingestion complete.")
		return err
	}
	if err != nil {
		panic(fmt.Sprintf("Row read error %#v\n", err))
	}

	return nil
}

func (pbs *polygonBackfillIter) closeFlatFile() {
	err := pbs.gz.Close()
	pbs.gz = nil
	if err != nil {
		panic("[Error] pbs.gz.Close(): " + err.Error())
	}

	err = pbs.obj.Close()
	if err != nil {
		panic("[Error] pbs.obj.Close(): " + err.Error())
	}

	// TODO: Handle scenarios where the date advancement leads to today's date.
	pbs.ingestFrom = pbs.ingestFrom.AddDate(0, 0, 1)
	if pbs.ingestFrom.After(time.Now()) {
		panic("After now!")
	}
}

// TODO: Decouple the metrics from the polygon ingestion implementation. The polygon backfill iterator should provide
//   values that the ingestion functionality can pick up and display from.

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
