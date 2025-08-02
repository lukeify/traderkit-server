package providers

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"time"

	"traderkit-server/ohlcv"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/polygon-io/client-go/rest"
)

// PolygonIngestion conforms to the `IngestionProvider` interface.
type PolygonIngestion struct {
	m      *ohlcv.Metrics
	client *polygon.Client
}

func New() *PolygonIngestion {
	return &PolygonIngestion{
		client: polygon.New(os.Getenv("POLYGON_API_KEY")),
	}
}

func (pi *PolygonIngestion) Backfill(ingestFrom time.Time) (pgx.CopyFromSource, error) {
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

	return &polygonBackfillIter{
		m:          pi.m,
		s3:         s3,
		ingestFrom: ingestFrom,
	}, nil
}

func (pi *PolygonIngestion) SetMetrics(m *ohlcv.Metrics) {
	pi.m = m
}

type polygonBackfillIter struct {
	m          *ohlcv.Metrics
	s3         *minio.Client
	ingestFrom time.Time
	obj        *minio.Object
	gz         *gzip.Reader
	csv        *csv.Reader
	row        []string
	err        error
}

// Next prepares the next row of data to be read for backfilling. Data is ready sequentially from the Polygon's
// flatfiles corresponding to the `ingestFrom` date, iterating through each file until no more flatfiles exist.
// Following this, the iterator switches to reading from the REST API for un-backfilled data that is not available in a
// flatfile yet (a flatfile for the yesterday's data is not published until 11AM ET the following day).
//
// If the backfill has not begun, then `pbi.gz` will be `nil`, and opening a flatfile corresponding to the `ingestFrom`
// date will be attempted.
func (pbi *polygonBackfillIter) Next() bool {
	// TODO: Make this a helper method
	if pbi.gz == nil {
		// If `openFlatFile` returns an error, it will be because the flat file does not exist on the server, so we
		// should switch to using the REST API fo continue to backfill.
		err := pbi.openFlatFile(pbi.toFlatFileName(pbi.ingestFrom))
		if err != nil {
			return false
		}
	}

	err := pbi.readFromFlatFile()
	if err == io.EOF {
		pbi.closeFlatFile()
		err = pbi.incrementDate()
		if err != nil {
			return false
		}
		return pbi.Next()
	} else if err != nil {
		log.Fatal("non-EOF error from reading from flat file: ", err)
	}

	return true
}

func (pbi *polygonBackfillIter) Values() ([]any, error) {
	// Parse the CSV row into the expected values provided by polygon.
	// Extract ticker symbol
	sId := pbi.row[0]
	pbi.m.IngestRow(sId)

	// Parse numeric values
	v, _ := strconv.ParseUint(pbi.row[1], 10, 32)
	o, _ := strconv.ParseFloat(pbi.row[2], 32)
	c, _ := strconv.ParseFloat(pbi.row[3], 32)
	h, _ := strconv.ParseFloat(pbi.row[4], 32)
	l, _ := strconv.ParseFloat(pbi.row[5], 32)

	// Parse timestamp (nanoseconds since epoch)
	windowStartNs, _ := strconv.ParseUint(pbi.row[6], 10, 64)
	ts := time.Unix(0, int64(windowStartNs))

	// Parse the transaction count
	txns, _ := strconv.ParseUint(pbi.row[7], 10, 32)

	// Return values in order matching the DB columns.
	return []any{sId, ts, o, h, l, c, v, txns}, nil
}

func (pbi *polygonBackfillIter) Err() error {
	// TODO: Find out how to use this method.
	return pbi.err
}

// Polygon's flat file naming structure is YYYY-MM-DD, accessible as a gzipped CSV file. The directory this flat file
// is placed under is the` minute_aggs_v1` directory, with year and month subdirectories.
func (pbi *polygonBackfillIter) toFlatFileName(t time.Time) string {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalf("[Fatal] Error loading timezone: %v\n", err)
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
func (pbi *polygonBackfillIter) openFlatFile(fileName string) error {
	var err error
	pbi.obj, err = pbi.s3.GetObject(
		context.Background(),
		"flatfiles",
		fileName,
		minio.GetObjectOptions{},
	)
	if err != nil {
		log.Fatalf("[Fatal] pbi.s3.GetObject() error: %v\n", err)
	}
	pbi.m.SetSource(fileName)

	// If the flatfile does not exist on the server (such as because it hasn't been uploaded yet), this is where the
	// error will be encountered—calling minio.GetObject() merely instantiates an object instance, it doesn't fetch it.
	pbi.gz, err = gzip.NewReader(pbi.obj)
	if err != nil {
		// TODO: Close pbi.obj here.
		var minioErr minio.ErrorResponse
		if errors.As(err, &minioErr) && (minioErr.StatusCode == 403 || minioErr.StatusCode == 404) {
			fmt.Printf(
				"[Warning] Flat file %s does not exist on the server %d, skipping.\n",
				fileName,
				minioErr.StatusCode,
			)
			return err
		} else {
			log.Fatalf("[Fatal] gzip.NewReader() error: %v\n", err)
		}
	}

	pbi.csv = csv.NewReader(pbi.gz)
	// Read the first row to ignore the header.
	_, err = pbi.csv.Read()
	if err != nil {
		log.Fatalf("[Fatal] csv.Read() error reading header row: %#v\n", err)
	}

	return nil
}

// readFromFlatFile reads rows until an error is received, or a row is encountered that is equal to or after the
// `ingestFrom` time (rows before the `ingestFrom` time are discarded as they are already stored in the database).
func (pbi *polygonBackfillIter) readFromFlatFile() error {
	var err error
	for {
		pbi.row, err = pbi.csv.Read()
		if err != nil {
			break
		}

		windowStartNs, _ := strconv.ParseUint(pbi.row[6], 10, 64)
		ts := time.Unix(0, int64(windowStartNs))

		if ts.Equal(pbi.ingestFrom) || ts.After(pbi.ingestFrom) {
			break
		}
		pbi.m.SkipRow()
	}

	if err == io.EOF {
		// TODO: Write a comment to the progress printer.
		return err
	}
	if err != nil {
		log.Fatalf("[Fatal] row read error %#v\n", err)
	}

	return nil
}

func (pbi *polygonBackfillIter) closeFlatFile() {
	err := pbi.gz.Close()
	pbi.gz = nil
	if err != nil {
		log.Fatalf("[Fatal] gzip.Close() %#v\n", err)
	}

	err = pbi.obj.Close()
	if err != nil {
		log.Fatalf("[Fatal] minio.Object.Close() %#v\n", err)
	}
}

func (pbi *polygonBackfillIter) incrementDate() error {
	pbi.ingestFrom = pbi.ingestFrom.AddDate(0, 0, 1)
	if pbi.ingestFrom.After(time.Now()) {
		return fmt.Errorf("cannot advance past current date")
	}
	return nil
}
