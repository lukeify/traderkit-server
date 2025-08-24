package polygon

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"path"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7"
)

type flatFilesBackfill struct {
	parent       *backfillIterator
	minio        *minio.Client
	obj          *minio.Object
	gzipReader   *gzip.Reader
	csvReader    *csv.Reader
	flatFileDate time.Time
	row          []string
}

func (ffb *flatFilesBackfill) Next() bool {
	if ffb.gzipReader == nil {
		// If `openFlatFile` returns an error, it will be because the flat file does not exist on the server, so we
		// should switch to using the REST API fo continue to backfill. Switch over to the Rest API by setting the
		// source to `RestAPI` and internally calling the iterator.
		err := ffb.openFlatFile()
		if err != nil {
			// TODO: openFlatFile now increments
			println("Switching to rest API")
			// TODO: Close any open resources after switching to REST API from flat file API.
			ffb.parent.source = RestAPI
			ffb.parent.ingestFrom = ffb.getFlatFileDate()
			return ffb.parent.Next()
		}
	}

	err := ffb.readFromFlatFile()
	if err == io.EOF {
		ffb.closeFlatFile()
		return ffb.Next()
	} else if err != nil {
		log.Fatal("non-EOF error from reading from flat file: ", err)
	}

	return true
}

// Values parses the current CSV-extracted row from the FlatFile provided by Polygon into the expected types necessary
// for database insertion.
func (ffb *flatFilesBackfill) Values() ([]any, error) {
	sId := ffb.row[0]
	ffb.parent.metrics.IngestRow(sId)

	// Parse numeric values
	v, _ := strconv.ParseUint(ffb.row[1], 10, 32)
	o, _ := strconv.ParseFloat(ffb.row[2], 32)
	c, _ := strconv.ParseFloat(ffb.row[3], 32)
	h, _ := strconv.ParseFloat(ffb.row[4], 32)
	l, _ := strconv.ParseFloat(ffb.row[5], 32)

	// Parse timestamp (nanoseconds since epoch)
	windowStartNs, _ := strconv.ParseUint(ffb.row[6], 10, 64)
	ts := time.Unix(0, int64(windowStartNs))

	// Parse the transaction count
	txns, _ := strconv.ParseUint(ffb.row[7], 10, 32)

	// Return values in order matching the DB columns.
	return []any{sId, ts, o, h, l, c, v, txns}, nil
}

// toFlatFileName recreates Polygon's flat file naming structure. Which is YYYY-MM-DD in an S3 bucket, accessible as a
// gzipped CSV file. The directory this flat file is placed under is the` minute_aggs_v1` directory, with year and month
// subdirectories.
func (ffb *flatFilesBackfill) toFlatFileName(t time.Time) string {
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

func (ffb *flatFilesBackfill) openFlatFile() error {
	print("Calling getFlatFileDate in openFile\n")
	fileName := ffb.toFlatFileName(ffb.getFlatFileDate())
	fmt.Printf("File name is %s\n", fileName)
	var err error
	ffb.obj, err = ffb.minio.GetObject(
		context.Background(),
		"flatfiles",
		fileName,
		minio.GetObjectOptions{},
	)
	if err != nil {
		log.Fatalf("[Fatal] bi.s3.GetObject() error: %v\n", err)
	}
	ffb.parent.metrics.SetSource(fileName)

	// If the flatfile does not exist on the server (such as because it hasn't been uploaded yet), this is where the
	// error will be encountered—calling minio.GetObject() merely instantiates an object instance, it doesn't fetch it.
	ffb.gzipReader, err = gzip.NewReader(ffb.obj)
	if err != nil {
		// TODO: Check closing here is appropriate
		ffb.closeMinioObject()

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

	ffb.csvReader = csv.NewReader(ffb.gzipReader)
	// Read the first row to ignore the header.
	_, err = ffb.csvReader.Read()
	if err != nil {
		log.Fatalf("[Fatal] csv.Read() error reading header row: %#v\n", err)
	}

	// Increment the date so the next time we open a flat file, it will use the newly-set date. This also avoids having
	// to wind back the date by a day if the flat file does not exist.
	print("Calling setNextFlatFileDate at end of openFile\n")
	ffb.setNextFlatFileDate()
	print("End of openFile, flatFileDate is now ", ffb.flatFileDate.String(), "\n")
	//if err != nil {
	//	log.Fatalf("[Fatal] bi.incrementDate() error: %#v\n", err)
	//}

	return nil
}

// readFromFlatFile reads rows until it finds a row that can be inserted into the database. Rows that can be inserted
// are from row reads that don't result in an error, have a predicate that indicates the row should be skipped, or rows
// that are equal to or after the `ingestFrom` time (rows before the `ingestFrom` time are discarded as they are already
// stored in the database).
func (ffb *flatFilesBackfill) readFromFlatFile() error {
	var err error

	// Break either when there's an error, or an acceptable row that can be inserted.
	for {
		ffb.row, err = ffb.csvReader.Read()
		if err != nil {
			break
		}

		// Continue looping while the predicate indicates the row should be skipped.
		if !ffb.parent.predicate(ffb.row[0]) {
			ffb.parent.metrics.SkipRow()
			continue
		}

		// Break if the row's timestamp is equal or after the `ingestFrom`
		windowStartNs, _ := strconv.ParseUint(ffb.row[6], 10, 64)
		ts := time.Unix(0, int64(windowStartNs))

		if ts.Equal(ffb.parent.ingestFrom) || ts.After(ffb.parent.ingestFrom) {
			break
		}

		ffb.parent.metrics.SkipRow()
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

func (ffb *flatFilesBackfill) closeFlatFile() {
	err := ffb.gzipReader.Close()
	ffb.gzipReader = nil
	if err != nil {
		log.Fatalf("gzip.Close() %#v\n", err)
	}

	ffb.closeMinioObject()
}

func (ffb *flatFilesBackfill) closeMinioObject() {
	err := ffb.obj.Close()
	if err != nil {
		log.Fatalf("minio.Object.Close() %#v\n", err)
	}
}

func (ffb *flatFilesBackfill) getFlatFileDate() time.Time {
	// fmt.Printf("current flatFileDate: %s\n", ffb.flatFileDate)
	fmt.Printf("[getFlatFileDate] current flatFileDate: %s\n", ffb.flatFileDate)
	if ffb.flatFileDate.IsZero() {
		fmt.Printf("flatFileDate is zero")
		ffb.flatFileDate = ffb.parent.ingestFrom

	}
	return ffb.flatFileDate
}

func (ffb *flatFilesBackfill) setNextFlatFileDate() {
	fmt.Printf("[setNextFlatFileDate] current flatFileDate: %s\n", ffb.flatFileDate)
	if ffb.flatFileDate.IsZero() {
		fmt.Printf("flatFileDate is zero")
		ffb.flatFileDate = ffb.parent.ingestFrom
	}

	loc, _ := time.LoadLocation("America/New_York")
	ffb.flatFileDate = time.Date(
		ffb.flatFileDate.Year(),
		ffb.flatFileDate.Month(),
		ffb.flatFileDate.Day()+1,
		0,
		0,
		0,
		0,
		loc,
	)
}
