package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"

	healthChecksApi "traderkit-server/apis/health_checks"
	"traderkit-server/backfill"
	polygonBackfill "traderkit-server/backfill/providers/polygon"
	"traderkit-server/database"
	"traderkit-server/utils"

	"google.golang.org/grpc"
)

func main() {
	if err := os.Setenv("TZ", ""); err != nil {
		log.Fatal("Could not set timezone to UTC")
	}
	// Application startup: load environment variables, initialize a database connection, and backfill any data that
	// has been missed since last startup.
	if err := utils.LoadEnvFile(); err != nil {
		log.Fatalf("Error loading environment variables: %v\n", err)
	}
	db := database.New()

	// Create an ingestor struct that uses `Polygon` as the ingestion data provider. Then backfill any unloaded data
	// into the `bars` database table. This may not need to be done if the table is up to date. Alternatively, it may
	// need to be completely done if the table is empty.
	err := backfill.NewBackfill(db, polygonBackfill.New()).Backfill(awaitNextMinute(), func(ticker string) bool {
		return ticker == "AA"
	})
	if err != nil {
		log.Fatalf("Backfill failed with error: %v\n", err)
	}

	// After backfilling is complete, start the gRPC server and register the services.
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", 8080))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	healthChecksApi.RegisterHealthChecksServer(grpcServer, &healthChecksApi.HealthChecksServerImpl{})
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// awaitNextMinute will sleep until the start of the next minute, and return that `time.Time`. This is the demarcation
// point between backfilled data and live data. This is then passed to the backfilling operation as the endpoint for
// when to retrieve data until.
func awaitNextMinute() time.Time {
	now := time.Now()
	next := now.Truncate(time.Minute).Add(time.Minute)

	if now.Second() > 50 {
		next = next.Add(time.Minute)
	}

	time.Sleep(time.Until(next))

	return next
}
