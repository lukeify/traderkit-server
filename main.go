package main

import (
	"fmt"
	"log"
	"net"
	"os"

	healthChecksApi "traderkit-server/apis/health_checks"
	"traderkit-server/database"
	"traderkit-server/ohlcv"
	pip "traderkit-server/ohlcv/providers"
	"traderkit-server/utils"

	"google.golang.org/grpc"
)

func main() {
	// Application startup: load environment variables, initialize a database connection, and backfill any data that
	// has been missed since last startup.
	if err := utils.LoadEnvFile(); err != nil {
		os.Exit(1)
	}
	db := database.New()

	// Create an ingestor struct that uses `Polygon` as the ingestion data provider. Then backfill any unloaded data
	//into the `bars` database table. This may not need to be done if the table is up to date. Alternatively, it may
	//need to be completely done if the table is empty.
	ohlcv.NewIngestor(db, pip.New()).Backfill()

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
