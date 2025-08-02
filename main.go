package main

import (
	"log"
	"traderkit-server/database"
	"traderkit-server/ohlcv"
	pip "traderkit-server/ohlcv/providers"
	"traderkit-server/utils"
)

func main() {
	// Application startup: load environment variables, initialize a database connection, and backfill any data that
	// has been missed since last startup.
	if err := utils.LoadEnvFile(); err != nil {
		log.Fatalf("Error loading environment variables: %v\n", err)
	}
	db := database.New()

	// Create an ingestor struct that uses `Polygon` as the ingestion data provider. Then backfill any unloaded data
	//into the `bars` database table. This may not need to be done if the table is up to date. Alternatively, it may
	//need to be completely done if the table is empty.
	err := ohlcv.NewIngestor(db, pip.New()).Backfill()
	if err != nil {
		log.Fatalf("Backfill failed with error: %v\n", err)
	}

	//if err != nil {
	//	fmt.Printf("Backfill failed %#v\n", err)
	//	os.Exit(1)
	//}

	//app := fiber.New()
	//
	//app.Get("/", func(c *fiber.Ctx) error {
	//	return c.SendString("Hello, World!")
	//})
	//
	//log.Fatal(app.Listen(":3000"))
}
