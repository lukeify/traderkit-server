package database

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func New() *pgx.Conn {
	dbUrl := "postgres://postgres:postgres@localhost:5432/postgres"
	conn, err := pgx.Connect(context.Background(), dbUrl)

	if err != nil {
		fmt.Printf("Unable to connect to datbaase: %v\n", err)
		os.Exit(1)
	}

	return conn
}
