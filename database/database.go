package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/jackc/pgx/v5"
)

// New creates a new database connection, initializes the `migrations` table if it doesn't exist,
// and then runs any migrations that haven't already been applied.
func New() *pgx.Conn {
	dbUrl := "postgres://postgres:postgres@localhost:5432/postgres"
	conn, err := pgx.Connect(context.Background(), dbUrl)
	if err != nil {
		fmt.Printf("Unable to connect to datbaase: %v\n", err)
		os.Exit(1)
	}

	_, err = conn.Query(context.Background(), "CREATE TABLE IF NOT EXISTS migrations (name VARCHAR(255))")
	if err != nil {
		fmt.Printf("Unable to create migrations table: %v\n", err)
		os.Exit(1)
	}

	runMigrations(conn)

	return conn
}

// runMigrations gathers the `.sql` files in the migration directory, retrieves the applied migrations from the
// database, and then compares
func runMigrations(conn *pgx.Conn) {
	allMigrations, err := filepath.Glob("./migrations/*.sql")
	if err != nil {
		fmt.Printf("Unable to read migrations directory: %v\n", err)
	}

	rows, err := conn.Query(context.Background(), "SELECT * FROM migrations")
	if err != nil {
		fmt.Printf("Unable to read migrations from table: %v\n", err)
	}

	appliedMigrations, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		fmt.Printf("CollectRows for applied migrations error: %v\n", err)
	}

	sort.Strings(allMigrations)
	sort.Strings(appliedMigrations)

	unappliedMigrations := migrationDifference(allMigrations, appliedMigrations)

	for _, file := range unappliedMigrations {
		executeMigrationFile(conn, file)
	}
}

// executeMigrationFile reads the contents of a migration file and applies to against the database using the provided
// connection. It also inserts a record of the migration into the `migrations` table to track that the migration has
// been applied.
func executeMigrationFile(conn *pgx.Conn, fileName string) {
	contents, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Printf("Unable to read unapplied migration file %s: %v\n", fileName, err)
		os.Exit(1)
	}

	sql := string(contents) + "INSERT INTO migrations (name) VALUES ($1);"

	_, err = conn.Exec(context.Background(), sql, fileName)
	if err != nil {
		fmt.Printf("Unable to apply migration %s: %v\n", fileName, err)
		os.Exit(1)
	}
}

// migrationDifference returns a slice of migration file names that are in `all` but not in `applied`—these are the
// unapplied migrations that need to be executed for the application to boot. This function is currently O(n^2) and
// could be sped up in future by using a `map` for O(1) lookups (O(n) overall).
func migrationDifference(all, applied []string) []string {
	unapplied := make([]string, 0)
	for _, m := range all {
		if !slices.Contains(applied, m) {
			unapplied = append(unapplied, m)
		}
	}

	return unapplied
}
