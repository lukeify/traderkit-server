package database

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/jackc/pgx/v5"
)

// New creates a new database connection, initializes the `migrations` table if it doesn't exist,
// and then runs any migrations that haven't already been applied.
func New() *pgxpool.Pool {
	dbUrl := os.Getenv("DATABASE_URL")
	pool, err := pgxpool.New(context.Background(), dbUrl)
	if err != nil {
		fmt.Printf("Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	_, err = pool.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS migrations (name VARCHAR(255))")
	if err != nil {
		fmt.Printf("Unable to create migrations table: %v\n", err)
		os.Exit(1)
	}

	runMigrations(pool)

	return pool
}

// runMigrations gathers the `.sql` files in the migration directory, retrieves the applied migrations from the
// database, and then compares
func runMigrations(pool *pgxpool.Pool) {
	allMigrations, err := filepath.Glob("./migrations/*.sql")
	if err != nil {
		fmt.Printf("Unable to read migrations directory: %v\n", err)
	}

	rows, err := pool.Query(context.Background(), "SELECT * FROM migrations")
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
		executeMigrationFile(pool, file)
	}
}

// executeMigrationFile reads the contents of a migration file and applies to against the database using the provided
// connection. It also inserts a record of the migration into the `migrations` table to track that the migration has
// been applied.
func executeMigrationFile(pool *pgxpool.Pool, fileName string) {
	contents, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Printf("Unable to read unapplied migration file %s: %v\n", fileName, err)
		os.Exit(1)
	}

	// Initiate a transaction, rolling back after the method completes.
	tx, err := pool.Begin(context.Background())
	if err != nil {
		fmt.Printf("Unable to begin transaction for migration %s: %v\n", fileName, err)
		os.Exit(1)
	}
	defer tx.Rollback(context.Background())

	// Apply the migration
	_, err = tx.Exec(context.Background(), string(contents))
	if err != nil {
		fmt.Printf("Unable to apply migration %s: %v\n", fileName, err)
		os.Exit(1)
	}

	_, err = tx.Exec(context.Background(), "INSERT INTO migrations (name) VALUES ($1);", fileName)
	if err != nil {
		fmt.Printf("Unable to persist migration status %s: %v\n", fileName, err)
		os.Exit(1)
	}

	err = tx.Commit(context.Background())
	fmt.Printf("Appled migration %s successfully.\n", fileName)
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
