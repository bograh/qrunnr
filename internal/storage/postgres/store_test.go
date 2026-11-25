package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge.dev/internal/storage/postgres"
	"taskforge.dev/internal/storage/storetest"
	"taskforge.dev/migrations"
)

// TestStoreContract runs the shared storage suite against a real Postgres.
// Set TASKFORGE_TEST_DATABASE_URL to a disposable database to enable it
// (`make test-integration` does this). Every table is truncated between
// subtests.
func TestStoreContract(t *testing.T) {
	pool := testPool(t)
	storetest.Run(t, func(t *testing.T) (storetest.QueueStore, storetest.TaskStore) {
		truncate(t, pool)
		return postgres.NewQueueStore(pool), postgres.NewTaskStore(pool)
	})
}

func TestMigrateIsIdempotent(t *testing.T) {
	pool := testPool(t)
	applied, err := postgres.Migrate(context.Background(), pool, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 0 {
		t.Fatalf("expected no pending migrations on second run, applied %v", applied)
	}
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TASKFORGE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TASKFORGE_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := postgres.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func truncate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`TRUNCATE task_events, workers, dead_letter_tasks, tasks, queues`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}
