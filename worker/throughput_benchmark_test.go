package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"chronos/database"
	"chronos/job"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type benchmarkHandler struct {
	completed atomic.Int64
}

func (h *benchmarkHandler) Execute(
	ctx context.Context,
	j job.Job,
) error {
	h.completed.Add(1)
	return nil
}

func BenchmarkWorkerThroughput(b *testing.B) {
	if err := godotenv.Load("../.env"); err != nil {
		b.Log(".env not found; using environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Minute,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		b.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	for _, workerCount := range []int{1, 2, 4, 8} {
		b.Run(
			fmt.Sprintf("workers=%d", workerCount),
			func(b *testing.B) {
				benchmarkWorkerThroughput(
					b,
					ctx,
					pool,
					workerCount,
				)
			},
		)
	}
}

func benchmarkWorkerThroughput(
	b *testing.B,
	ctx context.Context,
	pool *pgxpool.Pool,
	workerCount int,
) {
	handler := &benchmarkHandler{}

	workers := make([]*Worker, workerCount)

	for i := range workers {
		workers[i] = &Worker{
			WorkerID: fmt.Sprintf(
				"benchmark-worker-%d",
				i,
			),
			Pool:    pool,
			Handler: handler,
		}
	}

	baseID := 1_000_000_000 +
		int(time.Now().UnixNano()%500_000_000)

	// Cleanup runs before the database pool is closed.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()

		_, err := pool.Exec(
			cleanupCtx,
			"DELETE FROM jobs WHERE id >= $1 AND id < $2",
			baseID,
			baseID+b.N,
		)
		if err != nil {
			b.Logf("cleanup benchmark jobs: %v", err)
		}
	}()

	b.StopTimer()

	for i := 0; i < b.N; i++ {
		id := baseID + i

		j, err := job.NewJob(
			id,
			"throughput-benchmark",
		)
		if err != nil {
			b.Fatalf("create benchmark job: %v", err)
		}

		j.RunAt = time.Now().Add(-time.Minute)

		if err := database.SaveJob(ctx, pool, j); err != nil {
			b.Fatalf("save benchmark job: %v", err)
		}
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	var processed atomic.Int64
	var wg sync.WaitGroup

	errCh := make(chan error, 1)

	b.ReportAllocs()
	b.StartTimer()

	for i := range workers {
		wg.Add(1)

		go func(w *Worker) {
			defer wg.Done()

			for {
				if workerCtx.Err() != nil {
					return
				}

				if processed.Load() >= int64(b.N) {
					return
				}

				ran, err := w.RunOnce(workerCtx)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}

					workerCancel()
					return
				}

				if ran {
					processed.Add(1)
					continue
				}

				time.Sleep(time.Millisecond)
			}
		}(workers[i])
	}

	wg.Wait()
	b.StopTimer()

	select {
	case err := <-errCh:
		b.Fatalf("worker execution failed: %v", err)
	default:
	}

	if processed.Load() != int64(b.N) {
		b.Fatalf(
			"processed %d jobs; expected %d",
			processed.Load(),
			b.N,
		)
	}

	if handler.completed.Load() != int64(b.N) {
		b.Fatalf(
			"handler completed %d jobs; expected %d",
			handler.completed.Load(),
			b.N,
		)
	}

	b.ReportMetric(
		float64(processed.Load())/b.Elapsed().Seconds(),
		"jobs/sec",
	)
}
