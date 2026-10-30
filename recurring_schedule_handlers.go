package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"chronos/database"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func recurringScheduleID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("invalid recurring schedule ID")
	}
	return id, nil
}

func writeRecurringScheduleError(
	w http.ResponseWriter,
	action string,
	err error,
) {
	message := strings.ToLower(err.Error())

	if errors.Is(err, pgx.ErrNoRows) ||
		strings.Contains(message, "not found") {
		http.Error(w, "recurring schedule not found", http.StatusNotFound)
		return
	}

	if strings.Contains(message, "must be positive") ||
		strings.Contains(message, "interval must") ||
		strings.Contains(message, "whole number of seconds") ||
		strings.Contains(message, "not an interval schedule") ||
		strings.Contains(message, "not a cron schedule") ||
		strings.Contains(message, "permanently cancelled") ||
		strings.Contains(message, "calculate next cron occurrence") {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Failed to %s recurring schedule: %v", action, err)
	http.Error(
		w,
		"failed to "+action+" recurring schedule",
		http.StatusInternalServerError,
	)
}

func writeRecurringSchedule(
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	id int64,
	action string,
) {
	updated, err := database.GetRecurringSchedule(r.Context(), pool, id)
	if err != nil {
		writeRecurringScheduleError(w, action, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(updated); err != nil {
		log.Printf("Recurring schedule response encoding error: %v", err)
	}
}

func recurringScheduleEnabledHandler(
	pool *pgxpool.Pool,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id, err := recurringScheduleID(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var request struct {
			Enabled *bool `json:"enabled"`
		}

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		if request.Enabled == nil {
			http.Error(
				w,
				"enabled is required and must be a boolean",
				http.StatusBadRequest,
			)
			return
		}

		err = database.SetRecurringScheduleEnabled(
			r.Context(),
			pool,
			id,
			*request.Enabled,
		)
		if err != nil {
			writeRecurringScheduleError(w, "update", err)
			return
		}

		writeRecurringSchedule(w, r, pool, id, "retrieve")
	}
}

func rescheduleIntervalHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id, err := recurringScheduleID(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var request struct {
			Interval string `json:"interval"`
		}

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		if strings.TrimSpace(request.Interval) == "" {
			http.Error(w, "interval is required", http.StatusBadRequest)
			return
		}

		interval, err := time.ParseDuration(request.Interval)
		if err != nil {
			http.Error(
				w,
				"invalid interval; use a duration such as 15m or 1h30m",
				http.StatusBadRequest,
			)
			return
		}

		if err := database.RescheduleIntervalSchedule(
			r.Context(),
			pool,
			id,
			interval,
		); err != nil {
			writeRecurringScheduleError(w, "reschedule", err)
			return
		}

		writeRecurringSchedule(w, r, pool, id, "retrieve")
	}
}

func rescheduleCronHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id, err := recurringScheduleID(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var request struct {
			CronExpression string `json:"cron_expression"`
			Timezone       string `json:"timezone"`
		}

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		if strings.TrimSpace(request.CronExpression) == "" {
			http.Error(
				w,
				"cron_expression is required",
				http.StatusBadRequest,
			)
			return
		}

		if strings.TrimSpace(request.Timezone) == "" {
			http.Error(w, "timezone is required", http.StatusBadRequest)
			return
		}

		if err := database.RescheduleCronSchedule(
			r.Context(),
			pool,
			id,
			request.CronExpression,
			request.Timezone,
		); err != nil {
			writeRecurringScheduleError(w, "reschedule", err)
			return
		}

		writeRecurringSchedule(w, r, pool, id, "retrieve")
	}
}

func cancelRecurringScheduleHandler(
	pool *pgxpool.Pool,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(
				w,
				"method not allowed",
				http.StatusMethodNotAllowed,
			)
			return
		}

		id, err := recurringScheduleID(r)
		if err != nil {
			http.Error(
				w,
				err.Error(),
				http.StatusBadRequest,
			)
			return
		}

		err = database.CancelRecurringSchedule(
			r.Context(),
			pool,
			id,
		)
		if err != nil {
			writeRecurringScheduleError(
				w,
				"cancel",
				err,
			)
			return
		}

		writeRecurringSchedule(w, r, pool, id, "retrieve")
	}
}

// func newRecurringScheduleHandlerTestMuxWithPool(
// 	pool *pgxpool.Pool,
// ) *http.ServeMux {
// 	mux := http.NewServeMux()

// 	mux.HandleFunc(
// 		"PATCH /recurring-schedules/{id}/enabled",
// 		recurringScheduleEnabledHandler(pool),
// 	)
// 	mux.HandleFunc(
// 		"PATCH /recurring-schedules/{id}/interval",
// 		rescheduleIntervalHandler(pool),
// 	)
// 	mux.HandleFunc(
// 		"PATCH /recurring-schedules/{id}/cron",
// 		rescheduleCronHandler(pool),
// 	)

// 	return mux
// }

// func newRecurringScheduleHandlerTestPool(
// 	t *testing.T,
// ) *pgxpool.Pool {
// 	t.Helper()

// 	if err := godotenv.Load(); err != nil {
// 		t.Log(".env not found; using system environment variables")
// 	}

// 	ctx, cancel := context.WithTimeout(
// 		context.Background(),
// 		10*time.Second,
// 	)
// 	defer cancel()

// 	pool, err := database.Connect(ctx)
// 	if err != nil {
// 		t.Fatalf("connect to database: %v", err)
// 	}

// 	t.Cleanup(pool.Close)

// 	return pool
// }

// func createHandlerTestIntervalSchedule(
// 	t *testing.T,
// 	pool *pgxpool.Pool,
// ) int64 {
// 	t.Helper()

// 	ctx, cancel := context.WithTimeout(
// 		context.Background(),
// 		10*time.Second,
// 	)
// 	defer cancel()

// 	name := fmt.Sprintf(
// 		"api-interval-test-%d",
// 		time.Now().UnixNano(),
// 	)

// 	original, err := schedule.NewRecurringSchedule(
// 		name,
// 		5*time.Minute,
// 		time.Now().Add(time.Hour),
// 	)
// 	if err != nil {
// 		t.Fatalf("create interval schedule: %v", err)
// 	}

// 	if err := database.SaveRecurringSchedule(ctx, pool, original); err != nil {
// 		t.Fatalf("save interval schedule: %v", err)
// 	}

// 	var id int64
// 	err = pool.QueryRow(
// 		ctx,
// 		`SELECT id FROM recurring_schedules WHERE name = $1`,
// 		name,
// 	).Scan(&id)
// 	if err != nil {
// 		t.Fatalf("get interval schedule ID: %v", err)
// 	}

// 	t.Cleanup(func() {
// 		cleanupCtx, cleanupCancel := context.WithTimeout(
// 			context.Background(),
// 			5*time.Second,
// 		)
// 		defer cleanupCancel()

// 		_, err := pool.Exec(
// 			cleanupCtx,
// 			`DELETE FROM recurring_schedules WHERE id = $1`,
// 			id,
// 		)
// 		if err != nil {
// 			t.Errorf("delete test interval schedule: %v", err)
// 		}
// 	})

// 	return id
// }

// func createHandlerTestCronSchedule(
// 	t *testing.T,
// 	pool *pgxpool.Pool,
// ) int64 {
// 	t.Helper()

// 	ctx, cancel := context.WithTimeout(
// 		context.Background(),
// 		10*time.Second,
// 	)
// 	defer cancel()

// 	name := fmt.Sprintf(
// 		"api-cron-test-%d",
// 		time.Now().UnixNano(),
// 	)

// 	original, err := schedule.NewCronSchedule(
// 		name,
// 		"0 9 * * *",
// 		"Asia/Kolkata",
// 		time.Now().Add(24*time.Hour),
// 	)
// 	if err != nil {
// 		t.Fatalf("create cron schedule: %v", err)
// 	}

// 	if err := database.SaveRecurringSchedule(ctx, pool, original); err != nil {
// 		t.Fatalf("save cron schedule: %v", err)
// 	}

// 	var id int64
// 	err = pool.QueryRow(
// 		ctx,
// 		`SELECT id FROM recurring_schedules WHERE name = $1`,
// 		name,
// 	).Scan(&id)
// 	if err != nil {
// 		t.Fatalf("get cron schedule ID: %v", err)
// 	}

// 	t.Cleanup(func() {
// 		cleanupCtx, cleanupCancel := context.WithTimeout(
// 			context.Background(),
// 			5*time.Second,
// 		)
// 		defer cleanupCancel()

// 		_, err := pool.Exec(
// 			cleanupCtx,
// 			`DELETE FROM recurring_schedules WHERE id = $1`,
// 			id,
// 		)
// 		if err != nil {
// 			t.Errorf("delete test cron schedule: %v", err)
// 		}
// 	})

// 	return id
// }
