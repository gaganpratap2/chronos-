package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"chronos/database"
	"chronos/schedule"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// Register the recurring-schedule routes without a database.
// These tests exercise requests that should be rejected before
// any database operation is attempted.
func newRecurringScheduleHandlerTestMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc(
		"PATCH /recurring-schedules/{id}/enabled",
		recurringScheduleEnabledHandler(nil),
	)
	mux.HandleFunc(
		"PATCH /recurring-schedules/{id}/interval",
		rescheduleIntervalHandler(nil),
	)
	mux.HandleFunc(
		"PATCH /recurring-schedules/{id}/cron",
		rescheduleCronHandler(nil),
	)

	return mux
}

func TestRecurringScheduleHandlersRejectInvalidIDs(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "enabled endpoint",
			path: "/recurring-schedules/not-a-number/enabled",
			body: `{"enabled":true}`,
		},
		{
			name: "interval endpoint",
			path: "/recurring-schedules/not-a-number/interval",
			body: `{"interval":"15m"}`,
		},
		{
			name: "cron endpoint",
			path: "/recurring-schedules/not-a-number/cron",
			body: `{"cron_expression":"*/5 * * * *","timezone":"Asia/Kolkata"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mux := newRecurringScheduleHandlerTestMux()

			request := httptest.NewRequest(
				http.MethodPatch,
				tc.path,
				strings.NewReader(tc.body),
			)
			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf(
					"expected status %d, got %d; body: %s",
					http.StatusBadRequest,
					response.Code,
					response.Body.String(),
				)
			}
		})
	}
}

func TestRecurringScheduleHandlersRejectMalformedJSON(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{
			name: "enabled endpoint",
			path: "/recurring-schedules/123/enabled",
		},
		{
			name: "interval endpoint",
			path: "/recurring-schedules/123/interval",
		},
		{
			name: "cron endpoint",
			path: "/recurring-schedules/123/cron",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mux := newRecurringScheduleHandlerTestMux()

			request := httptest.NewRequest(
				http.MethodPatch,
				tc.path,
				strings.NewReader("{"),
			)
			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf(
					"expected status %d, got %d; body: %s",
					http.StatusBadRequest,
					response.Code,
					response.Body.String(),
				)
			}
		})
	}
}

func TestRecurringScheduleHandlersRejectMissingFields(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "enabled is missing",
			path: "/recurring-schedules/123/enabled",
			body: `{}`,
		},
		{
			name: "interval is missing",
			path: "/recurring-schedules/123/interval",
			body: `{"interval":""}`,
		},
		{
			name: "cron expression is missing",
			path: "/recurring-schedules/123/cron",
			body: `{"cron_expression":"","timezone":"Asia/Kolkata"}`,
		},
		{
			name: "timezone is missing",
			path: "/recurring-schedules/123/cron",
			body: `{"cron_expression":"*/5 * * * *","timezone":""}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mux := newRecurringScheduleHandlerTestMux()

			request := httptest.NewRequest(
				http.MethodPatch,
				tc.path,
				strings.NewReader(tc.body),
			)
			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf(
					"expected status %d, got %d; body: %s",
					http.StatusBadRequest,
					response.Code,
					response.Body.String(),
				)
			}
		})
	}
}

func TestRescheduleIntervalHandlerRejectsInvalidDuration(t *testing.T) {
	mux := newRecurringScheduleHandlerTestMux()

	request := httptest.NewRequest(
		http.MethodPatch,
		"/recurring-schedules/123/interval",
		strings.NewReader(`{"interval":"not-a-duration"}`),
	)
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status %d, got %d; body: %s",
			http.StatusBadRequest,
			response.Code,
			response.Body.String(),
		)
	}
}

func TestRecurringScheduleHandlersRejectWrongMethod(t *testing.T) {
	paths := []string{
		"/recurring-schedules/123/enabled",
		"/recurring-schedules/123/interval",
		"/recurring-schedules/123/cron",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			mux := newRecurringScheduleHandlerTestMux()

			request := httptest.NewRequest(
				http.MethodGet,
				path,
				nil,
			)
			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != http.StatusMethodNotAllowed {
				t.Fatalf(
					"expected status %d, got %d; body: %s",
					http.StatusMethodNotAllowed,
					response.Code,
					response.Body.String(),
				)
			}
		})
	}
}

func newRecurringScheduleHandlerTestMuxWithPool(
	pool *pgxpool.Pool,
) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc(
		"PATCH /recurring-schedules/{id}/enabled",
		recurringScheduleEnabledHandler(pool),
	)
	mux.HandleFunc(
		"PATCH /recurring-schedules/{id}/interval",
		rescheduleIntervalHandler(pool),
	)
	mux.HandleFunc(
		"PATCH /recurring-schedules/{id}/cron",
		rescheduleCronHandler(pool),
	)

	mux.HandleFunc(
		"DELETE /recurring-schedules/{id}",
		cancelRecurringScheduleHandler(pool),
	)

	return mux
}

func newRecurringScheduleHandlerTestPool(
	t *testing.T,
) *pgxpool.Pool {
	t.Helper()

	if err := godotenv.Load(); err != nil {
		t.Log(".env not found; using system environment variables")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}

	t.Cleanup(pool.Close)

	return pool
}

func createHandlerTestIntervalSchedule(
	t *testing.T,
	pool *pgxpool.Pool,
) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	name := fmt.Sprintf(
		"api-interval-test-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewRecurringSchedule(
		name,
		5*time.Minute,
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create interval schedule: %v", err)
	}

	if err := database.SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save interval schedule: %v", err)
	}

	var id int64
	err = pool.QueryRow(
		ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		name,
	).Scan(&id)
	if err != nil {
		t.Fatalf("get interval schedule ID: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, err := pool.Exec(
			cleanupCtx,
			`DELETE FROM recurring_schedules WHERE id = $1`,
			id,
		)
		if err != nil {
			t.Errorf("delete test interval schedule: %v", err)
		}
	})

	return id
}

func createHandlerTestCronSchedule(
	t *testing.T,
	pool *pgxpool.Pool,
) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	name := fmt.Sprintf(
		"api-cron-test-%d",
		time.Now().UnixNano(),
	)

	original, err := schedule.NewCronSchedule(
		name,
		"0 9 * * *",
		"Asia/Kolkata",
		time.Now().Add(24*time.Hour),
	)
	if err != nil {
		t.Fatalf("create cron schedule: %v", err)
	}

	if err := database.SaveRecurringSchedule(ctx, pool, original); err != nil {
		t.Fatalf("save cron schedule: %v", err)
	}

	var id int64
	err = pool.QueryRow(
		ctx,
		`SELECT id FROM recurring_schedules WHERE name = $1`,
		name,
	).Scan(&id)
	if err != nil {
		t.Fatalf("get cron schedule ID: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, err := pool.Exec(
			cleanupCtx,
			`DELETE FROM recurring_schedules WHERE id = $1`,
			id,
		)
		if err != nil {
			t.Errorf("delete test cron schedule: %v", err)
		}
	})

	return id
}

func TestRecurringScheduleEnabledHandler(t *testing.T) {
	pool := newRecurringScheduleHandlerTestPool(t)
	scheduleID := createHandlerTestIntervalSchedule(t, pool)
	mux := newRecurringScheduleHandlerTestMuxWithPool(pool)

	tests := []struct {
		name    string
		enabled bool
	}{
		{
			name:    "disable schedule",
			enabled: false,
		},
		{
			name:    "enable schedule",
			enabled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(
				`{"enabled":%t}`,
				tc.enabled,
			)

			request := httptest.NewRequest(
				http.MethodPatch,
				fmt.Sprintf(
					"/recurring-schedules/%d/enabled",
					scheduleID,
				),
				strings.NewReader(body),
			)
			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf(
					"expected status 200, got %d; body: %s",
					response.Code,
					response.Body.String(),
				)
			}

			assertJSONResponse(t, response)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			defer cancel()

			updated, err := database.GetRecurringSchedule(
				ctx,
				pool,
				scheduleID,
			)
			if err != nil {
				t.Fatalf("get updated schedule: %v", err)
			}

			if updated.Enabled != tc.enabled {
				t.Fatalf(
					"expected enabled=%v, got %v",
					tc.enabled,
					updated.Enabled,
				)
			}
		})
	}
}

func TestRescheduleIntervalHandler(t *testing.T) {
	pool := newRecurringScheduleHandlerTestPool(t)
	scheduleID := createHandlerTestIntervalSchedule(t, pool)
	mux := newRecurringScheduleHandlerTestMuxWithPool(pool)

	request := httptest.NewRequest(
		http.MethodPatch,
		fmt.Sprintf(
			"/recurring-schedules/%d/interval",
			scheduleID,
		),
		strings.NewReader(`{"interval":"15m"}`),
	)
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status 200, got %d; body: %s",
			response.Code,
			response.Body.String(),
		)
	}

	assertJSONResponse(t, response)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	updated, err := database.GetRecurringSchedule(
		ctx,
		pool,
		scheduleID,
	)
	if err != nil {
		t.Fatalf("get updated schedule: %v", err)
	}

	if updated.Interval != 15*time.Minute {
		t.Fatalf(
			"expected interval 15m, got %v",
			updated.Interval,
		)
	}
}

func TestRescheduleCronHandler(t *testing.T) {
	pool := newRecurringScheduleHandlerTestPool(t)
	scheduleID := createHandlerTestCronSchedule(t, pool)
	mux := newRecurringScheduleHandlerTestMuxWithPool(pool)

	request := httptest.NewRequest(
		http.MethodPatch,
		fmt.Sprintf(
			"/recurring-schedules/%d/cron",
			scheduleID,
		),
		strings.NewReader(
			`{"cron_expression":"*/5 * * * *","timezone":"Asia/Kolkata"}`,
		),
	)
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status 200, got %d; body: %s",
			response.Code,
			response.Body.String(),
		)
	}

	assertJSONResponse(t, response)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	updated, err := database.GetRecurringSchedule(
		ctx,
		pool,
		scheduleID,
	)
	if err != nil {
		t.Fatalf("get updated schedule: %v", err)
	}

	if updated.CronExpression != "*/5 * * * *" {
		t.Fatalf(
			"expected cron expression */5 * * * *, got %q",
			updated.CronExpression,
		)
	}

	if updated.Timezone != "Asia/Kolkata" {
		t.Fatalf(
			"expected timezone Asia/Kolkata, got %q",
			updated.Timezone,
		)
	}

	if !updated.NextRunAt.After(time.Now()) {
		t.Fatalf(
			"expected next run to be in the future, got %v",
			updated.NextRunAt,
		)
	}
}

func TestRecurringScheduleHandlersReturnNotFound(t *testing.T) {
	pool := newRecurringScheduleHandlerTestPool(t)
	mux := newRecurringScheduleHandlerTestMuxWithPool(pool)

	// Use a positive ID that is extremely unlikely to exist.
	missingID := time.Now().UnixNano()

	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "enable or disable missing schedule",
			path: fmt.Sprintf(
				"/recurring-schedules/%d/enabled",
				missingID,
			),
			body: `{"enabled":false}`,
		},
		{
			name: "reschedule missing interval schedule",
			path: fmt.Sprintf(
				"/recurring-schedules/%d/interval",
				missingID,
			),
			body: `{"interval":"15m"}`,
		},
		{
			name: "reschedule missing cron schedule",
			path: fmt.Sprintf(
				"/recurring-schedules/%d/cron",
				missingID,
			),
			body: `{"cron_expression":"*/5 * * * *","timezone":"Asia/Kolkata"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodPatch,
				tc.path,
				strings.NewReader(tc.body),
			)
			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != http.StatusNotFound {
				t.Fatalf(
					"expected status %d, got %d; body: %s",
					http.StatusNotFound,
					response.Code,
					response.Body.String(),
				)
			}
		})
	}
}

func TestRecurringScheduleHandlersRejectInvalidConfigurations(t *testing.T) {
	pool := newRecurringScheduleHandlerTestPool(t)
	mux := newRecurringScheduleHandlerTestMuxWithPool(pool)

	intervalID := createHandlerTestIntervalSchedule(t, pool)
	cronID := createHandlerTestCronSchedule(t, pool)

	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "interval shorter than one second",
			path: fmt.Sprintf(
				"/recurring-schedules/%d/interval",
				intervalID,
			),
			body: `{"interval":"500ms"}`,
		},
		{
			name: "interval with fractional seconds",
			path: fmt.Sprintf(
				"/recurring-schedules/%d/interval",
				intervalID,
			),
			body: `{"interval":"1500ms"}`,
		},
		{
			name: "interval endpoint used on cron schedule",
			path: fmt.Sprintf(
				"/recurring-schedules/%d/interval",
				cronID,
			),
			body: `{"interval":"15m"}`,
		},
		{
			name: "invalid cron expression",
			path: fmt.Sprintf(
				"/recurring-schedules/%d/cron",
				cronID,
			),
			body: `{"cron_expression":"not a cron expression","timezone":"Asia/Kolkata"}`,
		},
		{
			name: "invalid timezone",
			path: fmt.Sprintf(
				"/recurring-schedules/%d/cron",
				cronID,
			),
			body: `{"cron_expression":"*/5 * * * *","timezone":"Invalid/Timezone"}`,
		},
		{
			name: "cron endpoint used on interval schedule",
			path: fmt.Sprintf(
				"/recurring-schedules/%d/cron",
				intervalID,
			),
			body: `{"cron_expression":"*/5 * * * *","timezone":"Asia/Kolkata"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodPatch,
				tc.path,
				strings.NewReader(tc.body),
			)
			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf(
					"expected status %d, got %d; body: %s",
					http.StatusBadRequest,
					response.Code,
					response.Body.String(),
				)
			}
		})
	}
}

func assertJSONResponse(
	t *testing.T,
	response *httptest.ResponseRecorder,
) {
	t.Helper()

	contentType := response.Header().Get("Content-Type")
	if !strings.HasPrefix(contentType, "application/json") {
		t.Fatalf(
			"expected application/json content type, got %q",
			contentType,
		)
	}

	if !json.Valid(response.Body.Bytes()) {
		t.Fatalf(
			"expected valid JSON response, got %q",
			response.Body.String(),
		)
	}
}

func TestCancelRecurringScheduleHandler(t *testing.T) {
	pool := newRecurringScheduleHandlerTestPool(t)
	scheduleID := createHandlerTestIntervalSchedule(t, pool)
	mux := newRecurringScheduleHandlerTestMuxWithPool(pool)

	path := fmt.Sprintf(
		"/recurring-schedules/%d",
		scheduleID,
	)

	// First cancellation request.
	request := httptest.NewRequest(
		http.MethodDelete,
		path,
		nil,
	)
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status 200, got %d; body: %s",
			response.Code,
			response.Body.String(),
		)
	}

	assertJSONResponse(t, response)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	cancelled, err := database.GetRecurringSchedule(
		ctx,
		pool,
		scheduleID,
	)
	if err != nil {
		t.Fatalf("get cancelled schedule: %v", err)
	}

	if cancelled.Enabled {
		t.Fatal("expected schedule to be disabled")
	}

	if cancelled.CancelledAt == nil {
		t.Fatal("expected cancellation timestamp")
	}

	firstCancellation := *cancelled.CancelledAt

	// Repeated DELETE request must be safe.
	request = httptest.NewRequest(
		http.MethodDelete,
		path,
		nil,
	)
	response = httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected repeated cancellation to return 200, got %d; body: %s",
			response.Code,
			response.Body.String(),
		)
	}

	assertJSONResponse(t, response)

	cancelledAgain, err := database.GetRecurringSchedule(
		ctx,
		pool,
		scheduleID,
	)
	if err != nil {
		t.Fatalf("get schedule after repeated cancellation: %v", err)
	}

	if cancelledAgain.CancelledAt == nil {
		t.Fatal("expected cancellation timestamp to remain present")
	}

	if !cancelledAgain.CancelledAt.Equal(firstCancellation) {
		t.Fatalf(
			"cancellation timestamp changed: first=%v, second=%v",
			firstCancellation,
			*cancelledAgain.CancelledAt,
		)
	}

	if cancelledAgain.Enabled {
		t.Fatal("expected schedule to remain disabled")
	}
}

func TestCancelledRecurringScheduleCannotBeEnabledThroughAPI(t *testing.T) {
	pool := newRecurringScheduleHandlerTestPool(t)
	scheduleID := createHandlerTestIntervalSchedule(t, pool)
	mux := newRecurringScheduleHandlerTestMuxWithPool(pool)

	// Permanently cancel the schedule through the DELETE endpoint.
	cancelRequest := httptest.NewRequest(
		http.MethodDelete,
		fmt.Sprintf("/recurring-schedules/%d", scheduleID),
		nil,
	)
	cancelResponse := httptest.NewRecorder()

	mux.ServeHTTP(cancelResponse, cancelRequest)

	if cancelResponse.Code != http.StatusOK {
		t.Fatalf(
			"expected cancellation status 200, got %d; body: %s",
			cancelResponse.Code,
			cancelResponse.Body.String(),
		)
	}

	// Attempt to re-enable the permanently cancelled schedule.
	enableRequest := httptest.NewRequest(
		http.MethodPatch,
		fmt.Sprintf(
			"/recurring-schedules/%d/enabled",
			scheduleID,
		),
		strings.NewReader(`{"enabled":true}`),
	)
	enableResponse := httptest.NewRecorder()

	mux.ServeHTTP(enableResponse, enableRequest)

	if enableResponse.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status 400, got %d; body: %s",
			http.StatusBadRequest,
			enableResponse.Body.String(),
		)
	}

	// Verify the database still contains the permanent cancellation.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	saved, err := database.GetRecurringSchedule(
		ctx,
		pool,
		scheduleID,
	)
	if err != nil {
		t.Fatalf("get cancelled schedule: %v", err)
	}

	if saved.Enabled {
		t.Fatal("cancelled schedule must remain disabled")
	}

	if saved.CancelledAt == nil {
		t.Fatal("expected cancellation timestamp to remain present")
	}
}

func TestCancelRecurringScheduleHandlerErrorCases(t *testing.T) {
	pool := newRecurringScheduleHandlerTestPool(t)
	mux := newRecurringScheduleHandlerTestMuxWithPool(pool)

	missingID := time.Now().UnixNano()

	tests := []struct {
		name           string
		method         string
		path           string
		expectedStatus int
	}{
		{
			name:           "invalid schedule ID",
			method:         http.MethodDelete,
			path:           "/recurring-schedules/not-a-number",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:   "nonexistent schedule",
			method: http.MethodDelete,
			path: fmt.Sprintf(
				"/recurring-schedules/%d",
				missingID,
			),
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "incorrect HTTP method",
			method:         http.MethodGet,
			path:           "/recurring-schedules/123",
			expectedStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(
				tc.method,
				tc.path,
				nil,
			)
			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != tc.expectedStatus {
				t.Fatalf(
					"expected status %d, got %d; body: %s",
					tc.expectedStatus,
					response.Code,
					response.Body.String(),
				)
			}
		})
	}
}
