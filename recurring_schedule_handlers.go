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
