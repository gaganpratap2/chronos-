package schedule

import (
	"testing"
	"time"
)

func TestCalculateCatchUpSkip(t *testing.T) {
	nextRunAt := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 1, 10, 23, 0, 0, time.UTC)

	result := CalculateCatchUp(
		nextRunAt,
		5*time.Minute,
		now,
		CatchUpSkip,
		100,
	)

	if len(result.Occurrences) != 0 {
		t.Fatalf("expected 0 occurrences, got %d", len(result.Occurrences))
	}

	expectedNext := time.Date(2026, 1, 1, 10, 25, 0, 0, time.UTC)

	if !result.NextRunAt.Equal(expectedNext) {
		t.Fatalf(
			"expected next run at %v, got %v",
			expectedNext,
			result.NextRunAt,
		)
	}
}

func TestCalculateCatchUpFireOnce(t *testing.T) {
	nextRunAt := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 1, 10, 23, 0, 0, time.UTC)

	result := CalculateCatchUp(
		nextRunAt,
		5*time.Minute,
		now,
		CatchUpFireOnce,
		100,
	)

	if len(result.Occurrences) != 1 {
		t.Fatalf("expected 1 occurrence, got %d", len(result.Occurrences))
	}

	if !result.Occurrences[0].Equal(nextRunAt) {
		t.Fatalf(
			"expected occurrence %v, got %v",
			nextRunAt,
			result.Occurrences[0],
		)
	}

	expectedNext := time.Date(2026, 1, 1, 10, 25, 0, 0, time.UTC)

	if !result.NextRunAt.Equal(expectedNext) {
		t.Fatalf(
			"expected next run at %v, got %v",
			expectedNext,
			result.NextRunAt,
		)
	}
}

func TestCalculateCatchUpFireAllRespectsLimit(t *testing.T) {
	nextRunAt := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 1, 10, 23, 0, 0, time.UTC)

	result := CalculateCatchUp(
		nextRunAt,
		5*time.Minute,
		now,
		CatchUpFireAll,
		2,
	)

	if len(result.Occurrences) != 2 {
		t.Fatalf("expected 2 occurrences, got %d", len(result.Occurrences))
	}

	if !result.Occurrences[0].Equal(nextRunAt) {
		t.Fatalf("unexpected first occurrence: %v", result.Occurrences[0])
	}

	expectedSecond := nextRunAt.Add(5 * time.Minute)

	if !result.Occurrences[1].Equal(expectedSecond) {
		t.Fatalf(
			"expected second occurrence %v, got %v",
			expectedSecond,
			result.Occurrences[1],
		)
	}

	expectedNext := nextRunAt.Add(10 * time.Minute)

	if !result.NextRunAt.Equal(expectedNext) {
		t.Fatalf(
			"expected next run at %v, got %v",
			expectedNext,
			result.NextRunAt,
		)
	}
}

func TestCalculateCatchUpNothingDue(t *testing.T) {
	nextRunAt := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	now := time.Date(2026, 1, 1, 10, 23, 0, 0, time.UTC)

	result := CalculateCatchUp(
		nextRunAt,
		5*time.Minute,
		now,
		CatchUpFireAll,
		100,
	)

	if len(result.Occurrences) != 0 {
		t.Fatalf("expected 0 occurrences, got %d", len(result.Occurrences))
	}

	if !result.NextRunAt.Equal(nextRunAt) {
		t.Fatalf(
			"expected next run at %v, got %v",
			nextRunAt,
			result.NextRunAt,
		)
	}
}
