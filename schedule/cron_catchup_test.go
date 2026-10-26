package schedule

import (
	"testing"
	"time"
)

func TestCalculateCronCatchUpSkip(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}

	nextRunAt := time.Date(2026, 10, 21, 9, 0, 0, 0, loc)
	now := time.Date(2026, 10, 23, 11, 0, 0, 0, loc)

	result, err := CalculateCronCatchUp(
		"0 9 * * *",
		"Asia/Kolkata",
		nextRunAt,
		now,
		CatchUpSkip,
		100,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Occurrences) != 0 {
		t.Fatalf("expected 0 occurrences, got %d", len(result.Occurrences))
	}

	expected := time.Date(2026, 10, 24, 9, 0, 0, 0, loc)

	if !result.NextRunAt.Equal(expected) {
		t.Fatalf("expected next run %v, got %v",
			expected, result.NextRunAt)
	}
}

func TestCalculateCronCatchUpFireOnce(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}

	nextRunAt := time.Date(2026, 10, 21, 9, 0, 0, 0, loc)
	now := time.Date(2026, 10, 23, 11, 0, 0, 0, loc)

	result, err := CalculateCronCatchUp(
		"0 9 * * *",
		"Asia/Kolkata",
		nextRunAt,
		now,
		CatchUpFireOnce,
		100,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Occurrences) != 1 {
		t.Fatalf("expected 1 occurrence, got %d", len(result.Occurrences))
	}

	expectedOccurrence := time.Date(
		2026, 10, 21, 9, 0, 0, 0, loc,
	)

	if !result.Occurrences[0].Equal(expectedOccurrence) {
		t.Fatalf("expected occurrence %v, got %v",
			expectedOccurrence, result.Occurrences[0])
	}

	expectedNext := time.Date(
		2026, 10, 24, 9, 0, 0, 0, loc,
	)

	if !result.NextRunAt.Equal(expectedNext) {
		t.Fatalf("expected next run %v, got %v",
			expectedNext, result.NextRunAt)
	}
}

func TestCalculateCronCatchUpFireAll(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}

	nextRunAt := time.Date(2026, 10, 21, 9, 0, 0, 0, loc)
	now := time.Date(2026, 10, 23, 11, 0, 0, 0, loc)

	result, err := CalculateCronCatchUp(
		"0 9 * * *",
		"Asia/Kolkata",
		nextRunAt,
		now,
		CatchUpFireAll,
		100,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Occurrences) != 3 {
		t.Fatalf("expected 3 occurrences, got %d", len(result.Occurrences))
	}

	expected := []time.Time{
		time.Date(2026, 10, 21, 9, 0, 0, 0, loc),
		time.Date(2026, 10, 22, 9, 0, 0, 0, loc),
		time.Date(2026, 10, 23, 9, 0, 0, 0, loc),
	}

	for i := range expected {
		if !result.Occurrences[i].Equal(expected[i]) {
			t.Fatalf(
				"occurrence %d: expected %v, got %v",
				i,
				expected[i],
				result.Occurrences[i],
			)
		}
	}

	expectedNext := time.Date(
		2026, 10, 24, 9, 0, 0, 0, loc,
	)

	if !result.NextRunAt.Equal(expectedNext) {
		t.Fatalf("expected next run %v, got %v",
			expectedNext, result.NextRunAt)
	}
}

func TestCalculateCronCatchUpFireAllRespectsLimit(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}

	nextRunAt := time.Date(2026, 10, 20, 9, 0, 0, 0, loc)
	now := time.Date(2026, 10, 26, 11, 0, 0, 0, loc)

	result, err := CalculateCronCatchUp(
		"0 9 * * *",
		"Asia/Kolkata",
		nextRunAt,
		now,
		CatchUpFireAll,
		3,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Occurrences) != 3 {
		t.Fatalf("expected 3 occurrences, got %d", len(result.Occurrences))
	}

	expected := []time.Time{
		time.Date(2026, 10, 20, 9, 0, 0, 0, loc),
		time.Date(2026, 10, 21, 9, 0, 0, 0, loc),
		time.Date(2026, 10, 22, 9, 0, 0, 0, loc),
	}

	for i := range expected {
		if !result.Occurrences[i].Equal(expected[i]) {
			t.Fatalf(
				"occurrence %d: expected %v, got %v",
				i,
				expected[i],
				result.Occurrences[i],
			)
		}
	}

	// The remaining backlog must stay eligible.
	expectedNext := time.Date(
		2026, 10, 23, 9, 0, 0, 0, loc,
	)

	if !result.NextRunAt.Equal(expectedNext) {
		t.Fatalf("expected next run %v, got %v",
			expectedNext, result.NextRunAt)
	}
}
