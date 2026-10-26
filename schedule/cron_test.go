package schedule

import (
	"testing"
	"time"
)

func TestNextCronOccurrence(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}

	from := time.Date(
		2026,
		10,
		26,
		10,
		0,
		0,
		0,
		loc,
	)

	next, err := NextCronOccurrence(
		"0 9 * * *",
		"Asia/Kolkata",
		from,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := time.Date(
		2026,
		10,
		27,
		9,
		0,
		0,
		0,
		loc,
	)

	if !next.Equal(expected) {
		t.Fatalf(
			"expected next occurrence %v, got %v",
			expected,
			next,
		)
	}
}

func TestNextCronOccurrenceRespectsTimezone(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}

	from := time.Date(
		2026,
		11,
		1,
		1,
		0,
		0,
		0,
		loc,
	)

	next, err := NextCronOccurrence(
		"0 9 * * *",
		"America/New_York",
		from,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if next.Hour() != 9 {
		t.Fatalf(
			"expected local execution at 09:00, got %v",
			next,
		)
	}

	if next.Location().String() != "America/New_York" {
		t.Fatalf(
			"expected timezone America/New_York, got %v",
			next.Location(),
		)
	}
}
