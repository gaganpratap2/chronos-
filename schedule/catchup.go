package schedule

import "time"

type CatchUpResult struct {
	Occurrences []time.Time
	NextRunAt   time.Time
}

func CalculateCatchUp(nextRunAt time.Time, interval time.Duration, now time.Time, policy CatchUpPolicy, maxCatchUp int) CatchUpResult {

	result := CatchUpResult{
		NextRunAt: nextRunAt,
	}

	if nextRunAt.After(now) {
		return result
	}

	switch policy {
	case CatchUpSkip:
		for !result.NextRunAt.After(now) {
			result.NextRunAt = result.NextRunAt.Add(interval)
		}

	case CatchUpFireOnce:
		result.Occurrences = append(
			result.Occurrences,
			result.NextRunAt,
		)

		for !result.NextRunAt.After(now) {
			result.NextRunAt = result.NextRunAt.Add(interval)
		}

	case CatchUpFireAll:
		if maxCatchUp <= 0 {
			return result
		}

		for !result.NextRunAt.After(now) && len(result.Occurrences) < maxCatchUp {
			result.Occurrences = append(
				result.Occurrences,
				result.NextRunAt,
			)

			result.NextRunAt = result.NextRunAt.Add(interval)
		}
	}
	return result
}
