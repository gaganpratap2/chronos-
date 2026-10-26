package schedule
 import (
	"time"
 )

 type CronCatchUpResult struct {
	Occurrences []time.Time
	NextRunAt   time.Time
 }

 func CalculateCronCatchUp(expression string , timezone string , nextRunAt time.Time , now time.Time , policy CatchUpPolicy , maxCatchUp int) (CronCatchUpResult , error) {

	var result CronCatchUpResult

	loc , err := time.LoadLocation(timezone)
	if err != nil {
		return result , err
	}

	current := nextRunAt.In(loc)
	currentNow := now.In(loc)

	if current.After(currentNow) {
		result.NextRunAt = current.UTC()
		return result , nil
	}

	switch policy{
	case CatchUpSkip :
		for !current.After(currentNow){
			current , err = NextCronOccurrence(expression, timezone, current)
			if err != nil {
				return result , err
		}
	}

	case CatchUpFireOnce:
		result.Occurrences = append(
			result.Occurrences,
			current.UTC(),
		)

		for !current.After(currentNow) {
			current, err = NextCronOccurrence(expression, timezone, current)
			if err != nil {
				return result, err
			}
		}

	case CatchUpFireAll:
		if maxCatchUp <= 0 {
			result.NextRunAt = current.UTC()
			return result, nil
		}

		for !current.After(currentNow) &&
			len(result.Occurrences) < maxCatchUp {

			result.Occurrences = append(
				result.Occurrences,
				current.UTC(),
			)

			current, err = NextCronOccurrence( expression, timezone, current )
			if err != nil {
				return result, err
			}
		}

	default:
		result.NextRunAt = current.UTC()
		return result, nil
	}

	result.NextRunAt = current.UTC()

	return result, nil
}
