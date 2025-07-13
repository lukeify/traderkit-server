package utils

import (
	"time"
)

// LastRetainedDay returns the time.Time in UTC that represents the start of the last day in Eastern Time that should
// have aggregate bars retained for.
func LastRetainedDay(now time.Time, n int) time.Time {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		panic(err)
	}

	i := 0
	today := truncateToLocationDay(now.In(loc))
	curr := today

	for i < n {
		curr = curr.AddDate(0, 0, -1)
		if curr.Weekday() != time.Saturday && curr.Weekday() != time.Sunday {
			i++
		}
	}

	return curr.UTC()
}

func truncateToLocationDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
