package utils

import (
	"time"
)

func GetLastRetainedDay(now time.Time, n int) time.Time {
	i := 0
	today := now.UTC().Truncate(24 * time.Hour)
	curr := today

	for i < n {
		curr = curr.AddDate(0, 0, -1)
		if curr.Weekday() != time.Saturday && curr.Weekday() != time.Sunday {
			i++
		}
	}
	return curr
}
