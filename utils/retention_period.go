package utils

import (
	"fmt"
	"os"
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
		if IsMarketOpenOnDay(curr) {
			i++
		}
	}

	return curr.UTC()
}

// IsMarketOpenOnDay checks if the given time.Time instance is neither a weekend nor a market holiday, thus data is
// assumed to be present for the given time.Time's date if `true` is returned.
func IsMarketOpenOnDay(t time.Time) bool {
	return t.Weekday() != time.Saturday && t.Weekday() != time.Sunday && !IsMarketHoliday(t)
}

// IsMarketHoliday checks if the given time.Time instance is on the same date as any of the listed market holidays in
// the `holidays` slice. This data is sourced manually from https://www.nasdaq.com/market-activity/stock-market-holiday-schedule
// and should be updated annually. Note that early close dates are not considered holidays.
func IsMarketHoliday(t time.Time) bool {
	holidays := []string{
		"01 January 2025",
		"20 January 2025",
		"17 February 2025",
		"18 April 2025",
		"26 May 2025",
		"19 June 2025",
		"04 July 2025",
		"01 September 2025",
		"27 November 2025",
		"25 December 2025",
	}

	for _, h := range holidays {
		ht, err := time.ParseInLocation("02 January 2006", h, t.Location())
		if err != nil {
			fmt.Printf("Unable to parse holiday date %s\n", h)
			os.Exit(1)
		}
		if t.Year() == ht.Year() && t.Month() == ht.Month() && t.Day() == ht.Day() {
			return true
		}
	}

	return false
}

func truncateToLocationDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
