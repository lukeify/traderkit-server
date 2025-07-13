package utils

import (
	"testing"
	"time"
)

// TestLastRetainedDay_IsAThursdayIfGivenASunday. If the current day is a Sunday, and two business days are retained,
// then it should be expected that Thursday and Friday are retained, as Saturday and Sunday are ignored.
func TestLastRetainedDay_IsAThursdayIfGivenASunday(t *testing.T) {
	now := time.Date(2025, 7, 13, 0, 0, 0, 0, time.UTC)
	expected := time.Date(2025, 7, 10, 4, 0, 0, 0, time.UTC) // Thursday before the weekend, in UTC.
	result := LastRetainedDay(now, 2)

	if !result.Equal(expected) {
		t.Errorf("Expected %v but got %v", expected, result)
	}
}

// TestLastRetainedDay_IsAWednesdayIfGivenAFriday. If the current day is the middle of Friday, and two business days
// are retained, then we expect Wednesday and Thursday are trained, as Friday is not complete yet.
func TestLastRetainedDay_IsAWednesdayIfGivenAFriday(t *testing.T) {
	now := time.Date(2025, 7, 11, 12, 0, 0, 0, time.UTC)
	expected := time.Date(2025, 7, 9, 4, 0, 0, 0, time.UTC) // Friday before the weekend
	result := LastRetainedDay(now, 2)

	if !result.Equal(expected) {
		t.Errorf("Expected %v but got %v", expected, result)
	}
}
