package utils

import (
	"testing"
	"time"
)

// TestGetLastRetainedDay_IsAThursdayIfGivenASunday. If the current day is a Sunday, and two business days are retained,
// then it should be expected that Thursday and Friday are retained, as Saturday and Sunday are ignored.
func TestGetLastRetainedDay_IsAThursdayIfGivenASunday(t *testing.T) {
	now := time.Date(2025, 7, 13, 0, 0, 0, 0, time.UTC)
	expected := time.Date(2025, 7, 10, 0, 0, 0, 0, time.UTC) // Thursday before the weekend
	result := GetLastRetainedDay(now, 2)

	if !result.Equal(expected) {
		t.Errorf("Expected %v but got %v", expected, result)
	}
}

// TestGetLastRetainedDay_IsAWednesdayIfGivenAFriday. If the current day is the middle of Friday, and two business days
// are retained, then we expect Wednesday and Thursday are trained, as Friday is not complete yet.
func TestGetLastRetainedDay_IsAWednesdayIfGivenAFriday(t *testing.T) {
	now := time.Date(2025, 7, 11, 12, 0, 0, 0, time.UTC)
	expected := time.Date(2025, 7, 9, 0, 0, 0, 0, time.UTC) // Friday before the weekend
	result := GetLastRetainedDay(now, 2)

	if !result.Equal(expected) {
		t.Errorf("Expected %v but got %v", expected, result)
	}
}
