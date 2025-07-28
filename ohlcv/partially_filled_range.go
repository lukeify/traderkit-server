package ohlcv

import "time"

// partiallyFilledRange represents the earliest and latest timestamps in the OHLCV database where bars before
// `FilledBefore` have definitely been filled, and bars after `UnfilledAfter` have never been filled.
type partiallyFilledRange struct {
	FilledBefore  *time.Time
	UnfilledAfter *time.Time
}

// Contains will return `true` if the given `time.Time` is within the range of timestamps contained within the struct,
// and false otherwise.
func (ir *partiallyFilledRange) Contains(t time.Time) bool {
	if ir.FilledBefore != nil && t.Compare(*ir.FilledBefore) >= 0 {
		return true
	}

	if ir.UnfilledAfter != nil && t.Compare(*ir.UnfilledAfter) <= 0 {
		return true
	}

	return false
}

func (ir *partiallyFilledRange) Exists() bool {
	return ir.FilledBefore != nil && ir.UnfilledAfter != nil
}
