package ohlcv

import (
	"context"
	"fmt"
	"time"
	"traderkit-server/utils/progress_printer"
)

type Metrics struct {
	currentSource string
	currentTicker string
	rows          uint64
	skippedRows   uint64
}

func (m *Metrics) SetSource(source string) {
	m.currentSource = source
}

func (m *Metrics) IngestRow(ticker string) {
	m.currentTicker = ticker
	m.rows++
}

func (m *Metrics) SkipRow() {
	m.skippedRows++
}

func (m *Metrics) SetError(reason string) {

}

func (m *Metrics) Print(pp *progress_printer.ProgressPrinter) {
	pp.Update(fmt.Sprintf(
		"[%s] %d bars read, %d bars skipped (current ticker: %s)",
		m.currentSource,
		m.rows,
		m.skippedRows,
		m.currentTicker,
	))
}

func (m *Metrics) StartPrinting(ctx context.Context, pp *progress_printer.ProgressPrinter) {
	t := time.NewTicker(100 * time.Millisecond)

	go func() {
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.Print(pp)
			}
		}
	}()
}
