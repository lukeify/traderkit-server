package progress_printer

import (
	"fmt"
	"io"
	"strings"
)

// ProgressPrinter is a utility for printing progress messages that overwrite previous messages in the terminal.
type ProgressPrinter struct {
	w   io.Writer // The writer to which messages are printed
	max int       // Tracks the maximum line length that's been printed
}

func NewProgressPrinter(w io.Writer) *ProgressPrinter {
	return &ProgressPrinter{max: 0, w: w}
}

// Update prints a progress message that overwrites the previous message.
// It keeps track of the maximum line length to ensure proper clearing of previous content.
func (p *ProgressPrinter) Update(message string) {
	// Clear the previous line by printing spaces
	_, _ = fmt.Fprint(p.w, message+strings.Repeat(" ", max(0, p.max-len(message)))+"\r")

	// Update the max length if this message is longer
	if len(message) > p.max {
		p.max = len(message)
	}
}

// Complete prints a final message and adds a newline. Use this when the progress is complete, and you want to move to
// the next line.
func (p *ProgressPrinter) Complete(message string) {
	p.Update(message)
	_, _ = fmt.Fprintln(p.w)
}
