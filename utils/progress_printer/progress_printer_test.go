package progress_printer

import (
	"bytes"
	"strings"
	"testing"
)

// TestProgressPrinter_UpdateIncreasesMaxLength ensures that if a longer message is printed after a shorter one,
// the max length is updated accordingly.
func TestProgressPrinter_UpdateIncreasesMaxLength(t *testing.T) {
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)

	for _, msg := range []string{"Short", "This is a longer message"} {
		pp.Update(msg)
		if pp.max != len(msg) {
			t.Errorf("After Update(%q), max = %d; want %d", msg, pp.max, len(msg))
		}
	}
}

// TestProgressPrinter_UpdateRetainsMaxLength ensures that if a shorter message is printed after a longer one, the max
// length remains the length of the original longer message. This ensures that any previous prints are fully cleared.
func TestProgressPrinter_UpdateRetainsMaxLength(t *testing.T) {
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)

	lMsg := "This is a longer message"
	pp.Update(lMsg)
	initialMax := pp.max

	sMsg := "Short"
	pp.Update(sMsg)

	// Max should remain at the length of the longest message
	if pp.max != initialMax {
		t.Errorf("After Update(%q), max = %d; want %d (the max should not decrease)", sMsg, pp.max, initialMax)
	}
}

// TestProgressPrinter_UpdatePrintsOutput tests that the buffer provided to `ProgressPrinter` appends additional
// messages followed by an ending carriage return.
func TestProgressPrinter_UpdatePrintsOutput(t *testing.T) {
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)

	pp.Update("First")
	out := buf.String()
	if out != "First\r" {
		t.Errorf("Expected output to contain 'First', got: %q", out)
	}

	pp.Update("Second message")
	out = buf.String()
	if out != "First\rSecond message\r" {
		t.Errorf("Expected output to contain 'Second message', got: %q", out)
	}
}

// TestProgressPrinter_CompletePrintsOutput tests that the buffer provided to `ProgressPrinter` contains a newline
// at the end of the string once `.Complete` is called.
func TestProgressPrinter_CompletePrintsOutput(t *testing.T) {
	var buf bytes.Buffer
	NewProgressPrinter(&buf).Complete("Done")
	out := buf.String()

	// Should contain the message
	if !strings.HasSuffix(out, "Done\r\n") {
		t.Errorf("Expected output to contain 'Done', got: %q", out)
	}
}

// TestProgressPrinter_PreviousUpdatesAreOverwritten ensures that if a short message appears after a long message,
// a sufficient number of spaces are printed to clear the previous content.
func TestProgressPrinter_PreviousUpdatesAreOverwritten(t *testing.T) {
	var buf bytes.Buffer
	pp := NewProgressPrinter(&buf)

	pp.Update("Longer message")
	pp.Complete("Short")
	out := buf.String()

	// Should contain spaces to clear previous content
	if !strings.Contains(out, strings.Repeat(" ", max(0, len("Longer message")-len("Short")))) {
		t.Errorf("Expected output to contain spaces for clearing, got: %q", out)
	}

	// Should end with a newline
	if !strings.HasSuffix(strings.TrimRight(out, "\r"), "\n") {
		t.Errorf("Expected output to end with newline, got: %q", out)
	}
}
