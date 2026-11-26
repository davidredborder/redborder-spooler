package replay_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/redborder/redborder-spooler/pkg/replay"
)

func TestProgressBarRendering(t *testing.T) {
	var buf bytes.Buffer
	pb := replay.NewProgressBar(&buf, 1000, 10.0)

	pb.Update(500)
	pb.Finish(nil)

	output := buf.String()
	if !strings.Contains(output, "Replaying") {
		t.Fatalf("expected output to contain 'Replaying', got: %s", output)
	}
	if !strings.Contains(output, "100.0%") {
		t.Fatalf("expected finish output to contain '100.0%%', got: %s", output)
	}
	if !strings.Contains(output, "10.0x") {
		t.Fatalf("expected output to contain speed '10.0x', got: %s", output)
	}
}

func TestProgressBarOverflowAndUnit(t *testing.T) {
	var buf bytes.Buffer
	pb := replay.NewProgressBar(&buf, 100, 5.0)
	pb.SetUnit("msgs")

	// Update beyond estimated 100 (overflow)
	pb.Update(150)
	pb.Finish(nil)

	output := buf.String()
	if !strings.Contains(output, "msgs") {
		t.Fatalf("expected output to contain unit 'msgs', got: %s", output)
	}
	if strings.Contains(output, "150 / 100") {
		t.Fatalf("progress bar should not display overflow like 150 / 100, got: %s", output)
	}
	if !strings.Contains(output, "150 / 150 msgs") {
		t.Fatalf("expected output to adjust total to 150 / 150 msgs, got: %s", output)
	}
}
