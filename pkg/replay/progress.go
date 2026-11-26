package replay

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// ProgressBar displays an interactive single-line terminal progress bar during replay.
type ProgressBar struct {
	mu           sync.Mutex
	writer       io.Writer
	totalPackets uint64
	current      uint64
	speed        float64
	follow       bool
	lastCapture  time.Time
	startTime    time.Time
	lastRender   time.Time
	width        int
	unit         string
	finished     bool
}

// NewProgressBar constructs a new ProgressBar.
func NewProgressBar(writer io.Writer, totalPackets uint64, speed float64) *ProgressBar {
	if writer == nil {
		writer = os.Stdout
	}
	return &ProgressBar{
		writer:       writer,
		totalPackets: totalPackets,
		speed:        speed,
		startTime:    time.Now(),
		width:        26,
	}
}

// SetFollow enables or disables continuous follow mode display.
func (p *ProgressBar) SetFollow(follow bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.follow = follow
}

// SetUnit sets the item unit label displayed in the progress bar (e.g. "pkts", "msgs", "reqs").
func (p *ProgressBar) SetUnit(unit string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unit = unit
}

// Update notifies the progress bar of the current replayed packet count.
func (p *ProgressBar) Update(packets uint64) {
	p.UpdateWithTime(packets, time.Time{})
}

// UpdateWithTime notifies the progress bar of the packet count and capture time for live lag tracking.
func (p *ProgressBar) UpdateWithTime(packets uint64, captureTime time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.current = packets
	if !captureTime.IsZero() {
		p.lastCapture = captureTime
	}
	now := time.Now()
	if !p.lastRender.IsZero() && now.Sub(p.lastRender) < 150*time.Millisecond {
		return
	}
	p.lastRender = now
	p.render(false)
}

// Finish marks the progress bar complete and prints the final summary line.
func (p *ProgressBar) Finish(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.finished {
		return
	}
	p.finished = true

	if err != nil {
		fmt.Fprintf(p.writer, "\nReplay stopped: %v\n", err)
		return
	}
	p.render(true)
}

func (p *ProgressBar) render(done bool) {
	unit := p.unit
	if unit == "" {
		unit = "pkts"
	}

	totalDisplay := p.totalPackets
	if p.current > totalDisplay {
		totalDisplay = p.current
	}

	pct := 0.0
	if totalDisplay > 0 {
		pct = float64(p.current) / float64(totalDisplay)
		if pct > 1.0 {
			pct = 1.0
		}
	} else if done {
		pct = 1.0
	}

	filled := int(pct * float64(p.width))
	if filled > p.width {
		filled = p.width
	}
	empty := p.width - filled

	bar := strings.Repeat("=", filled)
	if !done && filled > 0 && empty > 0 {
		bar = bar[:filled-1] + ">"
	}
	spaces := strings.Repeat(" ", empty)

	elapsed := time.Since(p.startTime).Truncate(time.Second)
	elapsedStr := fmt.Sprintf("%02d:%02d", int(elapsed.Minutes()), int(elapsed.Seconds())%60)

	speedStr := fmt.Sprintf("%.1fx", p.speed)
	if p.speed == 0 {
		speedStr = "burst"
	}

	if p.follow {
		lagStr := "--:--"
		if !p.lastCapture.IsZero() {
			lag := time.Since(p.lastCapture).Truncate(time.Second)
			if lag < 2*time.Second {
				lagStr = "00:00 (live)"
			} else {
				lagStr = fmt.Sprintf("%02d:%02d", int(lag.Minutes()), int(lag.Seconds())%60)
			}
		}
		if done {
			fmt.Fprintf(p.writer, "\r\033[KReplaying [FOLLOWING] %d %s | %s | Finished in %s\n",
				p.current, unit, speedStr, elapsedStr)
		} else {
			fmt.Fprintf(p.writer, "\r\033[KReplaying [FOLLOWING] %d %s | Lag: %s | %s | Elapsed: %s",
				p.current, unit, lagStr, speedStr, elapsedStr)
		}
		return
	}

	etaStr := "--:--"
	if pct > 0.02 && !done {
		totalEst := time.Duration(float64(time.Since(p.startTime)) / pct)
		remain := totalEst - time.Since(p.startTime)
		if remain > 0 {
			etaStr = fmt.Sprintf("%02d:%02d", int(remain.Minutes()), int(remain.Seconds())%60)
		}
	}

	if done {
		fmt.Fprintf(p.writer, "\r\033[KReplaying [%s] 100.0%% | %d / %d %s | %s | Finished in %s\n",
			strings.Repeat("=", p.width), p.current, totalDisplay, unit, speedStr, elapsedStr)
	} else {
		fmt.Fprintf(p.writer, "\r\033[KReplaying [%s%s] %5.1f%% | %d / %d %s | %s | Elapsed: %s | ETA: %s",
			bar, spaces, pct*100, p.current, totalDisplay, unit, speedStr, elapsedStr, etaStr)
	}
}
