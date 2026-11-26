package capture_test

import (
	"context"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/capture"
	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

func TestControllerDynamicToggle(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		SpoolDir: tmpDir,
		Capture: config.CaptureConfig{
			Interfaces: []string{"lo"},
			UDPPorts:   []int{2055, 514},
			Mode:       "socket",
			HTTP: config.HTTPConfig{
				Enabled:    false,
				ListenAddr: "127.0.0.1:0",
			},
			SyslogTCP: config.TCPSyslogConfig{
				Enabled:    false,
				ListenAddr: "127.0.0.1:0",
			},
		},
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sockListener := capture.NewUDPSocketListener([]int{0}, "127.0.0.1", store, 10*time.Millisecond)
	_ = sockListener.Start(ctx)

	httpSpooler := capture.NewHTTPSpooler(cfg.Capture.HTTP, store)
	syslogTCP := capture.NewTCPSyslogSpooler(cfg.Capture.SyslogTCP, store)

	ctrl := capture.NewController(cfg, store, sockListener, httpSpooler, syslogTCP)
	defer ctrl.Stop()

	// Initial status: HTTP and SyslogTCP should be disabled; Traffic and SyslogUDP enabled
	statuses := ctrl.Status()
	if len(statuses) != 4 {
		t.Fatalf("expected 4 statuses, got %d", len(statuses))
	}
	for _, s := range statuses {
		switch s.Name {
		case "traffic", "syslog_udp":
			if !s.Enabled {
				t.Errorf("%s expected enabled initially", s.Name)
			}
		case "syslog_tcp", "http":
			if s.Enabled {
				t.Errorf("%s expected disabled initially", s.Name)
			}
		}
	}

	// Dynamic Enable syslog_tcp
	if err := ctrl.Enable("syslog_tcp"); err != nil {
		t.Fatalf("Enable syslog_tcp failed: %v", err)
	}
	if !syslogTCP.IsEnabled() {
		t.Errorf("expected syslogTCP to be enabled")
	}

	// Dynamic Disable syslog_tcp
	if err := ctrl.Disable("syslog_tcp"); err != nil {
		t.Fatalf("Disable syslog_tcp failed: %v", err)
	}
	if syslogTCP.IsEnabled() {
		t.Errorf("expected syslogTCP to be disabled")
	}

	// Dynamic Enable all
	if err := ctrl.Enable("all"); err != nil {
		t.Fatalf("Enable all failed: %v", err)
	}
	statusesAll := ctrl.Status()
	for _, s := range statusesAll {
		if !s.Enabled {
			t.Errorf("expected %s to be enabled after Enable('all')", s.Name)
		}
	}

	// Dynamic Disable traffic
	if err := ctrl.Disable("traffic"); err != nil {
		t.Fatalf("Disable traffic failed: %v", err)
	}
	if !sockListener.IsTrafficPaused() {
		t.Errorf("expected traffic to be paused")
	}

	// Dynamic Disable all
	if err := ctrl.Disable("all"); err != nil {
		t.Fatalf("Disable all failed: %v", err)
	}
	statusesOff := ctrl.Status()
	for _, s := range statusesOff {
		if s.Enabled {
			t.Errorf("expected %s to be disabled after Disable('all')", s.Name)
		}
	}

	// Rapid toggle of HTTP to verify zero race conditions on s.server re-binding
	for i := 0; i < 50; i++ {
		_ = ctrl.Enable("http")
		_ = ctrl.Disable("http")
	}
}
