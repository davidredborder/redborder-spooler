package control_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/capture"
	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/control"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

func TestControlSocketServerAndClient(t *testing.T) {
	tmpDir := t.TempDir()
	sockPath := filepath.Join(tmpDir, "test-control.sock")

	cfg := &config.Config{
		SpoolDir:      tmpDir,
		ControlSocket: sockPath,
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

	server := control.NewServer(sockPath, ctrl)
	if err := server.Start(); err != nil {
		t.Fatalf("Server Start failed: %v", err)
	}
	defer server.Stop()

	client := control.NewClient(sockPath)

	// 1. Send status
	respStatus, err := client.Send("status", "")
	if err != nil {
		t.Fatalf("client.Send status failed: %v", err)
	}
	if !respStatus.Success {
		t.Errorf("expected success status, got: %+v", respStatus)
	}
	if len(respStatus.Listeners) != 4 {
		t.Errorf("expected 4 listeners in status, got %d", len(respStatus.Listeners))
	}

	// 2. Enable syslog_tcp
	respEnable, err := client.Send("enable", "syslog_tcp")
	if err != nil {
		t.Fatalf("client.Send enable syslog_tcp failed: %v", err)
	}
	if !respEnable.Success {
		t.Errorf("expected enable success, got: %+v", respEnable)
	}
	if !syslogTCP.IsEnabled() {
		t.Errorf("expected syslogTCP to be enabled")
	}

	// 3. Disable syslog_tcp
	respDisable, err := client.Send("disable", "syslog_tcp")
	if err != nil {
		t.Fatalf("client.Send disable syslog_tcp failed: %v", err)
	}
	if !respDisable.Success {
		t.Errorf("expected disable success, got: %+v", respDisable)
	}
	if syslogTCP.IsEnabled() {
		t.Errorf("expected syslogTCP to be disabled")
	}

	// 4. Enable all
	respAll, err := client.Send("enable", "all")
	if err != nil {
		t.Fatalf("client.Send enable all failed: %v", err)
	}
	if !respAll.Success {
		t.Errorf("expected enable all success, got: %+v", respAll)
	}
	for _, l := range respAll.Listeners {
		if !l.Enabled {
			t.Errorf("expected %s enabled after enable all", l.Name)
		}
	}

	// 5. Disable all
	respDisableAll, err := client.Send("disable", "all")
	if err != nil {
		t.Fatalf("client.Send disable all failed: %v", err)
	}
	if !respDisableAll.Success {
		t.Errorf("expected disable all success, got: %+v", respDisableAll)
	}
	for _, l := range respDisableAll.Listeners {
		if l.Enabled {
			t.Errorf("expected %s disabled after disable all", l.Name)
		}
	}

	// 6. Attempt to start a second Server on the active socket: must fail to prevent hijacking
	duplicateServer := control.NewServer(sockPath, ctrl)
	if err := duplicateServer.Start(); err == nil {
		duplicateServer.Stop()
		t.Fatal("expected error starting second server on active socket, got nil")
	}
}
