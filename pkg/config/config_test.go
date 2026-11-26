package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
)

func TestDefaultConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	if cfg == nil {
		t.Fatal("expected non-nil default config")
	}

	if cfg.SpoolDir != "/var/spool/redborder-spooler" {
		t.Errorf("expected default SpoolDir /var/spool/redborder-spooler, got %s", cfg.SpoolDir)
	}
	if cfg.ControlSocket != "/run/redborder-spooler/control.sock" {
		t.Errorf("expected default ControlSocket /run/redborder-spooler/control.sock, got %s", cfg.ControlSocket)
	}
	if cfg.MetaSyncInterval != 5*time.Second {
		t.Errorf("expected default MetaSyncInterval 5s, got %v", cfg.MetaSyncInterval)
	}
	if len(cfg.Capture.Interfaces) != 1 || cfg.Capture.Interfaces[0] != "any" {
		t.Errorf("unexpected interfaces: %v", cfg.Capture.Interfaces)
	}
	if len(cfg.Capture.UDPPorts) != 3 || cfg.Capture.UDPPorts[0] != 2055 || cfg.Capture.UDPPorts[1] != 6343 || cfg.Capture.UDPPorts[2] != 514 {
		t.Errorf("unexpected udp ports: %v", cfg.Capture.UDPPorts)
	}
	if cfg.Capture.SyslogTCP.Enabled || cfg.Capture.SyslogTCP.ListenAddr != ":10514" || cfg.Capture.SyslogTCP.UpstreamAddr != "" {
		t.Errorf("unexpected default SyslogTCP config: %+v", cfg.Capture.SyslogTCP)
	}
	if cfg.Capture.Mode != "af_packet" {
		t.Errorf("expected af_packet mode, got %s", cfg.Capture.Mode)
	}
	if cfg.Capture.LVSDRDedupWindowMs != 20 {
		t.Errorf("expected 20ms dedup window, got %d", cfg.Capture.LVSDRDedupWindowMs)
	}
	if cfg.Retention.MaxAge != 72*time.Hour {
		t.Errorf("expected 72h max age, got %v", cfg.Retention.MaxAge)
	}
	if cfg.Retention.MaxStorageMB != 20480 {
		t.Errorf("expected 20480 max storage MB, got %d", cfg.Retention.MaxStorageMB)
	}
	if cfg.Retention.MinFreeDiskPct != 15.0 {
		t.Errorf("expected 15.0 min free disk pct, got %f", cfg.Retention.MinFreeDiskPct)
	}
	if cfg.Retention.CheckInterval != 5*time.Minute {
		t.Errorf("expected 5m check interval, got %v", cfg.Retention.CheckInterval)
	}
	if cfg.Replay.TargetHost != "127.0.0.1" {
		t.Errorf("expected 127.0.0.1 replay target host, got %s", cfg.Replay.TargetHost)
	}
	if cfg.Replay.DefaultSpeed != 10.0 {
		t.Errorf("expected default speed 10.0, got %f", cfg.Replay.DefaultSpeed)
	}
	if !cfg.Replay.UseRBRPEnvelope {
		t.Errorf("expected UseRBRPEnvelope to be true")
	}
}

func TestLoadConfigYAML(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	yamlData := `
spool_dir: "/tmp/spool-test"
meta_sync_interval: "2s"
capture:
  interfaces:
    - "eth0"
    - "eth1"
  udp_ports:
    - 2055
  mode: "socket"
  lvs_dr_dedup_window_ms: 10
  http:
    enabled: true
    listen_addr: ":18080"
    upstream_url: "http://127.0.0.1:8080"
  syslog_tcp:
    enabled: true
    listen_addr: ":10514"
    upstream_addr: "10.0.0.2:514"
retention:
  max_age: "24h"
  max_storage_mb: 5000
  min_free_disk_pct: 10.5
  check_interval: "1m"
replay:
  target_host: "10.0.0.1"
  default_speed: 5.0
  use_rbrp_envelope: false
`
	if err := os.WriteFile(cfgPath, []byte(yamlData), 0644); err != nil {
		t.Fatalf("failed to write test yaml: %v", err)
	}

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.SpoolDir != "/tmp/spool-test" {
		t.Errorf("SpoolDir mismatch: %s", cfg.SpoolDir)
	}
	if cfg.MetaSyncInterval != 2*time.Second {
		t.Errorf("MetaSyncInterval mismatch: %v", cfg.MetaSyncInterval)
	}
	if len(cfg.Capture.Interfaces) != 2 || cfg.Capture.Interfaces[0] != "eth0" {
		t.Errorf("Interfaces mismatch: %v", cfg.Capture.Interfaces)
	}
	if cfg.Capture.Mode != "socket" {
		t.Errorf("Mode mismatch: %s", cfg.Capture.Mode)
	}
	if !cfg.Capture.HTTP.Enabled || cfg.Capture.HTTP.ListenAddr != ":18080" {
		t.Errorf("HTTP config mismatch: %+v", cfg.Capture.HTTP)
	}
	if !cfg.Capture.SyslogTCP.Enabled || cfg.Capture.SyslogTCP.ListenAddr != ":10514" || cfg.Capture.SyslogTCP.UpstreamAddr != "10.0.0.2:514" {
		t.Errorf("SyslogTCP config mismatch: %+v", cfg.Capture.SyslogTCP)
	}
	if cfg.Retention.MaxAge != 24*time.Hour {
		t.Errorf("MaxAge mismatch: %v", cfg.Retention.MaxAge)
	}
	if cfg.Retention.MaxStorageMB != 5000 {
		t.Errorf("MaxStorageMB mismatch: %d", cfg.Retention.MaxStorageMB)
	}
	if cfg.Retention.MinFreeDiskPct != 10.5 {
		t.Errorf("MinFreeDiskPct mismatch: %f", cfg.Retention.MinFreeDiskPct)
	}
	if cfg.Retention.CheckInterval != time.Minute {
		t.Errorf("CheckInterval mismatch: %v", cfg.Retention.CheckInterval)
	}
	if cfg.Replay.TargetHost != "10.0.0.1" {
		t.Errorf("Replay TargetHost mismatch: %s", cfg.Replay.TargetHost)
	}
	if cfg.Replay.DefaultSpeed != 5.0 {
		t.Errorf("Replay DefaultSpeed mismatch: %f", cfg.Replay.DefaultSpeed)
	}
	if cfg.Replay.UseRBRPEnvelope {
		t.Errorf("Replay UseRBRPEnvelope should be false")
	}
}

func TestLoadConfigEmptyPathReturnsDefault(t *testing.T) {
	cfg, err := config.LoadConfig("")
	if err != nil {
		t.Fatalf("expected nil error for empty path, got %v", err)
	}
	if cfg.SpoolDir != "/var/spool/redborder-spooler" {
		t.Errorf("expected default config, got SpoolDir %s", cfg.SpoolDir)
	}
}
