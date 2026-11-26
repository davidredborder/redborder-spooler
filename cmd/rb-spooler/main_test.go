package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/capture"
	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/control"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

func captureOutput(f func()) string {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	outC := make(chan string)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		outC <- buf.String()
	}()

	f()

	_ = w.Close()
	os.Stdout = oldStdout
	out := <-outC
	_ = r.Close()
	return out
}

func TestRunListVerbose(t *testing.T) {
	tmpDir := t.TempDir()
	spoolDir := filepath.Join(tmpDir, "spool")
	_ = os.MkdirAll(spoolDir, 0755)

	cfgPath := filepath.Join(tmpDir, "config.yaml")
	cfgContent := "spool_dir: \"" + spoolDir + "\"\n"
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg := &config.Config{SpoolDir: spoolDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	t1 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	_ = store.WriteUDP(t1, []byte{10, 1, 50, 20}, []byte{10, 0, 0, 1}, 1234, 2055, []byte("pkt-1"))
	_ = store.WriteUDP(t1.Add(time.Second), []byte{10, 1, 50, 21}, []byte{10, 0, 0, 1}, 1234, 2055, []byte("pkt-2"))
	_ = store.CloseHour()
	_ = store.Close()

	// 1. Run list without -v
	outDefault := captureOutput(func() {
		runList([]string{"-config", cfgPath})
	})

	if !strings.Contains(outDefault, "HOUR (UTC)          TRAFFIC PKTS  TRAFFIC BYTES  HTTP REQS     SYSLOG MSGS   DISK SIZE     TEMPLATES   EXPORTERS") {
		t.Fatalf("expected output to contain updated column headers, got:\n%s", outDefault)
	}
	if !strings.Contains(outDefault, "traffic pkts") {
		t.Fatalf("expected output footer to contain traffic pkts, got:\n%s", outDefault)
	}
	if !strings.Contains(outDefault, "HTTP reqs") {
		t.Fatalf("expected output footer to contain HTTP reqs, got:\n%s", outDefault)
	}
	if !strings.Contains(outDefault, "Syslog msgs") {
		t.Fatalf("expected output footer to contain Syslog msgs, got:\n%s", outDefault)
	}
	if !strings.Contains(outDefault, "2026-11-24/10") {
		t.Fatalf("expected output to contain hour path, got:\n%s", outDefault)
	}
	if strings.Contains(outDefault, "└─ Exporters:") {
		t.Fatalf("expected default list NOT to contain verbose exporters line, got:\n%s", outDefault)
	}

	// 2. Run list with -v
	outVerbose := captureOutput(func() {
		runList([]string{"-config", cfgPath, "-v"})
	})

	if !strings.Contains(outVerbose, "└─ Exporters: 10.1.50.20, 10.1.50.21") {
		t.Fatalf("expected verbose output to contain exporters line, got:\n%s", outVerbose)
	}

	// 3. Run list with --verbose
	outVerboseLong := captureOutput(func() {
		runList([]string{"-config", cfgPath, "--verbose"})
	})

	if !strings.Contains(outVerboseLong, "└─ Exporters: 10.1.50.20, 10.1.50.21") {
		t.Fatalf("expected verbose output to contain exporters line, got:\n%s", outVerboseLong)
	}
}

func TestRunReplaySummaryAndBypass(t *testing.T) {
	tmpDir := t.TempDir()
	spoolDir := filepath.Join(tmpDir, "spool")
	_ = os.MkdirAll(spoolDir, 0755)

	cfgPath := filepath.Join(tmpDir, "config.yaml")
	cfgContent := "spool_dir: \"" + spoolDir + "\"\n"
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg := &config.Config{SpoolDir: spoolDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	t1 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	_ = store.WriteUDP(t1, []byte{10, 1, 50, 20}, []byte{10, 0, 0, 1}, 1234, 2055, []byte("pkt-1"))
	_ = store.CloseHour()
	_ = store.Close()

	// 1. Test replay with -y (bypasses prompt)
	out := captureOutput(func() {
		runReplay([]string{
			"-config", cfgPath,
			"-from", "2026-11-24 10:00",
			"-to", "2026-11-24 10:30",
			"-type", "udp",
			"-speed", "0",
			"-y",
		})
	})

	if !strings.Contains(out, "Replay Action Summary:") {
		t.Errorf("expected output to contain Replay Action Summary, got:\n%s", out)
	}
	if !strings.Contains(out, "Replay Type:    TRAFFIC") {
		t.Errorf("expected output to contain Replay Type: TRAFFIC, got:\n%s", out)
	}
	if !strings.Contains(out, "Replay Summary:") {
		t.Errorf("expected output to contain Replay Summary, got:\n%s", out)
	}

	// 2. Test replay with interactive "y" on stdin
	oldStdin := os.Stdin
	rPipe, wPipe, _ := os.Pipe()
	_, _ = wPipe.Write([]byte("y\n"))
	_ = wPipe.Close()
	os.Stdin = rPipe

	outInteractiveY := captureOutput(func() {
		runReplay([]string{
			"-config", cfgPath,
			"-from", "2026-11-24 10:00",
			"-to", "2026-11-24 10:30",
			"-type", "udp",
			"-speed", "0",
		})
	})
	_ = rPipe.Close()
	os.Stdin = oldStdin

	if !strings.Contains(outInteractiveY, "Proceed with replay? [y/N]:") {
		t.Errorf("expected prompt in output, got:\n%s", outInteractiveY)
	}
	if !strings.Contains(outInteractiveY, "Replay Summary:") {
		t.Errorf("expected Replay Summary after confirming 'y', got:\n%s", outInteractiveY)
	}

	// 3. Test replay with interactive "n" (cancellation) on stdin
	rPipeN, wPipeN, _ := os.Pipe()
	_, _ = wPipeN.Write([]byte("n\n"))
	_ = wPipeN.Close()
	os.Stdin = rPipeN

	outInteractiveN := captureOutput(func() {
		runReplay([]string{
			"-config", cfgPath,
			"-from", "2026-11-24 10:00",
			"-to", "2026-11-24 10:30",
			"-type", "udp",
			"-speed", "0",
		})
	})
	_ = rPipeN.Close()
	os.Stdin = oldStdin

	if !strings.Contains(outInteractiveN, "Replay canceled.") {
		t.Errorf("expected 'Replay canceled.' output, got:\n%s", outInteractiveN)
	}
	if strings.Contains(outInteractiveN, "Replay Summary:") {
		t.Errorf("did not expect Replay Summary when canceled, got:\n%s", outInteractiveN)
	}

	// 4. Test status output including Total Syslog Messages
	_ = store.WriteSyslog(t1, []byte{10, 1, 50, 20}, "udp", []byte("<14>status test syslog"))
	_ = store.CloseHour()

	outStatus := captureOutput(func() {
		runStatus([]string{"-config", cfgPath})
	})
	if !strings.Contains(outStatus, "Total Traffic Packets:") {
		t.Errorf("expected status to contain 'Total Traffic Packets:', got:\n%s", outStatus)
	}
	if !strings.Contains(outStatus, "Total Syslog Messages:") {
		t.Errorf("expected status to contain 'Total Syslog Messages:', got:\n%s", outStatus)
	}
}

func TestHelpSubcommands(t *testing.T) {
	// 1. Top-level help
	outTop := captureOutput(func() {
		dispatch("help", []string{})
	})
	if !strings.Contains(outTop, "rb-spooler <command> [arguments]") {
		t.Errorf("expected top-level usage in outTop, got:\n%s", outTop)
	}
	if !strings.Contains(outTop, "help <command>") {
		t.Errorf("expected mention of 'help <command>' in outTop, got:\n%s", outTop)
	}

	// 2. Subcommand help via 'help replay'
	outReplayHelp := captureOutput(func() {
		dispatch("help", []string{"replay"})
	})
	if !strings.Contains(outReplayHelp, "rb-spooler replay --from") {
		t.Errorf("expected replay usage syntax, got:\n%s", outReplayHelp)
	}
	if !strings.Contains(outReplayHelp, "--type <type>") {
		t.Errorf("expected --type option explanation, got:\n%s", outReplayHelp)
	}
	if !strings.Contains(outReplayHelp, "--http-target <url>") {
		t.Errorf("expected --http-target option explanation, got:\n%s", outReplayHelp)
	}
	if !strings.Contains(outReplayHelp, "--syslog-target <host:port>") {
		t.Errorf("expected --syslog-target option explanation, got:\n%s", outReplayHelp)
	}
	if !strings.Contains(outReplayHelp, "--ports <ports>") {
		t.Errorf("expected --ports option explanation, got:\n%s", outReplayHelp)
	}
	if !strings.Contains(outReplayHelp, "-y, --yes") {
		t.Errorf("expected -y/--yes option explanation, got:\n%s", outReplayHelp)
	}

	// 3. Subcommand help via 'replay help'
	outReplayArg := captureOutput(func() {
		dispatch("replay", []string{"help"})
	})
	if !strings.Contains(outReplayArg, "rb-spooler replay --from") {
		t.Errorf("expected replay usage syntax from 'replay help', got:\n%s", outReplayArg)
	}

	// 4. Subcommand help via 'help export'
	outExport := captureOutput(func() {
		dispatch("help", []string{"export"})
	})
	if !strings.Contains(outExport, "rb-spooler export --from") {
		t.Errorf("expected export usage syntax, got:\n%s", outExport)
	}
	if !strings.Contains(outExport, "--output <path>") {
		t.Errorf("expected --output option explanation, got:\n%s", outExport)
	}

	// 5. Subcommand help via 'help list'
	outList := captureOutput(func() {
		dispatch("help", []string{"list"})
	})
	if !strings.Contains(outList, "-v, --verbose") {
		t.Errorf("expected verbose option explanation in list help, got:\n%s", outList)
	}

	// 6. Help help edge case
	outHelpHelp := captureOutput(func() {
		dispatch("help", []string{"help"})
	})
	if !strings.Contains(outHelpHelp, "rb-spooler <command> [arguments]") {
		t.Errorf("expected top-level usage for 'help help', got:\n%s", outHelpHelp)
	}

	// 7. Verify all 9 subcommands with flag -h, --help, and 'help'
	allCmds := []string{"record", "replay", "list", "prune", "status", "export", "import", "pcap", "ctl"}
	for _, cmdName := range allCmds {
		outDashH := captureOutput(func() {
			dispatch(cmdName, []string{"-h"})
		})
		if !strings.Contains(outDashH, "Usage:\n  rb-spooler "+cmdName) {
			t.Errorf("expected usage header for '%s -h', got:\n%s", cmdName, outDashH)
		}

		outDoubleDashHelp := captureOutput(func() {
			dispatch(cmdName, []string{"--help"})
		})
		if !strings.Contains(outDoubleDashHelp, "Usage:\n  rb-spooler "+cmdName) {
			t.Errorf("expected usage header for '%s --help', got:\n%s", cmdName, outDoubleDashHelp)
		}

		outHelpCmd := captureOutput(func() {
			dispatch("help", []string{cmdName})
		})
		if !strings.Contains(outHelpCmd, "Usage:\n  rb-spooler "+cmdName) {
			t.Errorf("expected usage header for 'help %s', got:\n%s", cmdName, outHelpCmd)
		}
	}
}

func TestRunCtlAndStatusWithLiveSocket(t *testing.T) {
	tmpDir := t.TempDir()
	spoolDir := filepath.Join(tmpDir, "spool")
	sockPath := filepath.Join(tmpDir, "control.sock")
	cfgPath := filepath.Join(tmpDir, "config.yaml")

	cfgYaml := "spool_dir: \"" + spoolDir + "\"\ncontrol_socket: \"" + sockPath + "\"\n"
	if err := os.WriteFile(cfgPath, []byte(cfgYaml), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg := &config.Config{
		SpoolDir:      spoolDir,
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
		t.Fatalf("failed to create store: %v", err)
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

	ctlServer := control.NewServer(sockPath, ctrl)
	if err := ctlServer.Start(); err != nil {
		t.Fatalf("failed to start control server: %v", err)
	}
	defer ctlServer.Stop()

	// 1. Test ctl status
	outCtlStatus := captureOutput(func() {
		runCtl([]string{"status", "--config", cfgPath})
	})
	if !strings.Contains(outCtlStatus, "Daemon Status:") || !strings.Contains(outCtlStatus, "Recording Listeners:") {
		t.Errorf("unexpected outCtlStatus:\n%s", outCtlStatus)
	}
	if !strings.Contains(outCtlStatus, "SYSLOG_TCP:  [DISABLED]") {
		t.Errorf("expected SYSLOG_TCP DISABLED initially, got:\n%s", outCtlStatus)
	}

	// 2. Test ctl enable syslog_tcp
	outEnable := captureOutput(func() {
		runCtl([]string{"enable", "syslog_tcp", "--config", cfgPath})
	})
	if !strings.Contains(outEnable, "Success:") || !strings.Contains(outEnable, "SYSLOG_TCP:  [ENABLED ]") {
		t.Errorf("expected SYSLOG_TCP ENABLED after enable, got:\n%s", outEnable)
	}

	// 3. Test ctl disable syslog_tcp
	outDisable := captureOutput(func() {
		runCtl([]string{"disable", "syslog_tcp", "--config", cfgPath})
	})
	if !strings.Contains(outDisable, "Success:") || !strings.Contains(outDisable, "SYSLOG_TCP:  [DISABLED]") {
		t.Errorf("expected SYSLOG_TCP DISABLED after disable, got:\n%s", outDisable)
	}

	// 4. Test runStatus querying live socket
	outStatus := captureOutput(func() {
		runStatus([]string{"-config", cfgPath})
	})
	if !strings.Contains(outStatus, "Daemon State:          RUNNING") {
		t.Errorf("expected status to show RUNNING daemon, got:\n%s", outStatus)
	}
	if !strings.Contains(outStatus, "Control Socket:") {
		t.Errorf("expected status to show Control Socket, got:\n%s", outStatus)
	}
	if !strings.Contains(outStatus, "Recording Listeners:") {
		t.Errorf("expected status to show Recording Listeners, got:\n%s", outStatus)
	}
}

func TestLoadConfigWithFallback(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Explicit valid custom path
	customPath := filepath.Join(tmpDir, "custom.yaml")
	if err := os.WriteFile(customPath, []byte("spool_dir: \"/tmp/custom_spool\"\n"), 0644); err != nil {
		t.Fatalf("failed to write custom config: %v", err)
	}
	cfg, err := loadConfigWithFallback(customPath)
	if err != nil {
		t.Fatalf("expected nil error for valid custom config, got: %v", err)
	}
	if cfg.SpoolDir != "/tmp/custom_spool" {
		t.Errorf("expected /tmp/custom_spool, got %s", cfg.SpoolDir)
	}

	// 2. Explicit invalid custom path returns error
	_, err = loadConfigWithFallback(filepath.Join(tmpDir, "nonexistent.yaml"))
	if err == nil {
		t.Errorf("expected error for nonexistent custom config path")
	}

	// 3. Fallback to default when DefaultConfigPath does not exist
	cfgDefault, err := loadConfigWithFallback(DefaultConfigPath)
	if err != nil {
		t.Fatalf("expected nil error when default config does not exist, got: %v", err)
	}
	if cfgDefault == nil {
		t.Fatalf("expected non-nil default config")
	}

	// 4. Test resolveSpoolDir
	resCustom := resolveSpoolDir("/custom/dir")
	if resCustom != "/custom/dir" {
		t.Errorf("expected /custom/dir, got %s", resCustom)
	}
	resDefault := resolveSpoolDir("")
	if resDefault != DefaultSpoolDir {
		t.Errorf("expected %s, got %s", DefaultSpoolDir, resDefault)
	}
}
