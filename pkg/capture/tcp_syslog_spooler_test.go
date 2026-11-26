package capture_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/capture"
	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

func TestTCPSyslogSpoolerNewlineFrames(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewHourlyStore(&config.Config{SpoolDir: tmpDir})
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	cfg := config.TCPSyslogConfig{
		Enabled:    true,
		ListenAddr: "127.0.0.1:0",
	}

	spooler := capture.NewTCPSyslogSpooler(cfg, store)
	if err := spooler.Start(); err != nil {
		t.Fatalf("spooler.Start failed: %v", err)
	}
	defer spooler.Stop()

	listenAddr := spooler.Addr().String()

	conn, err := net.Dial("tcp", listenAddr)
	if err != nil {
		t.Fatalf("failed to dial spooler: %v", err)
	}
	defer conn.Close()

	msg1 := "<14>Nov 25 12:00:00 app1 test message 1\n"
	msg2 := "<14>Nov 25 12:00:01 app1 test message 2\r\n"

	if _, err := conn.Write([]byte(msg1 + msg2)); err != nil {
		t.Fatalf("failed to write messages: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	reader, err := store.NewSyslogRangeReader(time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("NewSyslogRangeReader failed: %v", err)
	}
	defer reader.Close()

	r1, err := reader.Next()
	if err != nil {
		t.Fatalf("failed to read r1: %v", err)
	}
	if string(r1.Message) != "<14>Nov 25 12:00:00 app1 test message 1" {
		t.Errorf("r1 message mismatch: %s", string(r1.Message))
	}

	r2, err := reader.Next()
	if err != nil {
		t.Fatalf("failed to read r2: %v", err)
	}
	if string(r2.Message) != "<14>Nov 25 12:00:01 app1 test message 2" {
		t.Errorf("r2 message mismatch: %s", string(r2.Message))
	}
}

func TestTCPSyslogSpoolerOctetCounting(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewHourlyStore(&config.Config{SpoolDir: tmpDir})
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	cfg := config.TCPSyslogConfig{
		Enabled:    true,
		ListenAddr: "127.0.0.1:0",
	}

	spooler := capture.NewTCPSyslogSpooler(cfg, store)
	if err := spooler.Start(); err != nil {
		t.Fatalf("spooler.Start failed: %v", err)
	}
	defer spooler.Stop()

	conn, err := net.Dial("tcp", spooler.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial spooler: %v", err)
	}
	defer conn.Close()

	raw1 := "<34>1 2026-11-25T12:00:00Z myhost app 12 - msg one"
	raw2 := "<34>1 2026-11-25T12:00:01Z myhost app 13 - msg two"

	frame1 := fmt.Sprintf("%d %s", len(raw1), raw1)
	frame2 := fmt.Sprintf("%d %s\n", len(raw2)+1, raw2+"\n")

	if _, err := conn.Write([]byte(frame1 + frame2)); err != nil {
		t.Fatalf("failed to write octet counted messages: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	reader, err := store.NewSyslogRangeReader(time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("NewSyslogRangeReader failed: %v", err)
	}
	defer reader.Close()

	r1, err := reader.Next()
	if err != nil {
		t.Fatalf("failed to read r1: %v", err)
	}
	if string(r1.Message) != raw1 {
		t.Errorf("r1 message mismatch: got %q, want %q", string(r1.Message), raw1)
	}

	r2, err := reader.Next()
	if err != nil {
		t.Fatalf("failed to read r2: %v", err)
	}
	if string(r2.Message) != raw2 {
		t.Errorf("r2 message mismatch: got %q, want %q", string(r2.Message), raw2)
	}
}

func TestTCPSyslogSpoolerDropReplayed(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewHourlyStore(&config.Config{SpoolDir: tmpDir})
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	cfg := config.TCPSyslogConfig{
		Enabled:    true,
		ListenAddr: "127.0.0.1:0",
	}

	spooler := capture.NewTCPSyslogSpooler(cfg, store)
	if err := spooler.Start(); err != nil {
		t.Fatalf("spooler.Start failed: %v", err)
	}
	defer spooler.Stop()

	conn, err := net.Dial("tcp", spooler.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial spooler: %v", err)
	}
	defer conn.Close()

	replayedMsg := "<14>Nov 25 12:00:00 test: X-Redborder-Replay msg\n"
	normalMsg := "<14>Nov 25 12:00:01 test: normal msg\n"

	_, _ = conn.Write([]byte(replayedMsg + normalMsg))
	time.Sleep(100 * time.Millisecond)

	reader, err := store.NewSyslogRangeReader(time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("NewSyslogRangeReader failed: %v", err)
	}
	defer reader.Close()

	r1, err := reader.Next()
	if err != nil {
		t.Fatalf("failed to read r1: %v", err)
	}
	if string(r1.Message) != "<14>Nov 25 12:00:01 test: normal msg" {
		t.Errorf("expected normal msg, got %s", string(r1.Message))
	}

	_, err = reader.Next()
	if !bytes.Equal([]byte(err.Error()), []byte(io.EOF.Error())) {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestTCPSyslogSpoolerUpstreamForwarding(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewHourlyStore(&config.Config{SpoolDir: tmpDir})
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Mock upstream TCP listener
	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock upstream: %v", err)
	}
	defer upstreamLn.Close()

	receivedCh := make(chan string, 1)
	go func() {
		conn, err := upstreamLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		if scanner.Scan() {
			receivedCh <- scanner.Text()
		}
	}()

	cfg := config.TCPSyslogConfig{
		Enabled:      true,
		ListenAddr:   "127.0.0.1:0",
		UpstreamAddr: upstreamLn.Addr().String(),
	}

	spooler := capture.NewTCPSyslogSpooler(cfg, store)
	if err := spooler.Start(); err != nil {
		t.Fatalf("spooler.Start failed: %v", err)
	}
	defer spooler.Stop()

	conn, err := net.Dial("tcp", spooler.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial spooler: %v", err)
	}
	defer conn.Close()

	testMsg := "<14>Nov 25 12:00:00 app: forwarded syslog\n"
	if _, err := conn.Write([]byte(testMsg)); err != nil {
		t.Fatalf("failed to write test message: %v", err)
	}

	select {
	case received := <-receivedCh:
		if received != "<14>Nov 25 12:00:00 app: forwarded syslog" {
			t.Errorf("upstream received mismatch: got %q", received)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upstream to receive forwarded message")
	}
}

func TestTCPSyslogSpoolerGracefulShutdown(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewHourlyStore(&config.Config{SpoolDir: tmpDir})
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	spooler := capture.NewTCPSyslogSpooler(config.TCPSyslogConfig{
		Enabled:    true,
		ListenAddr: "127.0.0.1:0",
	}, store)

	if err := spooler.Start(); err != nil {
		t.Fatalf("spooler.Start failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		_ = spooler.Stop()
	}()

	wg.Wait()

	// Calling Stop again should be safe and return nil
	if err := spooler.Stop(); err != nil {
		t.Errorf("expected second Stop() to return nil, got %v", err)
	}
}

func TestTCPSyslogSpoolerShortMessagePersistentConn(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewHourlyStore(&config.Config{SpoolDir: tmpDir})
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	cfg := config.TCPSyslogConfig{
		Enabled:    true,
		ListenAddr: "127.0.0.1:0",
	}

	spooler := capture.NewTCPSyslogSpooler(cfg, store)
	if err := spooler.Start(); err != nil {
		t.Fatalf("spooler.Start failed: %v", err)
	}
	defer spooler.Stop()

	conn, err := net.Dial("tcp", spooler.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial spooler: %v", err)
	}
	defer conn.Close()

	// Short message (< 16 bytes) sent on persistent connection WITHOUT closing
	shortMsg := "<1>test\n"
	if _, err := conn.Write([]byte(shortMsg)); err != nil {
		t.Fatalf("failed to write short message: %v", err)
	}

	// Give spooler a short moment to process
	time.Sleep(100 * time.Millisecond)

	reader, err := store.NewSyslogRangeReader(time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("NewSyslogRangeReader failed: %v", err)
	}
	defer reader.Close()

	rec, err := reader.Next()
	if err != nil {
		t.Fatalf("expected to read short message from store, got error: %v", err)
	}
	if string(rec.Message) != "<1>test" {
		t.Errorf("message mismatch: got %q, want %q", string(rec.Message), "<1>test")
	}
}

func TestTCPSyslogSpoolerMaxMsgLenEnforced(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewHourlyStore(&config.Config{SpoolDir: tmpDir})
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	cfg := config.TCPSyslogConfig{
		Enabled:    true,
		ListenAddr: "127.0.0.1:0",
	}

	spooler := capture.NewTCPSyslogSpooler(cfg, store)
	if err := spooler.Start(); err != nil {
		t.Fatalf("spooler.Start failed: %v", err)
	}
	defer spooler.Stop()

	conn, err := net.Dial("tcp", spooler.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial spooler: %v", err)
	}
	defer conn.Close()

	// Octet counted message claiming 70000 bytes (> MaxSyslogMsgLen of 65536)
	oversizedHdr := "70000 <1>oversized\n"
	_, _ = conn.Write([]byte(oversizedHdr))
	time.Sleep(50 * time.Millisecond)

	// Send normal message after
	normalMsg := "<14>Nov 25 12:00:00 app normal after oversized\n"
	_, _ = conn.Write([]byte(normalMsg))
	time.Sleep(100 * time.Millisecond)

	reader, err := store.NewSyslogRangeReader(time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("NewSyslogRangeReader failed: %v", err)
	}
	defer reader.Close()

	// The 70000 length was rejected, so store should NOT have a 70000 byte record
	rec, err := reader.Next()
	if err == nil && len(rec.Message) == 70000 {
		t.Errorf("expected oversized message to not be allocated/stored as 70000 bytes")
	}
}
