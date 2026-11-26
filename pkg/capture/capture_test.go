package capture_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/capture"
	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

func TestUDPSocketCapture(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		SpoolDir: tmpDir,
	}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Find an available UDP port
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	_ = conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener := capture.NewUDPSocketListener([]int{port}, "127.0.0.1", store, 0)
	listener.FilterLoopback = false
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("listener.Start failed: %v", err)
	}
	defer listener.Stop()

	// Send UDP datagram
	clientConn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	testMsg := []byte("udp-socket-capture-test")
	if _, err := clientConn.Write(testMsg); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	_ = store.CloseHour()

	buckets, err := store.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) == 0 {
		t.Fatal("expected at least 1 bucket recorded")
	}
	if buckets[0].UDPPackets < 1 {
		t.Errorf("expected at least 1 UDP packet recorded, got %d", buckets[0].UDPPackets)
	}
}

func TestHTTPSpoolerTimestampInjection(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		SpoolDir: tmpDir,
	}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Find available TCP port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	httpCfg := config.HTTPConfig{
		Enabled:    true,
		ListenAddr: addr,
	}

	spooler := capture.NewHTTPSpooler(httpCfg, store)
	if err := spooler.Start(); err != nil {
		t.Fatalf("spooler.Start failed: %v", err)
	}
	defer spooler.Stop()

	// 1. Send POST without timestamp
	rawJSON := []byte(`{"device":"sensor-1","events":[{"type":"alarm"}]}`)
	url := fmt.Sprintf("http://%s/rbdata/events", addr)

	resp, err := http.Post(url, "application/json", bytes.NewReader(rawJSON))
	if err != nil {
		t.Fatalf("http.Post failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		t.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	time.Sleep(100 * time.Millisecond)
	_ = store.CloseHour()

	rr, err := store.OpenRange(time.Now().UTC().Add(-time.Hour), time.Now().UTC().Add(time.Hour))
	if err != nil {
		t.Fatalf("OpenRange failed: %v", err)
	}
	defer rr.Close()

	rec, err := rr.NextHTTP()
	if err != nil {
		t.Fatalf("NextHTTP failed: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(rec.Body, &parsed); err != nil {
		t.Fatalf("failed to unmarshal recorded body: %v", err)
	}

	ts, hasTS := parsed["timestamp"]
	if !hasTS {
		t.Fatal("expected timestamp to be injected into JSON body")
	}
	if tsNum, ok := ts.(float64); !ok || tsNum <= 0 {
		t.Errorf("invalid injected timestamp: %v", ts)
	}
}

func TestAFPacketListenerInit(t *testing.T) {
	// Should initialize without error and gracefully handle lack of CAP_NET_RAW
	listener := capture.NewAFPacketListener([]string{"any"}, []int{2055}, nil, 20*time.Millisecond)
	if listener == nil {
		t.Fatal("expected non-nil AFPacketListener")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := listener.Start(ctx)
	if err != nil {
		t.Logf("AFPacket Start returned expected permission/system error: %v", err)
	} else {
		defer listener.Stop()
	}
}
