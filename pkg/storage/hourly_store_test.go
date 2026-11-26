package storage_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

func TestHourlyStoreWriteAndRangeRead(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		SpoolDir: tmpDir,
		Retention: config.RetentionConfig{
			MaxAge:         72 * time.Hour,
			MaxStorageMB:   1000,
			MinFreeDiskPct: 5.0,
			CheckInterval:  time.Minute,
		},
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	t1 := time.Date(2026, 11, 24, 10, 5, 0, 0, time.UTC)
	t2 := time.Date(2026, 11, 24, 10, 15, 30, 0, time.UTC)

	srcIP := net.ParseIP("192.168.1.10")
	dstIP := net.ParseIP("10.0.0.1")

	// Write UDP
	if err := store.WriteUDP(t1, srcIP, dstIP, 12345, 2055, []byte("udp-pkt-1")); err != nil {
		t.Fatalf("WriteUDP 1 failed: %v", err)
	}
	if err := store.WriteUDP(t2, srcIP, dstIP, 12345, 2055, []byte("udp-pkt-2")); err != nil {
		t.Fatalf("WriteUDP 2 failed: %v", err)
	}

	// Write HTTP
	headers := map[string][]string{"User-Agent": {"test-agent"}}
	if err := store.WriteHTTP(t1, srcIP, "POST", "/api/v1/event", headers, []byte(`{"event":"test1"}`)); err != nil {
		t.Fatalf("WriteHTTP 1 failed: %v", err)
	}

	// Flush active hour to disk
	if err := store.CloseHour(); err != nil {
		t.Fatalf("CloseHour failed: %v", err)
	}

	// Verify files created
	hourDir := filepath.Join(tmpDir, "2026-11-24", "10")
	for _, fName := range []string{"udp.wal", "udp.idx", "http.wal", "meta.json"} {
		path := filepath.Join(hourDir, fName)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected file %s to exist: %v", path, err)
		}
	}

	// List buckets
	buckets, err := store.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(buckets))
	}
	if buckets[0].UDPPackets != 2 {
		t.Errorf("expected 2 udp packets, got %d", buckets[0].UDPPackets)
	}
	if buckets[0].HTTPRequests != 1 {
		t.Errorf("expected 1 http request, got %d", buckets[0].HTTPRequests)
	}

	firstTime, ok := store.FirstAvailableTime()
	if !ok || !firstTime.Equal(t1) {
		t.Errorf("expected first available time %v, got %v (ok=%v)", t1, firstTime, ok)
	}
	lastTime, ok := store.LastAvailableTime()
	if !ok || !lastTime.Equal(t2) {
		t.Errorf("expected last available time %v, got %v (ok=%v)", t2, lastTime, ok)
	}

	// Read range
	reader, err := store.OpenRange(t1.Add(-time.Minute), t2.Add(time.Minute))
	if err != nil {
		t.Fatalf("OpenRange failed: %v", err)
	}
	defer reader.Close()

	rec1, err := reader.NextUDP()
	if err != nil {
		t.Fatalf("NextUDP 1 failed: %v", err)
	}
	if string(rec1.Payload) != "udp-pkt-1" {
		t.Errorf("rec1 payload mismatch: %s", string(rec1.Payload))
	}

	rec2, err := reader.NextUDP()
	if err != nil {
		t.Fatalf("NextUDP 2 failed: %v", err)
	}
	if string(rec2.Payload) != "udp-pkt-2" {
		t.Errorf("rec2 payload mismatch: %s", string(rec2.Payload))
	}

	// End of UDP
	_, err = reader.NextUDP()
	if err == nil {
		t.Fatal("expected EOF on NextUDP 3")
	}

	// Next HTTP
	hRec, err := reader.NextHTTP()
	if err != nil {
		t.Fatalf("NextHTTP failed: %v", err)
	}
	if hRec.URI != "/api/v1/event" {
		t.Errorf("HTTP URI mismatch: %s", hRec.URI)
	}
}

func TestMinuteSeekOnMissingStartHour(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		SpoolDir: tmpDir,
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Hour 11 has records at minute 10 and minute 50
	t1 := time.Date(2026, 11, 24, 11, 10, 0, 0, time.UTC)
	t2 := time.Date(2026, 11, 24, 11, 50, 0, 0, time.UTC)
	srcIP := net.ParseIP("192.168.1.10")
	dstIP := net.ParseIP("10.0.0.1")

	if err := store.WriteUDP(t1, srcIP, dstIP, 12345, 2055, []byte("pkt-11-10")); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteUDP(t2, srcIP, dstIP, 12345, 2055, []byte("pkt-11-50")); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseHour(); err != nil {
		t.Fatal(err)
	}

	// Request range starting at 10:45 (hour 10 does not exist)
	from := time.Date(2026, 11, 24, 10, 45, 0, 0, time.UTC)
	to := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	reader, err := store.OpenRange(from, to)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Should read pkt-11-10, NOT skip it because from.Minute() was 45!
	rec, err := reader.NextUDP()
	if err != nil {
		t.Fatalf("NextUDP failed: %v", err)
	}
	if string(rec.Payload) != "pkt-11-10" {
		t.Fatalf("expected pkt-11-10, got %s", string(rec.Payload))
	}
}

func TestRetentionPrune(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		SpoolDir: tmpDir,
		Retention: config.RetentionConfig{
			MaxAge:         24 * time.Hour,
			MaxStorageMB:   1000,
			MinFreeDiskPct: 1.0,
			CheckInterval:  time.Minute,
		},
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	// Create 3 hourly directories:
	// 1. Old (48h ago): 2026-11-22/12
	// 2. Recent (5h ago): 2026-11-24/07
	// 3. Current (now): 2026-11-24/12

	oldDir := filepath.Join(tmpDir, "2026-11-22", "12")
	recentDir := filepath.Join(tmpDir, "2026-11-24", "07")
	currentDir := filepath.Join(tmpDir, "2026-11-24", "12")

	for _, d := range []string{oldDir, recentDir, currentDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatal(err)
		}
		// Write a dummy file to give it size
		if err := os.WriteFile(filepath.Join(d, "meta.json"), []byte("{}"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	pruned, err := store.Prune(now)
	if err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

	if len(pruned) != 1 {
		t.Errorf("expected 1 pruned bucket, got %d: %v", len(pruned), pruned)
	}

	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Errorf("expected oldDir to be pruned")
	}
	if _, err := os.Stat(recentDir); err != nil {
		t.Errorf("expected recentDir to be kept")
	}
	if _, err := os.Stat(currentDir); err != nil {
		t.Errorf("expected currentDir to be kept")
	}
}

func TestRetentionNeverDeletesCurrentHour(t *testing.T) {
	tmpDir := t.TempDir()

	// MaxStorageMB = 0 to force prune of all eligible directories
	cfg := &config.Config{
		SpoolDir: tmpDir,
		Retention: config.RetentionConfig{
			MaxAge:         1 * time.Hour,
			MaxStorageMB:   0,
			MinFreeDiskPct: 100.0,
			CheckInterval:  time.Minute,
		},
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	now := time.Date(2026, 11, 24, 15, 0, 0, 0, time.UTC)
	currentDir := filepath.Join(tmpDir, "2026-11-24", "15")
	if err := os.MkdirAll(currentDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(currentDir, "udp.wal"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	pruned, err := store.Prune(now)
	if err != nil {
		t.Fatalf("Prune failed: %v", err)
	}

	for _, p := range pruned {
		if p == filepath.Join("2026-11-24", "15") {
			t.Fatal("current hour was pruned!")
		}
	}

	if _, err := os.Stat(currentDir); err != nil {
		t.Errorf("currentDir was deleted!")
	}
}

func TestEstimatePacketsPartialHours(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}

	srcIP := net.ParseIP("192.168.1.10")
	dstIP := net.ParseIP("10.0.0.1")

	// Write 60 packets across hour 10 and 60 packets across hour 11 (1 per minute)
	for min := 0; min < 60; min++ {
		t10 := time.Date(2026, 11, 24, 10, min, 0, 0, time.UTC)
		if err := store.WriteUDP(t10, srcIP, dstIP, 1234, 2055, []byte("pkt-10")); err != nil {
			t.Fatal(err)
		}
		t11 := time.Date(2026, 11, 24, 11, min, 0, 0, time.UTC)
		if err := store.WriteUDP(t11, srcIP, dstIP, 1234, 2055, []byte("pkt-11")); err != nil {
			t.Fatal(err)
		}
	}
	_ = store.CloseHour()
	_ = store.Close()

	// Reopen store to read persisted buckets and indices
	readStore, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore read failed: %v", err)
	}
	defer readStore.Close()

	// 1. Full 2 hours (10:00 - 12:00) should estimate 120 packets
	f1 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	estFull := readStore.EstimatePackets(f1, t1)
	if estFull != 120 {
		t.Errorf("expected 120 packets for full range, got %d", estFull)
	}

	// 2. Partial cross-hour: 10:30 to 11:30 (30 min in hour 10, 30 min in hour 11) -> ~60 packets
	f2 := time.Date(2026, 11, 24, 10, 30, 0, 0, time.UTC)
	t2 := time.Date(2026, 11, 24, 11, 30, 0, 0, time.UTC)
	estCross := readStore.EstimatePackets(f2, t2)
	if estCross < 55 || estCross > 65 {
		t.Errorf("expected ~60 packets for 10:30-11:30, got %d", estCross)
	}

	// 3. Same hour partial: 10:15 to 10:45 (30 minutes) -> ~30 packets
	f3 := time.Date(2026, 11, 24, 10, 15, 0, 0, time.UTC)
	t3 := time.Date(2026, 11, 24, 10, 45, 0, 0, time.UTC)
	estSame := readStore.EstimatePackets(f3, t3)
	if estSame < 27 || estSame > 33 {
		t.Errorf("expected ~30 packets for 10:15-10:45, got %d", estSame)
	}
}

func TestEstimateHTTPRequestsPartialHours(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}

	srcIP := net.ParseIP("192.168.1.10")

	// Write 60 HTTP requests across hour 10 and 60 across hour 11 (1 per minute)
	for min := 0; min < 60; min++ {
		t10 := time.Date(2026, 11, 24, 10, min, 0, 0, time.UTC)
		if err := store.WriteHTTP(t10, srcIP, "POST", "/events", nil, []byte("body10")); err != nil {
			t.Fatal(err)
		}
		t11 := time.Date(2026, 11, 24, 11, min, 0, 0, time.UTC)
		if err := store.WriteHTTP(t11, srcIP, "POST", "/events", nil, []byte("body11")); err != nil {
			t.Fatal(err)
		}
	}
	_ = store.CloseHour()
	_ = store.Close()

	readStore, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore read failed: %v", err)
	}
	defer readStore.Close()

	// 1. Full 2 hours (10:00 - 12:00) should estimate 120 requests
	f1 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	estFull := readStore.EstimateHTTPRequests(f1, t1)
	if estFull != 120 {
		t.Errorf("expected 120 requests for full range, got %d", estFull)
	}

	// 2. Partial cross-hour: 10:30 to 11:30 -> ~60 requests
	f2 := time.Date(2026, 11, 24, 10, 30, 0, 0, time.UTC)
	t2 := time.Date(2026, 11, 24, 11, 30, 0, 0, time.UTC)
	estCross := readStore.EstimateHTTPRequests(f2, t2)
	if estCross < 55 || estCross > 65 {
		t.Errorf("expected ~60 requests for 10:30-11:30, got %d", estCross)
	}

	// 3. Same hour partial: 10:15 to 10:45 -> ~30 requests
	f3 := time.Date(2026, 11, 24, 10, 15, 0, 0, time.UTC)
	t3 := time.Date(2026, 11, 24, 10, 45, 0, 0, time.UTC)
	estSame := readStore.EstimateHTTPRequests(f3, t3)
	if estSame < 27 || estSame > 33 {
		t.Errorf("expected ~30 requests for 10:15-10:45, got %d", estSame)
	}

	// 4. from after to -> 0
	if readStore.EstimateHTTPRequests(t1, f1) != 0 {
		t.Errorf("expected 0 for reversed time range")
	}
}

func TestRangeReaderFollow(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	srcIP := net.ParseIP("192.168.1.50")
	dstIP := net.ParseIP("10.0.0.1")

	// 1. Create two pre-existing historical buckets (Hour 10 and Hour 11)
	t10 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	t11 := time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC)
	t12 := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 2; i++ {
		if err := store.WriteUDP(t10.Add(time.Duration(i)*time.Second), srcIP, dstIP, 1234, 2055, []byte("hour-10-pkt")); err != nil {
			t.Fatal(err)
		}
		if err := store.WriteUDP(t11.Add(time.Duration(i)*time.Second), srcIP, dstIP, 1234, 2055, []byte("hour-11-pkt")); err != nil {
			t.Fatal(err)
		}
	}
	_ = store.CloseHour()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Open range follower starting from Hour 10
	rr, err := store.OpenRangeFollow(ctx, t10)
	if err != nil {
		t.Fatalf("OpenRangeFollow failed: %v", err)
	}
	defer rr.Close()

	// Verify reading across multi-bucket historical backlog (Hour 10 and Hour 11)
	for i := 0; i < 2; i++ {
		rec, err := rr.NextUDP()
		if err != nil || string(rec.Payload) != "hour-10-pkt" {
			t.Fatalf("expected hour-10-pkt, got %v (err=%v)", rec, err)
		}
	}
	for i := 0; i < 2; i++ {
		rec, err := rr.NextUDP()
		if err != nil || string(rec.Payload) != "hour-11-pkt" {
			t.Fatalf("expected hour-11-pkt, got %v (err=%v)", rec, err)
		}
	}

	// 3. Dynamically write into a brand new hour (Hour 12) in the background
	go func() {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < 2; i++ {
			_ = store.WriteUDP(t12.Add(time.Duration(i)*time.Second), srcIP, dstIP, 1234, 2055, []byte("hour-12-live-pkt"))
		}
	}()

	// 4. Follower should seamlessly transition into Hour 12 and read the live packets
	for i := 0; i < 2; i++ {
		rec, err := rr.NextUDP()
		if err != nil || string(rec.Payload) != "hour-12-live-pkt" {
			t.Fatalf("expected hour-12-live-pkt, got %v (err=%v)", rec, err)
		}
	}

	// 5. Cancel context; NextUDP should unblock and return context.Canceled
	cancel()
	_, err = rr.NextUDP()
	if err == nil {
		t.Fatal("expected error after context cancellation, got nil")
	}
}

func TestBucketDiskBytesAndFormatBytes(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		SpoolDir: tmpDir,
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	t1 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	srcIP := net.ParseIP("192.168.1.10")
	dstIP := net.ParseIP("10.0.0.1")

	for i := 0; i < 10; i++ {
		if err := store.WriteUDP(t1.Add(time.Duration(i)*time.Second), srcIP, dstIP, 1234, 2055, []byte("disk-byte-test-payload")); err != nil {
			t.Fatalf("WriteUDP failed: %v", err)
		}
	}
	if err := store.CloseHour(); err != nil {
		t.Fatalf("CloseHour failed: %v", err)
	}

	buckets, err := store.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(buckets))
	}

	if buckets[0].DiskBytes <= 0 {
		t.Errorf("expected DiskBytes > 0, got %d", buckets[0].DiskBytes)
	}

	totalDisk, err := store.TotalDiskUsage()
	if err != nil {
		t.Fatalf("TotalDiskUsage failed: %v", err)
	}
	if totalDisk != buckets[0].DiskBytes {
		t.Errorf("expected TotalDiskUsage %d, got %d", buckets[0].DiskBytes, totalDisk)
	}

	// Verify FormatBytes
	cases := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{10485760, "10.0 MB"},
		{1073741824, "1.0 GB"},
	}
	for _, c := range cases {
		out := storage.FormatBytes(c.input)
		if out != c.expected {
			t.Errorf("FormatBytes(%d) = %s; want %s", c.input, out, c.expected)
		}
	}
}

func TestHourlyStoreSyncMeta(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	t1 := time.Date(2026, 11, 25, 8, 10, 0, 0, time.UTC)
	srcIP := net.ParseIP("192.168.1.50")
	dstIP := net.ParseIP("10.0.0.1")

	// Write 1 UDP and 1 HTTP without closing hour
	if err := store.WriteUDP(t1, srcIP, dstIP, 1234, 2055, []byte("sync-test-udp")); err != nil {
		t.Fatalf("WriteUDP failed: %v", err)
	}
	if err := store.WriteHTTP(t1, srcIP, "POST", "/events", map[string][]string{}, []byte(`{"test":true}`)); err != nil {
		t.Fatalf("WriteHTTP failed: %v", err)
	}

	metaPath := filepath.Join(tmpDir, "2026-11-25", "08", "meta.json")
	// Before SyncMeta, meta.json should not yet reflect the writes (or file doesn't exist)
	if data, err := os.ReadFile(metaPath); err == nil {
		if strings.Contains(string(data), `"udp_packets": 1`) {
			t.Fatal("expected meta.json not to contain udp_packets before SyncMeta")
		}
	}

	// Trigger SyncMeta
	if err := store.SyncMeta(); err != nil {
		t.Fatalf("SyncMeta failed: %v", err)
	}

	// Now meta.json must exist and reflect 1 UDP and 1 HTTP
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("failed to read meta.json after SyncMeta: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, `"udp_packets": 1`) {
		t.Errorf("expected udp_packets 1 in meta.json, got:\n%s", content)
	}
	if !strings.Contains(content, `"http_requests": 1`) {
		t.Errorf("expected http_requests 1 in meta.json, got:\n%s", content)
	}
	if !strings.Contains(content, "192.168.1.50") {
		t.Errorf("expected exporter IP in meta.json, got:\n%s", content)
	}
}

func TestSyslogHourlyStore(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	clientIP := net.ParseIP("10.10.10.20")
	t10 := time.Date(2026, 11, 25, 10, 15, 0, 0, time.UTC)
	t11 := time.Date(2026, 11, 25, 11, 45, 0, 0, time.UTC)

	msg1 := []byte("<14>Nov 25 10:15:00 test: msg 1")
	msg2 := []byte("<14>Nov 25 10:15:01 test: msg 2")
	msg3 := []byte("<14>Nov 25 11:45:00 test: msg 3")

	if err := store.WriteSyslog(t10, clientIP, "tcp", msg1); err != nil {
		t.Fatalf("WriteSyslog 1 failed: %v", err)
	}
	if err := store.WriteSyslog(t10.Add(time.Second), clientIP, "udp", msg2); err != nil {
		t.Fatalf("WriteSyslog 2 failed: %v", err)
	}
	if err := store.WriteSyslog(t11, clientIP, "tcp", msg3); err != nil {
		t.Fatalf("WriteSyslog 3 failed: %v", err)
	}

	if err := store.CloseHour(); err != nil {
		t.Fatalf("CloseHour failed: %v", err)
	}

	// Verify BucketMeta
	buckets, err := store.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(buckets))
	}
	if buckets[0].SyslogMessages != 2 {
		t.Errorf("expected 2 syslog messages in hour 10, got %d", buckets[0].SyslogMessages)
	}
	if buckets[0].SyslogBytes != uint64(len(msg1)+len(msg2)) {
		t.Errorf("expected %d syslog bytes, got %d", len(msg1)+len(msg2), buckets[0].SyslogBytes)
	}
	if buckets[1].SyslogMessages != 1 {
		t.Errorf("expected 1 syslog message in hour 11, got %d", buckets[1].SyslogMessages)
	}

	// Verify EstimateSyslogMessages
	est := store.EstimateSyslogMessages(t10.Add(-time.Hour), t11.Add(time.Hour))
	if est != 3 {
		t.Errorf("expected estimate 3 messages, got %d", est)
	}

	// Verify SyslogRangeReader
	reader, err := store.NewSyslogRangeReader(t10.Add(-time.Minute), t11.Add(time.Minute))
	if err != nil {
		t.Fatalf("NewSyslogRangeReader failed: %v", err)
	}
	defer reader.Close()

	rec1, err := reader.Next()
	if err != nil || string(rec1.Message) != string(msg1) || rec1.Transport != "tcp" {
		t.Fatalf("expected msg1 with transport tcp, got %v (err=%v)", rec1, err)
	}
	rec2, err := reader.Next()
	if err != nil || string(rec2.Message) != string(msg2) || rec2.Transport != "udp" {
		t.Fatalf("expected msg2 with transport udp, got %v (err=%v)", rec2, err)
	}
	rec3, err := reader.Next()
	if err != nil || string(rec3.Message) != string(msg3) || rec3.Transport != "tcp" {
		t.Fatalf("expected msg3 with transport tcp, got %v (err=%v)", rec3, err)
	}
	_, err = reader.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}

	// Verify NewSyslogRangeReader with zero toTimeUTC (unbounded)
	unboundedReader, err := store.NewSyslogRangeReader(t10.Add(-time.Minute), time.Time{})
	if err != nil {
		t.Fatalf("NewSyslogRangeReader unbounded failed: %v", err)
	}
	defer unboundedReader.Close()

	u1, err := unboundedReader.Next()
	if err != nil || string(u1.Message) != string(msg1) {
		t.Fatalf("expected msg1 on unbounded reader, got %v (err=%v)", u1, err)
	}
}


