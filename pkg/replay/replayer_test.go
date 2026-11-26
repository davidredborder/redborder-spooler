package replay_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/replay"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

func makeV9TemplatePacket(sourceID uint32) []byte {
	buf := make([]byte, 20+12)
	binary.BigEndian.PutUint16(buf[0:2], 9)           // NetFlow v9
	binary.BigEndian.PutUint16(buf[2:4], 1)           // Count
	binary.BigEndian.PutUint32(buf[4:8], 1000)        // SysUptime
	binary.BigEndian.PutUint32(buf[8:12], 1700000000) // UnixSecs
	binary.BigEndian.PutUint32(buf[12:16], 1)         // Seq
	binary.BigEndian.PutUint32(buf[16:20], sourceID)  // SourceID

	// Template FlowSet (FlowSet ID = 0)
	binary.BigEndian.PutUint16(buf[20:22], 0)   // FlowSet ID 0 = Template
	binary.BigEndian.PutUint16(buf[22:24], 12)  // FlowSet Length = 12
	binary.BigEndian.PutUint16(buf[24:26], 256) // Template ID = 256
	binary.BigEndian.PutUint16(buf[26:28], 1)   // Field count = 1
	binary.BigEndian.PutUint16(buf[28:30], 8)   // Field type 8 (IPV4_SRC_ADDR)
	binary.BigEndian.PutUint16(buf[30:32], 4)   // Field length = 4
	return buf
}

func TestReplayerUDPWithRBRPAndTemplates(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		SpoolDir: tmpDir,
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Create a test UDP listener to receive replayed packets
	recvConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer recvConn.Close()
	targetPort := recvConn.LocalAddr().(*net.UDPAddr).Port

	t1 := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 11, 24, 10, 0, 1, 0, time.UTC)
	srcIP := net.ParseIP("192.168.100.55")
	dstIP := net.ParseIP("10.0.0.1")

	// 1. Write NetFlow v9 template packet to trigger template tracker
	templatePkt := makeV9TemplatePacket(42)
	if err := store.WriteUDP(t1, srcIP, dstIP, 54321, uint16(targetPort), templatePkt); err != nil {
		t.Fatal(err)
	}

	// 2. Write regular flow packets
	if err := store.WriteUDP(t1, srcIP, dstIP, 54321, uint16(targetPort), []byte("packet-1")); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteUDP(t2, srcIP, dstIP, 54321, uint16(targetPort), []byte("packet-2")); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()

	// Check that templates.snap was generated
	snapFile := filepath.Join(tmpDir, "2026-11-24", "10", storage.TemplateSnapName)
	if _, err := os.Stat(snapFile); err != nil {
		t.Fatalf("expected %s to exist: %v", snapFile, err)
	}

	// Receive packets in background
	var receivedPackets [][]byte
	var mu sync.Mutex
	doneReceiving := make(chan struct{})

	go func() {
		buf := make([]byte, 2048)
		for {
			_ = recvConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			n, _, err := recvConn.ReadFromUDP(buf)
			if err != nil {
				close(doneReceiving)
				return
			}
			cp := make([]byte, n)
			copy(cp, buf[:n])
			mu.Lock()
			receivedPackets = append(receivedPackets, cp)
			mu.Unlock()
		}
	}()

	replayer := replay.NewReplayer(store)
	opts := replay.Options{
		FromTimeUTC:     t1.Add(-time.Minute),
		ToTimeUTC:       t2.Add(time.Minute),
		SpeedMultiplier: 0, // Unthrottled burst
		TargetHost:      "127.0.0.1",
		UseRBRP:         true,
	}

	stats, err := replayer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Replayer.Run failed: %v", err)
	}

	if stats.TemplatesSent != 1 {
		t.Errorf("expected 1 template sent, got %d", stats.TemplatesSent)
	}
	if stats.PacketsReplayed != 3 {
		t.Errorf("expected 3 packets replayed (template + 2 packets), got %d", stats.PacketsReplayed)
	}

	<-doneReceiving

	mu.Lock()
	defer mu.Unlock()

	// Total packets received = 1 pre-transmitted template + 3 replayed WAL packets = 4 packets
	if len(receivedPackets) != 4 {
		t.Fatalf("expected 4 received packets, got %d", len(receivedPackets))
	}

	// Verify that the first packet received is the pre-transmitted template with original ExporterIP!
	preTmplPayload, _, origIP, ok := storage.UnwrapRBRP(receivedPackets[0])
	if !ok {
		t.Fatal("pre-transmitted template is not in valid RBRP envelope")
	}
	if !origIP.Equal(srcIP) {
		t.Errorf("template exporter IP mismatch: got %v, want %v", origIP, srcIP)
	}
	if len(preTmplPayload) != len(templatePkt) {
		t.Errorf("template payload length mismatch: got %d, want %d", len(preTmplPayload), len(templatePkt))
	}

	// Verify packet-1 and packet-2
	p1Payload, _, p1IP, ok := storage.UnwrapRBRP(receivedPackets[2])
	if !ok || string(p1Payload) != "packet-1" || !p1IP.Equal(srcIP) {
		t.Errorf("packet-1 mismatch: ok=%v, payload=%s, ip=%v", ok, string(p1Payload), p1IP)
	}

	p2Payload, _, p2IP, ok := storage.UnwrapRBRP(receivedPackets[3])
	if !ok || string(p2Payload) != "packet-2" || !p2IP.Equal(srcIP) {
		t.Errorf("packet-2 mismatch: ok=%v, payload=%s, ip=%v", ok, string(p2Payload), p2IP)
	}
}

func TestReplayerHTTP(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		SpoolDir: tmpDir,
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	var receivedBody string
	var receivedURI string
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		receivedURI = r.URL.RequestURI()
		receivedBody = string(body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	t1 := time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC)
	clientIP := net.ParseIP("192.168.1.10")
	headers := map[string][]string{"Content-Type": {"application/json"}}

	if err := store.WriteHTTP(t1, clientIP, "POST", "/events/stream", headers, []byte(`{"replayed":true}`)); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()

	replayer := replay.NewReplayer(store)
	opts := replay.Options{
		FromTimeUTC:     t1.Add(-time.Minute),
		ToTimeUTC:       t1.Add(time.Minute),
		SpeedMultiplier: 0,
		TargetHost:      "127.0.0.1",
		HTTPTargetURL:   ts.URL,
	}

	stats, err := replayer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Replayer.Run failed: %v", err)
	}

	if stats.HTTPRequestsReplayed != 1 {
		t.Errorf("expected 1 HTTP request replayed, got %d", stats.HTTPRequestsReplayed)
	}

	mu.Lock()
	defer mu.Unlock()
	if receivedURI != "/events/stream" {
		t.Errorf("URI mismatch: got %s, want /events/stream", receivedURI)
	}
	if receivedBody != `{"replayed":true}` {
		t.Errorf("Body mismatch: got %s", receivedBody)
	}
}

func TestReplayerFollowMode(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	recvConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer recvConn.Close()
	targetPort := recvConn.LocalAddr().(*net.UDPAddr).Port

	t0 := time.Now().UTC().Add(-time.Hour)
	clientIP := net.ParseIP("192.168.1.10")
	if err := store.WriteUDP(t0, clientIP, net.ParseIP("10.0.0.1"), 1234, uint16(targetPort), []byte("historical-packet")); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replayer := replay.NewReplayer(store)
	opts := replay.Options{
		FromTimeUTC:     t0.Add(-time.Minute),
		SpeedMultiplier: 0,
		TargetHost:      "127.0.0.1",
		UseRBRP:         false,
		Follow:          true,
	}

	var stats replay.Stats
	doneCh := make(chan error, 1)
	go func() {
		var err error
		stats, err = replayer.Run(ctx, opts)
		doneCh <- err
	}()

	// 1. Receive historical packet
	buf := make([]byte, 2048)
	_ = recvConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := recvConn.ReadFrom(buf)
	if err != nil || string(buf[:n]) != "historical-packet" {
		t.Fatalf("failed reading historical packet: %v", err)
	}

	// 2. Write dynamic follow packet in background
	time.Sleep(100 * time.Millisecond)
	if err := store.WriteUDP(time.Now().UTC(), clientIP, net.ParseIP("10.0.0.1"), 1234, uint16(targetPort), []byte("live-follow-packet")); err != nil {
		t.Fatal(err)
	}

	// 3. Receive dynamic follow packet
	_ = recvConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err = recvConn.ReadFrom(buf)
	if err != nil || string(buf[:n]) != "live-follow-packet" {
		t.Fatalf("failed reading follow packet: %v", err)
	}

	// 4. Cancel context to end follower
	cancel()
	select {
	case err := <-doneCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("replayer exited with unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replayer did not exit after context cancellation")
	}

	if stats.PacketsReplayed != 2 {
		t.Errorf("expected 2 packets replayed, got %d", stats.PacketsReplayed)
	}
}

func TestReplayerSelectiveType(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	t1 := time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC)
	srcIP := net.ParseIP("192.168.1.50")
	dstIP := net.ParseIP("10.0.0.1")

	// Set up mock HTTP target
	var httpReqCount int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		httpReqCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Set up mock UDP receiver
	recvConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer recvConn.Close()
	targetPort := recvConn.LocalAddr().(*net.UDPAddr).Port

	// Write 1 UDP and 1 HTTP
	if err := store.WriteUDP(t1, srcIP, dstIP, 1234, uint16(targetPort), []byte("test-udp")); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteHTTP(t1, srcIP, "POST", "/events", nil, []byte(`{"event":"test"}`)); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()

	rep := replay.NewReplayer(store)

	// 1. ReplayType == "http": should only send HTTP
	mu.Lock()
	httpReqCount = 0
	mu.Unlock()
	statsHTTP, err := rep.Run(context.Background(), replay.Options{
		FromTimeUTC:   t1.Add(-time.Minute),
		ToTimeUTC:     t1.Add(time.Minute),
		TargetHost:    "127.0.0.1",
		HTTPTargetURL: srv.URL,
		ReplayType:    "http",
	})
	if err != nil {
		t.Fatalf("ReplayType http failed: %v", err)
	}
	if statsHTTP.PacketsReplayed != 0 {
		t.Errorf("expected 0 UDP packets replayed for ReplayType http, got %d", statsHTTP.PacketsReplayed)
	}
	if statsHTTP.HTTPRequestsReplayed != 1 {
		t.Errorf("expected 1 HTTP request replayed for ReplayType http, got %d", statsHTTP.HTTPRequestsReplayed)
	}

	// 2. ReplayType == "udp": should only send UDP
	mu.Lock()
	httpReqCount = 0
	mu.Unlock()
	statsUDP, err := rep.Run(context.Background(), replay.Options{
		FromTimeUTC:   t1.Add(-time.Minute),
		ToTimeUTC:     t1.Add(time.Minute),
		TargetHost:    "127.0.0.1",
		HTTPTargetURL: srv.URL,
		ReplayType:    "udp",
	})
	if err != nil {
		t.Fatalf("ReplayType udp failed: %v", err)
	}
	if statsUDP.PacketsReplayed != 1 {
		t.Errorf("expected 1 UDP packet replayed for ReplayType udp, got %d", statsUDP.PacketsReplayed)
	}
	if statsUDP.HTTPRequestsReplayed != 0 {
		t.Errorf("expected 0 HTTP requests replayed for ReplayType udp, got %d", statsUDP.HTTPRequestsReplayed)
	}
	if httpReqCount != 0 {
		t.Errorf("expected HTTP server to receive 0 requests for ReplayType udp, got %d", httpReqCount)
	}

	// 3. ReplayType == "all": should send both UDP and HTTP and notify progress for both
	mu.Lock()
	httpReqCount = 0
	mu.Unlock()
	var lastReportedProgress uint64
	statsAll, err := rep.Run(context.Background(), replay.Options{
		FromTimeUTC:   t1.Add(-time.Minute),
		ToTimeUTC:     t1.Add(time.Minute),
		TargetHost:    "127.0.0.1",
		HTTPTargetURL: srv.URL,
		ReplayType:    "all",
		OnProgress: func(pkts uint64) {
			lastReportedProgress = pkts
		},
	})
	if err != nil {
		t.Fatalf("ReplayType all failed: %v", err)
	}
	if statsAll.PacketsReplayed != 1 {
		t.Errorf("expected 1 UDP packet replayed for ReplayType all, got %d", statsAll.PacketsReplayed)
	}
	if statsAll.HTTPRequestsReplayed != 1 {
		t.Errorf("expected 1 HTTP request replayed for ReplayType all, got %d", statsAll.HTTPRequestsReplayed)
	}
	if lastReportedProgress != 2 {
		t.Errorf("expected last reported progress to be 2 (1 UDP + 1 HTTP), got %d", lastReportedProgress)
	}
}

func TestReplayerSyslogTCP(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Mock TCP Syslog Receiver
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock syslog listener: %v", err)
	}
	defer ln.Close()

	tcpAddr := ln.Addr().(*net.TCPAddr)

	var receivedMessages []string
	var mu sync.Mutex
	doneReceiving := make(chan struct{})

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			close(doneReceiving)
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if len(line) > 0 {
				mu.Lock()
				receivedMessages = append(receivedMessages, strings.TrimRight(line, "\r\n"))
				mu.Unlock()
			}
			if err != nil {
				close(doneReceiving)
				return
			}
		}
	}()

	t1 := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	clientIP := net.ParseIP("192.168.1.100")
	msg1 := "<14>Nov 25 09:00:00 app syslog msg 1"
	msg2 := "<14>Nov 25 09:00:01 app syslog msg 2"

	if err := store.WriteSyslog(t1, clientIP, "tcp", []byte(msg1)); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteSyslog(t1.Add(time.Second), clientIP, "tcp", []byte(msg2)); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()

	rep := replay.NewReplayer(store)
	stats, err := rep.Run(context.Background(), replay.Options{
		FromTimeUTC:      t1.Add(-time.Minute),
		ToTimeUTC:        t1.Add(time.Minute),
		SpeedMultiplier:  0,
		SyslogTargetHost: tcpAddr.IP.String(),
		SyslogTargetPort: tcpAddr.Port,
		ReplayType:       "syslog",
	})
	if err != nil {
		t.Fatalf("replay Run failed: %v", err)
	}

	if stats.SyslogMessagesReplayed != 2 {
		t.Errorf("expected 2 syslog messages replayed, got %d", stats.SyslogMessagesReplayed)
	}

	// Wait or sleep briefly to ensure mock server read lines
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedMessages) != 2 {
		t.Fatalf("expected 2 messages received, got %d", len(receivedMessages))
	}
	for _, m := range receivedMessages {
		if !strings.Contains(m, "X-Redborder-Replay") {
			t.Errorf("expected replayed message to contain X-Redborder-Replay marker, got %q", m)
		}
	}
}

func TestReplayerSyslogUDP(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Mock UDP Syslog listener
	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket failed: %v", err)
	}
	defer udpConn.Close()

	udpAddr := udpConn.LocalAddr().(*net.UDPAddr)

	var receivedMessages []string
	var mu sync.Mutex
	doneReceiving := make(chan struct{})

	go func() {
		buf := make([]byte, 4096)
		for {
			n, _, err := udpConn.ReadFrom(buf)
			if n > 0 {
				mu.Lock()
				receivedMessages = append(receivedMessages, string(buf[:n]))
				count := len(receivedMessages)
				mu.Unlock()
				if count == 2 {
					close(doneReceiving)
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	t1 := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	clientIP := net.ParseIP("192.168.1.100")
	msg1 := "<14>Nov 25 09:00:00 app udp syslog msg 1"
	msg2 := "<14>Nov 25 09:00:01 app udp syslog msg 2"

	if err := store.WriteSyslog(t1, clientIP, "udp", []byte(msg1)); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteSyslog(t1.Add(time.Second), clientIP, "udp", []byte(msg2)); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()

	rep := replay.NewReplayer(store)
	stats, err := rep.Run(context.Background(), replay.Options{
		FromTimeUTC:      t1.Add(-time.Minute),
		ToTimeUTC:        t1.Add(time.Minute),
		SpeedMultiplier:  0,
		SyslogTargetHost: udpAddr.IP.String(),
		SyslogTargetPort: udpAddr.Port,
		ReplayType:       "syslog",
	})
	if err != nil {
		t.Fatalf("replay Run failed: %v", err)
	}

	if stats.SyslogMessagesReplayed != 2 {
		t.Errorf("expected 2 syslog messages replayed, got %d", stats.SyslogMessagesReplayed)
	}

	select {
	case <-doneReceiving:
	case <-time.After(500 * time.Millisecond):
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedMessages) != 2 {
		t.Fatalf("expected 2 messages received, got %d", len(receivedMessages))
	}
	if receivedMessages[0] != msg1 {
		t.Errorf("msg1 mismatch, got %q, want %q", receivedMessages[0], msg1)
	}
	if receivedMessages[1] != msg2 {
		t.Errorf("msg2 mismatch, got %q, want %q", receivedMessages[1], msg2)
	}
}

func TestReplayerSyslogUDP_InjectClientIP(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Mock UDP Syslog listener
	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket failed: %v", err)
	}
	defer udpConn.Close()

	udpAddr := udpConn.LocalAddr().(*net.UDPAddr)

	var receivedMessages []string
	var mu sync.Mutex
	doneReceiving := make(chan struct{})

	go func() {
		buf := make([]byte, 2048)
		for {
			_ = udpConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			n, _, err := udpConn.ReadFrom(buf)
			if err != nil {
				close(doneReceiving)
				return
			}
			mu.Lock()
			receivedMessages = append(receivedMessages, string(buf[:n]))
			mu.Unlock()
		}
	}()

	t1 := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	clientIP := net.ParseIP("10.1.33.158")
	rawMsg := "<14>Nov 25 14:13:00 debianDavid Fortitray.desktop[3620503]: /opt/forticlient: error"

	if err := store.WriteSyslog(t1, clientIP, "udp", []byte(rawMsg)); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()

	rep := replay.NewReplayer(store)
	stats, err := rep.Run(context.Background(), replay.Options{
		FromTimeUTC:          t1.Add(-time.Minute),
		ToTimeUTC:            t1.Add(time.Minute),
		SpeedMultiplier:      0,
		SyslogTargetHost:     udpAddr.IP.String(),
		SyslogTargetPort:     udpAddr.Port,
		SyslogInjectClientIP: true,
		ReplayType:           "syslog",
	})
	if err != nil {
		t.Fatalf("replay Run failed: %v", err)
	}

	if stats.SyslogMessagesReplayed != 1 {
		t.Errorf("expected 1 syslog message replayed, got %d", stats.SyslogMessagesReplayed)
	}

	select {
	case <-doneReceiving:
	case <-time.After(500 * time.Millisecond):
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedMessages) != 1 {
		t.Fatalf("expected 1 message received, got %d", len(receivedMessages))
	}

	expected := "<14>Nov 25 14:13:00 10.1.33.158 Fortitray.desktop[3620503]: /opt/forticlient: error"
	if receivedMessages[0] != expected {
		t.Errorf("message mismatch:\ngot:  %q\nwant: %q", receivedMessages[0], expected)
	}
}

func TestReplayerAllTypeSkipsSyslogWhenNoSyslogRecorded(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	t1 := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	// Write only UDP packet
	if err := store.WriteUDP(t1, net.ParseIP("10.0.0.1"), net.ParseIP("10.0.0.2"), 1234, 2055, []byte("test-udp")); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()

	rep := replay.NewReplayer(store)
	// ReplayType == "all", but with a closed/non-listening syslog port
	stats, err := rep.Run(context.Background(), replay.Options{
		FromTimeUTC:      t1.Add(-time.Minute),
		ToTimeUTC:        t1.Add(time.Minute),
		SpeedMultiplier:  0,
		TargetHost:       "127.0.0.1",
		SyslogTargetHost: "127.0.0.1",
		SyslogTargetPort: 54329, // port with nothing listening
		ReplayType:       "all",
	})
	if err != nil {
		t.Fatalf("expected ReplayType 'all' to succeed when 0 syslog records, but got error: %v", err)
	}
	if stats.SyslogMessagesReplayed != 0 {
		t.Errorf("expected 0 syslog messages replayed, got %d", stats.SyslogMessagesReplayed)
	}
	if stats.PacketsReplayed != 1 {
		t.Errorf("expected 1 UDP packet replayed, got %d", stats.PacketsReplayed)
	}
}

func TestReplayerUDPPortsFilter(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Listener for port A
	recvConnA, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer recvConnA.Close()
	portA := uint16(recvConnA.LocalAddr().(*net.UDPAddr).Port)

	// Listener for port B
	recvConnB, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer recvConnB.Close()
	portB := uint16(recvConnB.LocalAddr().(*net.UDPAddr).Port)

	t1 := time.Date(2026, 11, 25, 9, 30, 0, 0, time.UTC)
	srcIP := net.ParseIP("10.0.0.1")
	dstIP := net.ParseIP("10.0.0.2")

	// Write packet for portA and packet for portB
	if err := store.WriteUDP(t1, srcIP, dstIP, 1234, portA, []byte("packet-port-a")); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteUDP(t1.Add(time.Second), srcIP, dstIP, 1234, portB, []byte("packet-port-b")); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()

	rep := replay.NewReplayer(store)

	// 1. PortsFilter containing only portA: should only replay packet-port-a
	statsFiltered, err := rep.Run(context.Background(), replay.Options{
		FromTimeUTC:     t1.Add(-time.Minute),
		ToTimeUTC:       t1.Add(time.Minute),
		SpeedMultiplier: 0,
		TargetHost:      "127.0.0.1",
		PortsFilter:     []uint16{portA},
		ReplayType:      "traffic",
		UseRBRP:         true,
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if statsFiltered.PacketsReplayed != 1 {
		t.Errorf("expected 1 packet replayed with filter, got %d", statsFiltered.PacketsReplayed)
	}

	buf := make([]byte, 1024)
	_ = recvConnA.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, _, err := recvConnA.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("failed reading filtered packet from recvConnA: %v", err)
	}
	unwrapped, _, _, ok := storage.UnwrapRBRP(buf[:n])
	if !ok || string(unwrapped) != "packet-port-a" {
		t.Errorf("packet-port-a mismatch or not RBRP wrapped: %s", string(unwrapped))
	}

	// 2. PortsFilter containing portB
	statsB, err := rep.Run(context.Background(), replay.Options{
		FromTimeUTC:     t1.Add(-time.Minute),
		ToTimeUTC:       t1.Add(time.Minute),
		SpeedMultiplier: 0,
		TargetHost:      "127.0.0.1",
		PortsFilter:     []uint16{portB},
		ReplayType:      "traffic",
		UseRBRP:         true,
	})
	if err != nil {
		t.Fatalf("Replay portB failed: %v", err)
	}
	if statsB.PacketsReplayed != 1 {
		t.Errorf("expected 1 packet replayed for portB, got %d", statsB.PacketsReplayed)
	}

	bufB := make([]byte, 1024)
	_ = recvConnB.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	nB, _, err := recvConnB.ReadFromUDP(bufB)
	if err != nil {
		t.Fatalf("failed reading packet from recvConnB: %v", err)
	}

	unwrappedB, _, _, okB := storage.UnwrapRBRP(bufB[:nB])
	if !okB || string(unwrappedB) != "packet-port-b" {
		t.Errorf("expected packet-port-b, got %q", string(unwrappedB))
	}
}

func TestReplayer_TimeShift_RBRP(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		SpoolDir: tmpDir,
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	recvConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer recvConn.Close()
	targetPort := recvConn.LocalAddr().(*net.UDPAddr).Port

	// Write packet timestamped 5 hours ago
	histTime := time.Now().UTC().Add(-5 * time.Hour).Truncate(time.Second)
	srcIP := net.ParseIP("10.20.30.40")
	dstIP := net.ParseIP("10.0.0.1")

	if err := store.WriteUDP(histTime, srcIP, dstIP, 12345, uint16(targetPort), []byte("shift-me")); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()

	rep := replay.NewReplayer(store)

	// Replay with TimeShift: true
	startReplay := time.Now().UTC()
	_, err = rep.Run(context.Background(), replay.Options{
		FromTimeUTC:     histTime,
		ToTimeUTC:       histTime.Add(time.Minute),
		SpeedMultiplier: 0,
		TargetHost:      "127.0.0.1",
		ReplayType:      "traffic",
		UseRBRP:         true,
		TimeShift:       true,
	})
	if err != nil {
		t.Fatalf("Replay with TimeShift failed: %v", err)
	}

	buf := make([]byte, 1024)
	_ = recvConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, _, err := recvConn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("failed reading replayed packet: %v", err)
	}

	payload, shiftedTime, exporterIP, ok := storage.UnwrapRBRP(buf[:n])
	if !ok {
		t.Fatal("expected packet to be RBRP wrapped")
	}
	if string(payload) != "shift-me" {
		t.Errorf("payload mismatch: got %s, want shift-me", string(payload))
	}
	if !exporterIP.Equal(srcIP) {
		t.Errorf("exporter IP mismatch: got %v, want %v", exporterIP, srcIP)
	}

	// Verify shiftedTime is close to startReplay (within 5 seconds), NOT 5 hours ago
	if shiftedTime.Before(startReplay.Add(-5*time.Second)) || shiftedTime.After(time.Now().UTC().Add(5*time.Second)) {
		t.Errorf("expected shiftedTime near now (%v), got %v", startReplay, shiftedTime)
	}
}

func TestReplayerSyslogUDP_Resilience(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	t1 := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	clientIP := net.ParseIP("192.168.1.100")

	// Message 1: valid normal message
	if err := store.WriteSyslog(t1, clientIP, "udp", []byte("<14>valid msg")); err != nil {
		t.Fatal(err)
	}
	// Message 2: oversized payload (> 65507 bytes) that forces synchronous UDP write failure on Linux
	oversized := make([]byte, 70000)
	copy(oversized, []byte("<14>oversized "))
	if err := store.WriteSyslog(t1.Add(time.Second), clientIP, "udp", oversized); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()

	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udpConn.Close()
	port := udpConn.LocalAddr().(*net.UDPAddr).Port

	replayer := replay.NewReplayer(store)
	opts := replay.Options{
		FromTimeUTC:          t1.Add(-time.Minute),
		ToTimeUTC:            t1.Add(2 * time.Minute),
		SpeedMultiplier:      0,
		SyslogTargetHost:     "127.0.0.1",
		SyslogTargetPort:     port,
		SyslogInjectClientIP: false,
		ReplayType:           "syslog",
	}

	stats, err := replayer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("expected replayer to be resilient and not return fatal error, got: %v", err)
	}
	if stats.SyslogMessagesReplayed != 1 {
		t.Errorf("expected 1 replayed message, got %d", stats.SyslogMessagesReplayed)
	}
	if stats.SyslogMessagesDropped != 1 {
		t.Errorf("expected 1 dropped message due to write failure, got %d", stats.SyslogMessagesDropped)
	}
}
