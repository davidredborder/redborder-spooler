package capture

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

func buildRawUDPPacket(srcIP, dstIP net.IP, srcPort, dstPort uint16, payload []byte) []byte {
	buf := make([]byte, 14+20+8+len(payload))

	// Ethernet
	copy(buf[0:6], []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55})
	copy(buf[6:12], []byte{0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb})
	binary.BigEndian.PutUint16(buf[12:14], 0x0800) // IPv4

	// IPv4 Header
	buf[14] = 0x45
	buf[15] = 0x00
	totalLen := uint16(20 + 8 + len(payload))
	binary.BigEndian.PutUint16(buf[16:18], totalLen)
	buf[22] = 64 // TTL
	buf[23] = 17 // Protocol UDP
	copy(buf[26:30], srcIP.To4())
	copy(buf[30:34], dstIP.To4())

	// UDP Header
	binary.BigEndian.PutUint16(buf[34:36], srcPort)
	binary.BigEndian.PutUint16(buf[36:38], dstPort)
	binary.BigEndian.PutUint16(buf[38:40], uint16(8+len(payload)))

	// Payload
	copy(buf[42:], payload)

	return buf
}

func buildRawUDPIPv6Packet(srcIP, dstIP net.IP, srcPort, dstPort uint16, payload []byte) []byte {
	buf := make([]byte, 14+40+8+len(payload))

	// Ethernet
	copy(buf[0:6], []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55})
	copy(buf[6:12], []byte{0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb})
	binary.BigEndian.PutUint16(buf[12:14], 0x86DD) // IPv6

	// IPv6 Header (40 bytes)
	buf[14] = 0x60 // Version 6
	binary.BigEndian.PutUint16(buf[18:20], uint16(8+len(payload))) // Payload length
	buf[20] = 17 // NextHeader: UDP
	buf[21] = 64 // HopLimit
	copy(buf[22:38], srcIP.To16())
	copy(buf[38:54], dstIP.To16())

	// UDP Header
	binary.BigEndian.PutUint16(buf[54:56], srcPort)
	binary.BigEndian.PutUint16(buf[56:58], dstPort)
	binary.BigEndian.PutUint16(buf[58:60], uint16(8+len(payload)))

	// Payload
	copy(buf[62:], payload)

	return buf
}

func TestAFPacketDropLoopbackAndRBRP(t *testing.T) {
	listener := NewAFPacketListener([]string{"any"}, []int{2055}, nil, 0)

	// 1. Normal packet should pass
	normalPkt := buildRawUDPPacket(
		net.ParseIP("192.168.1.100"),
		net.ParseIP("10.0.0.1"),
		12345,
		2055,
		[]byte("valid-flow-packet"),
	)
	pkt, ok := listener.parsePacket(normalPkt, DirectionHost)
	if !ok || pkt == nil {
		t.Fatal("expected normal packet to be accepted")
	}

	// 2. Loopback src IPv4 should be dropped
	loopbackSrcPkt := buildRawUDPPacket(
		net.ParseIP("127.0.0.1"),
		net.ParseIP("10.0.0.1"),
		12345,
		2055,
		[]byte("loopback-src-packet"),
	)
	pkt, ok = listener.parsePacket(loopbackSrcPkt, DirectionHost)
	if ok || pkt != nil {
		t.Fatal("expected loopback src packet to be dropped")
	}

	// 3. Loopback dst IPv4 should be dropped
	loopbackDstPkt := buildRawUDPPacket(
		net.ParseIP("192.168.1.100"),
		net.ParseIP("127.0.0.1"),
		12345,
		2055,
		[]byte("loopback-dst-packet"),
	)
	pkt, ok = listener.parsePacket(loopbackDstPkt, DirectionHost)
	if ok || pkt != nil {
		t.Fatal("expected loopback dst packet to be dropped")
	}

	// 4. IPv6 loopback (::1) should be dropped
	ipv6Loopback := buildRawUDPIPv6Packet(
		net.ParseIP("::1"),
		net.ParseIP("2001:db8::1"),
		12345,
		2055,
		[]byte("ipv6-loopback-packet"),
	)
	pkt, ok = listener.parsePacket(ipv6Loopback, DirectionHost)
	if ok || pkt != nil {
		t.Fatal("expected IPv6 loopback packet to be dropped")
	}

	// 5. Packet with full WrapRBRP envelope should be dropped
	rbrpEnvelope := storage.WrapRBRP([]byte("netflow-data"), time.Now().UTC(), net.ParseIP("192.168.1.5"))
	rbrpPkt := buildRawUDPPacket(
		net.ParseIP("192.168.1.100"),
		net.ParseIP("10.0.0.1"),
		12345,
		2055,
		rbrpEnvelope,
	)
	pkt, ok = listener.parsePacket(rbrpPkt, DirectionHost)
	if ok || pkt != nil {
		t.Fatal("expected RBRP-wrapped packet to be dropped")
	}
}

func TestUDPSocketFilterLoopback(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Find available UDP port
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	_ = conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. With FilterLoopback = true (default), loopback packet should be dropped
	listener := NewUDPSocketListener([]int{port}, "127.0.0.1", store, 0)
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("listener.Start failed: %v", err)
	}
	defer listener.Stop()

	clientConn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	if _, err := clientConn.Write([]byte("loopback-test")); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)
	_ = store.CloseHour()

	buckets, err := store.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) > 0 && buckets[0].UDPPackets > 0 {
		t.Errorf("expected 0 packets recorded when FilterLoopback=true, got %d", buckets[0].UDPPackets)
	}
}

func TestUDPSocketDropRBRP(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

	// Find available UDP port
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	_ = conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener := NewUDPSocketListener([]int{port}, "127.0.0.1", store, 0)
	listener.FilterLoopback = false // Disable loopback filter to isolate RBRP check
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("listener.Start failed: %v", err)
	}
	defer listener.Stop()

	clientConn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	// Send an RBRP envelope packet
	rbrpEnvelope := storage.WrapRBRP([]byte("test-data"), time.Now().UTC(), net.ParseIP("192.168.1.1"))
	if _, err := clientConn.Write(rbrpEnvelope); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)
	_ = store.CloseHour()

	buckets, err := store.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) > 0 && buckets[0].UDPPackets > 0 {
		t.Errorf("expected 0 packets recorded for RBRP envelope, got %d", buckets[0].UDPPackets)
	}
}

func TestDedupDropsOutgoingWhenWindowZero(t *testing.T) {
	var delivered []*CapturedPacket
	handler := func(pkt *CapturedPacket) {
		delivered = append(delivered, pkt)
	}

	ring := NewDedupRing(0, handler)
	defer ring.Stop()

	// Push outgoing packet
	pkt := &CapturedPacket{
		CaptureTimeUTC: time.Now().UTC(),
		SrcIP:          net.ParseIP("192.168.1.1"),
		DstIP:          net.ParseIP("10.0.0.1"),
		SrcPort:        1234,
		DstPort:        2055,
		Payload:        []byte("outgoing-packet"),
		Direction:      DirectionOutgoing,
	}

	ring.Push(pkt)

	if len(delivered) != 0 {
		t.Fatalf("expected 0 delivered outgoing packets when window=0, got %d", len(delivered))
	}
}

func TestHTTPSpoolerDropsReplay(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{SpoolDir: tmpDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}
	defer store.Close()

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

	spooler := NewHTTPSpooler(httpCfg, store)
	if err := spooler.Start(); err != nil {
		t.Fatalf("spooler.Start failed: %v", err)
	}
	defer spooler.Stop()

	// Send POST with X-Redborder-Replay: true
	req, err := http.NewRequest("POST", "http://"+addr+"/rbdata/events", bytes.NewReader([]byte(`{"test":"replay"}`)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Redborder-Replay", "true")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	_ = resp.Body.Close()

	time.Sleep(50 * time.Millisecond)
	_ = store.CloseHour()

	buckets, err := store.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) > 0 && buckets[0].HTTPRequests > 0 {
		t.Errorf("expected 0 HTTP requests recorded when X-Redborder-Replay=true, got %d", buckets[0].HTTPRequests)
	}
}
