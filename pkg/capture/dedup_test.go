package capture_test

import (
	"bytes"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/capture"
)

func TestDedupDeliveryWithoutOutgoing(t *testing.T) {
	var deliveredCount int32
	var lastPayload []byte
	var mu sync.Mutex

	handler := func(pkt *capture.CapturedPacket) {
		atomic.AddInt32(&deliveredCount, 1)
		mu.Lock()
		lastPayload = pkt.Payload
		mu.Unlock()
	}

	ring := capture.NewDedupRing(20*time.Millisecond, handler)
	defer ring.Stop()

	pkt := &capture.CapturedPacket{
		CaptureTimeUTC: time.Now().UTC(),
		SrcIP:          net.ParseIP("192.168.1.1"),
		DstIP:          net.ParseIP("10.0.0.1"),
		SrcPort:        1234,
		DstPort:        2055,
		UDPChecksum:    0xABCD,
		Payload:        []byte("hello netflow"),
		Direction:      capture.DirectionHost,
	}

	ring.Push(pkt)

	// Check immediately (should not be delivered yet)
	if atomic.LoadInt32(&deliveredCount) != 0 {
		t.Fatalf("expected 0 delivered immediately, got %d", atomic.LoadInt32(&deliveredCount))
	}

	// Wait for window to expire (>20ms)
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt32(&deliveredCount) != 1 {
		t.Fatalf("expected 1 delivered after timeout, got %d", atomic.LoadInt32(&deliveredCount))
	}

	mu.Lock()
	if !bytes.Equal(lastPayload, []byte("hello netflow")) {
		t.Errorf("payload mismatch: %s", string(lastPayload))
	}
	mu.Unlock()
}

func TestDedupCancellationOnOutgoing(t *testing.T) {
	var deliveredCount int32

	handler := func(pkt *capture.CapturedPacket) {
		atomic.AddInt32(&deliveredCount, 1)
	}

	ring := capture.NewDedupRing(30*time.Millisecond, handler)
	defer ring.Stop()

	pktHost := &capture.CapturedPacket{
		CaptureTimeUTC: time.Now().UTC(),
		SrcIP:          net.ParseIP("192.168.1.1"),
		DstIP:          net.ParseIP("10.0.0.1"),
		SrcPort:        1234,
		DstPort:        2055,
		UDPChecksum:    0xABCD,
		Payload:        []byte("forwarded packet"),
		Direction:      capture.DirectionHost,
	}

	ring.Push(pktHost)

	// 5ms later, seen as outgoing (LVS-DR forwarded to backup node)
	time.Sleep(5 * time.Millisecond)

	pktOutgoing := &capture.CapturedPacket{
		CaptureTimeUTC: time.Now().UTC(),
		SrcIP:          net.ParseIP("192.168.1.1"),
		DstIP:          net.ParseIP("10.0.0.1"),
		SrcPort:        1234,
		DstPort:        2055,
		UDPChecksum:    0xABCD,
		Payload:        []byte("forwarded packet"),
		Direction:      capture.DirectionOutgoing,
	}

	ring.Push(pktOutgoing)

	// Wait past the original 30ms window
	time.Sleep(50 * time.Millisecond)

	// Packet should have been canceled and NOT delivered
	if count := atomic.LoadInt32(&deliveredCount); count != 0 {
		t.Fatalf("expected 0 delivered (canceled by outgoing), got %d", count)
	}
}

func TestDedupDisabledWindowZero(t *testing.T) {
	var deliveredCount int32

	handler := func(pkt *capture.CapturedPacket) {
		atomic.AddInt32(&deliveredCount, 1)
	}

	// 0ms window means disabled dedup -> deliver immediately
	ring := capture.NewDedupRing(0, handler)
	defer ring.Stop()

	pkt := &capture.CapturedPacket{
		CaptureTimeUTC: time.Now().UTC(),
		SrcIP:          net.ParseIP("192.168.1.1"),
		DstIP:          net.ParseIP("10.0.0.1"),
		SrcPort:        1234,
		DstPort:        2055,
		Payload:        []byte("immediate"),
		Direction:      capture.DirectionHost,
	}

	ring.Push(pkt)

	if atomic.LoadInt32(&deliveredCount) != 1 {
		t.Fatalf("expected immediate delivery with 0ms window, got %d", atomic.LoadInt32(&deliveredCount))
	}
}

func TestDedupDuplicateHostPackets(t *testing.T) {
	var deliveredCount int32

	handler := func(pkt *capture.CapturedPacket) {
		atomic.AddInt32(&deliveredCount, 1)
	}

	ring := capture.NewDedupRing(25*time.Millisecond, handler)
	defer ring.Stop()

	pkt1 := &capture.CapturedPacket{
		CaptureTimeUTC: time.Now().UTC(),
		SrcIP:          net.ParseIP("192.168.1.1"),
		DstIP:          net.ParseIP("10.0.0.1"),
		SrcPort:        1234,
		DstPort:        2055,
		UDPChecksum:    0x1234,
		Payload:        []byte("duplicate-payload"),
		Direction:      capture.DirectionHost,
	}
	pkt2 := &capture.CapturedPacket{
		CaptureTimeUTC: time.Now().UTC().Add(time.Millisecond),
		SrcIP:          net.ParseIP("192.168.1.1"),
		DstIP:          net.ParseIP("10.0.0.1"),
		SrcPort:        1234,
		DstPort:        2055,
		UDPChecksum:    0x1234,
		Payload:        []byte("duplicate-payload"),
		Direction:      capture.DirectionHost,
	}

	ring.Push(pkt1)
	ring.Push(pkt2)

	// Wait for window to expire
	time.Sleep(50 * time.Millisecond)

	// Both identical packets should be delivered without timer leak or dropping
	if count := atomic.LoadInt32(&deliveredCount); count != 2 {
		t.Fatalf("expected 2 delivered for duplicate host packets, got %d", count)
	}
}

