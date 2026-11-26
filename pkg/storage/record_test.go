package storage_test

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/storage"
)

func TestUDPRecordMarshalUnmarshal(t *testing.T) {
	captureTime := time.Date(2026, 11, 24, 10, 30, 45, 123456789, time.UTC)
	srcIP := net.ParseIP("192.168.1.100")
	dstIP := net.ParseIP("10.0.0.1")
	srcPort := uint16(54321)
	dstPort := uint16(2055)
	payload := []byte("netflow-v9-payload-bytes-example")

	rec := storage.UDPRecord{
		CaptureTimeUTC: captureTime,
		SrcIP:          srcIP,
		DstIP:          dstIP,
		SrcPort:        srcPort,
		DstPort:        dstPort,
		Payload:        payload,
	}

	data, err := rec.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var decoded storage.UDPRecord
	r := bytes.NewReader(data)
	if err := decoded.UnmarshalFrom(r); err != nil {
		t.Fatalf("UnmarshalFrom failed: %v", err)
	}

	if decoded.CaptureTimeUTC.UnixNano() != captureTime.UnixNano() {
		t.Errorf("CaptureTimeUTC mismatch: got %v, want %v", decoded.CaptureTimeUTC, captureTime)
	}
	if !decoded.SrcIP.Equal(srcIP) {
		t.Errorf("SrcIP mismatch: got %v, want %v", decoded.SrcIP, srcIP)
	}
	if !decoded.DstIP.Equal(dstIP) {
		t.Errorf("DstIP mismatch: got %v, want %v", decoded.DstIP, dstIP)
	}
	if decoded.SrcPort != srcPort || decoded.DstPort != dstPort {
		t.Errorf("Port mismatch: got %d->%d, want %d->%d", decoded.SrcPort, decoded.DstPort, srcPort, dstPort)
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Errorf("Payload mismatch: got %s, want %s", string(decoded.Payload), string(payload))
	}
}

func TestUDPRecordCorruptedPayload(t *testing.T) {
	rec := storage.UDPRecord{
		CaptureTimeUTC: time.Now().UTC(),
		SrcIP:          net.ParseIP("1.1.1.1"),
		DstIP:          net.ParseIP("2.2.2.2"),
		SrcPort:        1234,
		DstPort:        2055,
		Payload:        []byte("valid payload"),
	}

	data, err := rec.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Corrupt last byte
	data[len(data)-1] ^= 0xFF

	var decoded storage.UDPRecord
	if err := decoded.UnmarshalFrom(bytes.NewReader(data)); err == nil {
		t.Fatal("expected CRC error on corrupted payload, got nil")
	}
}

func TestHTTPRecordMarshalUnmarshal(t *testing.T) {
	captureTime := time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC)
	clientIP := net.ParseIP("172.16.0.5")
	headers := map[string][]string{
		"Content-Type": {"application/json"},
		"X-Custom-Hdr": {"foo", "bar"},
	}
	body := []byte(`{"event":"auth_login","status":"ok"}`)

	rec := storage.HTTPRecord{
		CaptureTimeUTC: captureTime,
		ClientIP:       clientIP,
		Method:         "POST",
		URI:            "/rbdata/events",
		Headers:        headers,
		Body:           body,
	}

	data, err := rec.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var decoded storage.HTTPRecord
	r := bytes.NewReader(data)
	if err := decoded.UnmarshalFrom(r); err != nil {
		t.Fatalf("UnmarshalFrom failed: %v", err)
	}

	if decoded.CaptureTimeUTC.UnixNano() != captureTime.UnixNano() {
		t.Errorf("CaptureTimeUTC mismatch: got %v, want %v", decoded.CaptureTimeUTC, captureTime)
	}
	if !decoded.ClientIP.Equal(clientIP) {
		t.Errorf("ClientIP mismatch: got %v, want %v", decoded.ClientIP, clientIP)
	}
	if decoded.Method != "POST" {
		t.Errorf("Method mismatch: got %s", decoded.Method)
	}
	if decoded.URI != "/rbdata/events" {
		t.Errorf("URI mismatch: got %s", decoded.URI)
	}
	if decoded.Headers["Content-Type"][0] != "application/json" {
		t.Errorf("Header Content-Type mismatch: %v", decoded.Headers["Content-Type"])
	}
	if !bytes.Equal(decoded.Body, body) {
		t.Errorf("Body mismatch: got %s, want %s", string(decoded.Body), string(body))
	}
}

func TestRBRPEnvelopeWrapUnwrap(t *testing.T) {
	captureTime := time.Date(2026, 11, 24, 12, 34, 56, 0, time.UTC)
	exporterIP := net.ParseIP("192.168.10.25")
	payload := []byte("original-packet-content")

	wrapped := storage.WrapRBRP(payload, captureTime, exporterIP)
	if len(wrapped) != 28+len(payload) {
		t.Fatalf("expected wrapped len %d, got %d", 28+len(payload), len(wrapped))
	}

	unwrappedPayload, unwrapTime, unwrapIP, ok := storage.UnwrapRBRP(wrapped)
	if !ok {
		t.Fatal("UnwrapRBRP failed")
	}

	if !bytes.Equal(unwrappedPayload, payload) {
		t.Errorf("Payload mismatch: got %s, want %s", string(unwrappedPayload), string(payload))
	}
	if unwrapTime.Unix() != captureTime.Unix() {
		t.Errorf("Timestamp mismatch: got %v, want %v", unwrapTime.Unix(), captureTime.Unix())
	}
	if !unwrapIP.Equal(exporterIP) {
		t.Errorf("ExporterIP mismatch: got %v, want %v", unwrapIP, exporterIP)
	}

	// Test short buffer
	_, _, _, ok = storage.UnwrapRBRP([]byte("too-short"))
	if ok {
		t.Error("expected ok=false for short data")
	}

	// Test wrong magic
	badMagic := make([]byte, len(wrapped))
	copy(badMagic, wrapped)
	badMagic[0] = 'X'
	_, _, _, ok = storage.UnwrapRBRP(badMagic)
	if ok {
		t.Error("expected ok=false for wrong magic")
	}
}

func TestSyslogRecordMarshalUnmarshal(t *testing.T) {
	ts := time.Date(2026, 11, 25, 12, 0, 0, 123456789, time.UTC)
	clientIP := net.ParseIP("192.168.1.100")
	msg := []byte("<34>1 2026-11-25T12:00:00Z myhost testapp 123 - - test syslog message")

	rec := storage.SyslogRecord{
		TimestampNano: ts.UnixNano(),
		ClientIP:      clientIP.To4(),
		Transport:     "udp",
		Message:       msg,
	}

	data, err := rec.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var decoded storage.SyslogRecord
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if decoded.TimestampNano != rec.TimestampNano {
		t.Errorf("TimestampNano mismatch: got %d, want %d", decoded.TimestampNano, rec.TimestampNano)
	}
	if !net.IP(decoded.ClientIP).Equal(clientIP) {
		t.Errorf("ClientIP mismatch: got %v, want %v", decoded.ClientIP, clientIP)
	}
	if decoded.Transport != "udp" {
		t.Errorf("Transport mismatch: got %q, want 'udp'", decoded.Transport)
	}
	if !bytes.Equal(decoded.Message, msg) {
		t.Errorf("Message mismatch: got %s, want %s", string(decoded.Message), string(msg))
	}

	// Test UnmarshalFrom
	var decodedFrom storage.SyslogRecord
	if err := decodedFrom.UnmarshalFrom(bytes.NewReader(data)); err != nil {
		t.Fatalf("UnmarshalFrom failed: %v", err)
	}
	if !bytes.Equal(decodedFrom.Message, msg) {
		t.Errorf("UnmarshalFrom message mismatch: got %s, want %s", string(decodedFrom.Message), string(msg))
	}
}

func TestSyslogRecordCorrupted(t *testing.T) {
	rec := storage.SyslogRecord{
		TimestampNano: time.Now().UnixNano(),
		ClientIP:      net.ParseIP("10.0.0.1").To4(),
		Message:       []byte("<13>corruptible message"),
	}

	data, err := rec.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Corrupt payload
	data[len(data)-1] ^= 0xFF
	var decoded storage.SyslogRecord
	if err := decoded.UnmarshalBinary(data); err == nil {
		t.Fatal("expected CRC error on corrupted syslog record, got nil")
	}

	// Short buffer
	var shortDecoded storage.SyslogRecord
	if err := shortDecoded.UnmarshalBinary([]byte("short")); err == nil {
		t.Fatal("expected error on short buffer, got nil")
	}
}
