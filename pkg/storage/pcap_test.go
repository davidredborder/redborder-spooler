package storage_test

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

func TestExportPCAP(t *testing.T) {
	srcDir := t.TempDir()
	cfg := &config.Config{SpoolDir: srcDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}

	t1 := time.Date(2026, 11, 24, 10, 15, 0, 123000, time.UTC)
	t2 := time.Date(2026, 11, 24, 10, 20, 0, 456000, time.UTC)

	// 1. IPv4 packet
	srcIPv4 := net.ParseIP("192.168.1.10").To4()
	dstIPv4 := net.ParseIP("10.0.0.1").To4()
	if err := store.WriteUDP(t1, srcIPv4, dstIPv4, 1234, 2055, []byte("netflow-v9-payload")); err != nil {
		t.Fatal(err)
	}

	// 2. IPv6 packet
	srcIPv6 := net.ParseIP("2001:db8::1")
	dstIPv6 := net.ParseIP("2001:db8::2")
	if err := store.WriteUDP(t2, srcIPv6, dstIPv6, 5678, 4739, []byte("ipfix-ipv6-payload")); err != nil {
		t.Fatal(err)
	}

	_ = store.CloseHour()
	_ = store.Close()

	// Export to PCAP buffer
	var pcapBuf bytes.Buffer
	fromTime := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	toTime := time.Date(2026, 11, 24, 11, 0, 0, 0, time.UTC)

	stats, err := storage.ExportPCAP(srcDir, fromTime, toTime, &pcapBuf)
	if err != nil {
		t.Fatalf("ExportPCAP failed: %v", err)
	}
	if stats.PacketsExported != 2 {
		t.Errorf("expected 2 packets exported, got %d", stats.PacketsExported)
	}

	data := pcapBuf.Bytes()
	if len(data) < 24 {
		t.Fatalf("pcap data too short: %d bytes", len(data))
	}

	// Verify Global Header (24 bytes)
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != 0xa1b2c3d4 {
		t.Errorf("expected pcap magic 0xa1b2c3d4, got 0x%x", magic)
	}
	verMajor := binary.LittleEndian.Uint16(data[4:6])
	verMinor := binary.LittleEndian.Uint16(data[6:8])
	if verMajor != 2 || verMinor != 4 {
		t.Errorf("expected version 2.4, got %d.%d", verMajor, verMinor)
	}
	linkType := binary.LittleEndian.Uint32(data[20:24])
	if linkType != 1 { // LINKTYPE_ETHERNET
		t.Errorf("expected linktype 1 (Ethernet), got %d", linkType)
	}

	// Verify Packet 1 (IPv4)
	offset := 24
	if len(data) < offset+16 {
		t.Fatal("truncated packet 1 header")
	}
	pkt1Sec := binary.LittleEndian.Uint32(data[offset : offset+4])
	pkt1Len := binary.LittleEndian.Uint32(data[offset+8 : offset+12])
	offset += 16

	if pkt1Sec != uint32(t1.Unix()) {
		t.Errorf("pkt1 timestamp mismatch: expected %d, got %d", t1.Unix(), pkt1Sec)
	}
	frame1 := data[offset : offset+int(pkt1Len)]
	offset += int(pkt1Len)

	// Ethernet (14 bytes)
	ethType1 := binary.BigEndian.Uint16(frame1[12:14])
	if ethType1 != 0x0800 { // IPv4
		t.Errorf("expected ethertype 0x0800, got 0x%04x", ethType1)
	}
	// IPv4 Header (20 bytes)
	ip1 := frame1[14 : 14+20]
	if ip1[9] != 17 { // UDP
		t.Errorf("expected IP protocol 17 (UDP), got %d", ip1[9])
	}
	if !bytes.Equal(ip1[12:16], srcIPv4) || !bytes.Equal(ip1[16:20], dstIPv4) {
		t.Errorf("IP endpoints mismatch: %v -> %v", ip1[12:16], ip1[16:20])
	}
	// UDP Header (8 bytes)
	udp1 := frame1[34 : 34+8]
	srcPort1 := binary.BigEndian.Uint16(udp1[0:2])
	dstPort1 := binary.BigEndian.Uint16(udp1[2:4])
	if srcPort1 != 1234 || dstPort1 != 2055 {
		t.Errorf("UDP ports mismatch: %d -> %d", srcPort1, dstPort1)
	}
	payload1 := frame1[42:]
	if string(payload1) != "netflow-v9-payload" {
		t.Errorf("payload mismatch: %s", string(payload1))
	}

	// Verify Packet 2 (IPv6)
	if len(data) < offset+16 {
		t.Fatal("truncated packet 2 header")
	}
	pkt2Len := binary.LittleEndian.Uint32(data[offset+8 : offset+12])
	offset += 16
	frame2 := data[offset : offset+int(pkt2Len)]

	ethType2 := binary.BigEndian.Uint16(frame2[12:14])
	if ethType2 != 0x86dd { // IPv6
		t.Errorf("expected ethertype 0x86dd, got 0x%04x", ethType2)
	}
	ip2 := frame2[14 : 14+40]
	if ip2[6] != 17 { // Next header UDP
		t.Errorf("expected IPv6 next header 17, got %d", ip2[6])
	}
	udp2 := frame2[54 : 54+8]
	srcPort2 := binary.BigEndian.Uint16(udp2[0:2])
	dstPort2 := binary.BigEndian.Uint16(udp2[2:4])
	if srcPort2 != 5678 || dstPort2 != 4739 {
		t.Errorf("UDP ports mismatch: %d -> %d", srcPort2, dstPort2)
	}
	payload2 := frame2[62:]
	if string(payload2) != "ipfix-ipv6-payload" {
		t.Errorf("payload mismatch: %s", string(payload2))
	}

	// 3. Test compatibility with actual tcpdump if present
	if tcpdumpPath, err := exec.LookPath("tcpdump"); err == nil {
		pcapFilePath := filepath.Join(t.TempDir(), "test.pcap")
		if err := os.WriteFile(pcapFilePath, data, 0644); err != nil {
			t.Fatal(err)
		}

		cmd := exec.Command(tcpdumpPath, "-nn", "-r", pcapFilePath)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("tcpdump failed to parse generated pcap: %v\nOutput: %s", err, string(out))
		}
		outStr := string(out)
		if !strings.Contains(outStr, "192.168.1.10.1234 > 10.0.0.1.2055") {
			t.Errorf("tcpdump output missing IPv4 flow: %s", outStr)
		}
		if !strings.Contains(outStr, "2001:db8::1.5678 > 2001:db8::2.4739") {
			t.Errorf("tcpdump output missing IPv6 flow: %s", outStr)
		}
	}
}
