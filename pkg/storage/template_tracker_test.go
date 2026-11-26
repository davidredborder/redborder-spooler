package storage_test

import (
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/redborder/redborder-spooler/pkg/storage"
)

func makeNetflowV9TemplatePacket(sourceID uint32, flowSetID uint16) []byte {
	buf := make([]byte, 20+8) // 20 header + 8 flowset
	binary.BigEndian.PutUint16(buf[0:2], 9)  // version 9
	binary.BigEndian.PutUint16(buf[2:4], 1)  // count
	binary.BigEndian.PutUint32(buf[4:8], 1000) // sysUptime
	binary.BigEndian.PutUint32(buf[8:12], 1700000000) // unixSecs
	binary.BigEndian.PutUint32(buf[12:16], 1) // seq
	binary.BigEndian.PutUint32(buf[16:20], sourceID) // sourceID

	binary.BigEndian.PutUint16(buf[20:22], flowSetID) // 0 for template
	binary.BigEndian.PutUint16(buf[22:24], 8)         // len
	// 4 bytes dummy template data
	return buf
}

func makeIPFIXTemplatePacket(domainID uint32, setID uint16) []byte {
	buf := make([]byte, 16+8) // 16 header + 8 set
	binary.BigEndian.PutUint16(buf[0:2], 10) // version 10 (IPFIX)
	binary.BigEndian.PutUint16(buf[2:4], 24) // total length
	binary.BigEndian.PutUint32(buf[4:8], 1700000000) // exportTime
	binary.BigEndian.PutUint32(buf[8:12], 1) // seq
	binary.BigEndian.PutUint32(buf[12:16], domainID)

	binary.BigEndian.PutUint16(buf[16:18], setID) // 2 for template
	binary.BigEndian.PutUint16(buf[18:20], 8)     // set length
	return buf
}

func TestTemplateTracker(t *testing.T) {
	tracker := storage.NewTemplateTracker()

	ip := net.ParseIP("192.168.1.50")
	port := uint16(2055)

	// Non-template packet (FlowSet ID 256)
	dataPkt := makeNetflowV9TemplatePacket(1, 256)
	if tracker.ProcessPacket(ip, port, dataPkt) {
		t.Errorf("expected false for data-only flowset")
	}
	if len(tracker.Snapshot()) != 0 {
		t.Errorf("expected 0 templates in snapshot")
	}

	// NetFlow v9 template packet
	v9TmplPkt := makeNetflowV9TemplatePacket(1, 0)
	if !tracker.ProcessPacket(ip, port, v9TmplPkt) {
		t.Errorf("expected true for NetFlow v9 template flowset")
	}
	if len(tracker.Snapshot()) != 1 {
		t.Fatalf("expected 1 template in snapshot, got %d", len(tracker.Snapshot()))
	}

	// IPFIX template packet
	ipfixTmplPkt := makeIPFIXTemplatePacket(2, 2)
	if !tracker.ProcessPacket(ip, port, ipfixTmplPkt) {
		t.Errorf("expected true for IPFIX template flowset")
	}
	if len(tracker.Snapshot()) != 2 {
		t.Fatalf("expected 2 templates in snapshot, got %d", len(tracker.Snapshot()))
	}

	// Save snapshot and reload in new tracker
	tmpDir := t.TempDir()
	snapFile := filepath.Join(tmpDir, "templates.snap")

	if err := tracker.SaveSnapshot(snapFile); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	tracker2 := storage.NewTemplateTracker()
	if err := tracker2.LoadSnapshot(snapFile); err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	snap2 := tracker2.Snapshot()
	if len(snap2) != 2 {
		t.Fatalf("expected 2 reloaded templates, got %d", len(snap2))
	}
	foundOrigIP := false
	for _, rec := range snap2 {
		if rec.ExporterIP.Equal(ip) && rec.DstPort == port {
			foundOrigIP = true
		}
	}
	if !foundOrigIP {
		t.Errorf("expected to find template record with original ExporterIP %s and DstPort %d", ip, port)
	}

	// Overwrite existing key
	v9TmplPktUpdated := makeNetflowV9TemplatePacket(1, 1) // Options template
	if !tracker.ProcessPacket(ip, port, v9TmplPktUpdated) {
		t.Errorf("expected true for NetFlow v9 options template")
	}
	if len(tracker.Snapshot()) != 2 {
		t.Errorf("expected still 2 templates due to key overwrite, got %d", len(tracker.Snapshot()))
	}
}

func TestTemplateTrackerLoadMissingFile(t *testing.T) {
	tracker := storage.NewTemplateTracker()
	err := tracker.LoadSnapshot("/non/existent/path/templates.snap")
	if !os.IsNotExist(err) && err != nil {
		t.Logf("load missing file returned non-fatal error: %v", err)
	}
}
