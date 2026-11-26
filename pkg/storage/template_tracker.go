package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
)

// TemplateRecord preserves a cached template packet along with its original network context.
type TemplateRecord struct {
	ExporterIP net.IP
	DstPort    uint16
	DomainID   uint32
	Packet     []byte
}

// TemplateTracker identifies and caches NetFlow v9 and IPFIX template flowset packets.
type TemplateTracker struct {
	mu        sync.RWMutex
	templates map[string]*TemplateRecord // key: "exporterIP:dstPort:domainID" -> TemplateRecord
}

// NewTemplateTracker constructs an empty TemplateTracker.
func NewTemplateTracker() *TemplateTracker {
	return &TemplateTracker{
		templates: make(map[string]*TemplateRecord),
	}
}

// ProcessPacket checks if a packet contains NetFlow v9 (FlowSet 0/1) or IPFIX (Set 2/3) templates.
// If it does, the packet is cached under "exporterIP:dstPort:domainID" and returns true.
func (t *TemplateTracker) ProcessPacket(exporterIP net.IP, dstPort uint16, packet []byte) bool {
	if len(packet) < 4 {
		return false
	}

	version := binary.BigEndian.Uint16(packet[0:2])
	hasTemplate := false
	var domainID uint32

	switch version {
	case 9:
		// NetFlow v9 header is 20 bytes:
		// bytes 0-1: version (9)
		// bytes 2-3: count
		// bytes 4-7: sysUptime
		// bytes 8-11: unixSecs
		// bytes 12-15: flowSequence
		// bytes 16-19: sourceID
		if len(packet) < 20 {
			return false
		}
		domainID = binary.BigEndian.Uint32(packet[16:20])

		offset := 20
		for offset+4 <= len(packet) {
			flowSetID := binary.BigEndian.Uint16(packet[offset : offset+2])
			flowSetLen := int(binary.BigEndian.Uint16(packet[offset+2 : offset+4]))
			if flowSetLen < 4 || offset+flowSetLen > len(packet) {
				break
			}
			if flowSetID == 0 || flowSetID == 1 { // 0 = Template, 1 = Options Template
				hasTemplate = true
				break
			}
			offset += flowSetLen
		}

	case 10:
		// IPFIX header is 16 bytes:
		// bytes 0-1: version (10)
		// bytes 2-3: length
		// bytes 4-7: exportTime
		// bytes 8-11: flowSequence
		// bytes 12-15: observationDomainID
		if len(packet) < 16 {
			return false
		}
		domainID = binary.BigEndian.Uint32(packet[12:16])

		totalLen := int(binary.BigEndian.Uint16(packet[2:4]))
		if totalLen > len(packet) {
			totalLen = len(packet)
		}

		offset := 16
		for offset+4 <= totalLen {
			setID := binary.BigEndian.Uint16(packet[offset : offset+2])
			setLen := int(binary.BigEndian.Uint16(packet[offset+2 : offset+4]))
			if setLen < 4 || offset+setLen > totalLen {
				break
			}
			if setID == 2 || setID == 3 { // 2 = Template, 3 = Options Template
				hasTemplate = true
				break
			}
			offset += setLen
		}
	}

	if !hasTemplate {
		return false
	}

	key := fmt.Sprintf("%s:%d:%d", exporterIP.String(), dstPort, domainID)
	pktClone := make([]byte, len(packet))
	copy(pktClone, packet)

	ipClone := append(net.IP(nil), exporterIP...)

	t.mu.Lock()
	t.templates[key] = &TemplateRecord{
		ExporterIP: ipClone,
		DstPort:    dstPort,
		DomainID:   domainID,
		Packet:     pktClone,
	}
	t.mu.Unlock()

	return true
}

// Snapshot returns a copy of all current template records.
func (t *TemplateTracker) Snapshot() []*TemplateRecord {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*TemplateRecord, 0, len(t.templates))
	for _, rec := range t.templates {
		cp := &TemplateRecord{
			ExporterIP: append(net.IP(nil), rec.ExporterIP...),
			DstPort:    rec.DstPort,
			DomainID:   rec.DomainID,
			Packet:     make([]byte, len(rec.Packet)),
		}
		copy(cp.Packet, rec.Packet)
		result = append(result, cp)
	}
	return result
}

// SaveSnapshot serializes all cached template records to a binary file.
// Format:
// 4 bytes: count of records
// For each record:
//   16 bytes: ExporterIP (To16)
//   2 bytes:  DstPort
//   4 bytes:  DomainID
//   4 bytes:  PacketLen
//   PacketLen bytes: Packet
func (t *TemplateTracker) SaveSnapshot(path string) error {
	snap := t.Snapshot()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(snap)))
	if _, err := f.Write(countBuf[:]); err != nil {
		return err
	}

	for _, rec := range snap {
		var hdr [26]byte
		ip16 := rec.ExporterIP.To16()
		if ip16 == nil {
			ip16 = net.IPv6zero
		}
		copy(hdr[0:16], ip16)
		binary.BigEndian.PutUint16(hdr[16:18], rec.DstPort)
		binary.BigEndian.PutUint32(hdr[18:22], rec.DomainID)
		binary.BigEndian.PutUint32(hdr[22:26], uint32(len(rec.Packet)))

		if _, err := f.Write(hdr[:]); err != nil {
			return err
		}
		if _, err := f.Write(rec.Packet); err != nil {
			return err
		}
	}

	return nil
}

// LoadSnapshot reads template records from a binary snapshot file.
func (t *TemplateTracker) LoadSnapshot(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var countBuf [4]byte
	if _, err := io.ReadFull(f, countBuf[:]); err != nil {
		return err
	}
	count := int(binary.BigEndian.Uint32(countBuf[:]))

	for i := 0; i < count; i++ {
		var hdr [26]byte
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			return err
		}

		rawIP := make([]byte, 16)
		copy(rawIP, hdr[0:16])
		ip := net.IP(rawIP)
		if v4 := ip.To4(); v4 != nil {
			ip = v4
		}

		dstPort := binary.BigEndian.Uint16(hdr[16:18])
		domainID := binary.BigEndian.Uint32(hdr[18:22])
		pktLen := int(binary.BigEndian.Uint32(hdr[22:26]))

		pkt := make([]byte, pktLen)
		if _, err := io.ReadFull(f, pkt); err != nil {
			return err
		}

		key := fmt.Sprintf("%s:%d:%d", ip.String(), dstPort, domainID)
		t.mu.Lock()
		t.templates[key] = &TemplateRecord{
			ExporterIP: ip,
			DstPort:    dstPort,
			DomainID:   domainID,
			Packet:     pkt,
		}
		t.mu.Unlock()
	}

	return nil
}
