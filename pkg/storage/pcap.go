package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
)

const (
	PCAPMagicMicroseconds uint32 = 0xa1b2c3d4
	PCAPVersionMajor      uint16 = 2
	PCAPVersionMinor      uint16 = 4
	PCAPSnapLenDefault    uint32 = 65535
	PCAPLinkTypeEthernet  uint32 = 1 // LINKTYPE_ETHERNET

	EtherTypeIPv4 uint16 = 0x0800
	EtherTypeIPv6 uint16 = 0x86dd
	IPProtoUDP    uint8  = 17
)

// PCAPStats summarizes a PCAP conversion operation.
type PCAPStats struct {
	PacketsExported uint64
	BytesExported   uint64
}

// ExportPCAP converts recorded UDP datagrams within [fromUTC, toUTC] into standard PCAP format.
func ExportPCAP(spoolDir string, fromUTC, toUTC time.Time, out io.Writer) (*PCAPStats, error) {
	if fromUTC.After(toUTC) {
		return nil, fmt.Errorf("from time %v cannot be after to time %v", fromUTC, toUTC)
	}

	store, err := NewHourlyStore(&config.Config{SpoolDir: spoolDir})
	if err != nil {
		return nil, fmt.Errorf("failed to open spool store: %w", err)
	}
	defer store.Close()

	reader, err := store.OpenRange(fromUTC, toUTC)
	if err != nil {
		return nil, fmt.Errorf("failed to open range reader: %w", err)
	}
	defer reader.Close()

	if err := writePCAPHeader(out); err != nil {
		return nil, fmt.Errorf("failed to write PCAP header: %w", err)
	}

	stats := &PCAPStats{}

	for {
		rec, err := reader.NextUDP()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return stats, fmt.Errorf("failed reading record: %w", err)
		}

		frame, err := buildEthernetFrame(rec)
		if err != nil {
			continue // Skip unencapsulatable frames
		}

		if err := writePCAPPacket(out, rec.CaptureTimeUTC, frame); err != nil {
			return stats, fmt.Errorf("failed writing PCAP packet: %w", err)
		}

		stats.PacketsExported++
		stats.BytesExported += uint64(len(frame))
	}

	return stats, nil
}

func writePCAPHeader(w io.Writer) error {
	var hdr [24]byte
	binary.LittleEndian.PutUint32(hdr[0:4], PCAPMagicMicroseconds)
	binary.LittleEndian.PutUint16(hdr[4:6], PCAPVersionMajor)
	binary.LittleEndian.PutUint16(hdr[6:8], PCAPVersionMinor)
	binary.LittleEndian.PutUint32(hdr[8:12], 0) // thiszone (GMT)
	binary.LittleEndian.PutUint32(hdr[12:16], 0) // sigfigs
	binary.LittleEndian.PutUint32(hdr[16:20], PCAPSnapLenDefault)
	binary.LittleEndian.PutUint32(hdr[20:24], PCAPLinkTypeEthernet)
	_, err := w.Write(hdr[:])
	return err
}

func writePCAPPacket(w io.Writer, t time.Time, frame []byte) error {
	var hdr [16]byte
	sec := uint32(t.Unix())
	usec := uint32(t.Nanosecond() / 1000)
	frameLen := uint32(len(frame))

	binary.LittleEndian.PutUint32(hdr[0:4], sec)
	binary.LittleEndian.PutUint32(hdr[4:8], usec)
	binary.LittleEndian.PutUint32(hdr[8:12], frameLen) // incl_len
	binary.LittleEndian.PutUint32(hdr[12:16], frameLen) // orig_len

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(frame)
	return err
}

func buildEthernetFrame(rec *UDPRecord) ([]byte, error) {
	dstMAC := [6]byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x02}
	srcMAC := [6]byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x01}

	src4 := rec.SrcIP.To4()
	dst4 := rec.DstIP.To4()

	if src4 != nil && dst4 != nil {
		// IPv4 UDP frame
		payload := rec.Payload
		if len(payload) > 65507 { // Max IPv4 UDP payload
			payload = payload[:65507]
		}

		ipTotalLen := 20 + 8 + len(payload)
		frameLen := 14 + ipTotalLen
		frame := make([]byte, frameLen)

		// Ethernet
		copy(frame[0:6], dstMAC[:])
		copy(frame[6:12], srcMAC[:])
		binary.BigEndian.PutUint16(frame[12:14], EtherTypeIPv4)

		// IPv4
		frame[14] = 0x45 // Version 4, IHL 5
		frame[15] = 0x00 // DSCP / ECN
		binary.BigEndian.PutUint16(frame[16:18], uint16(ipTotalLen))
		binary.BigEndian.PutUint16(frame[18:20], 0) // ID
		binary.BigEndian.PutUint16(frame[20:22], 0x4000) // DF flag
		frame[22] = 64 // TTL
		frame[23] = IPProtoUDP
		binary.BigEndian.PutUint16(frame[24:26], 0) // Checksum initial
		copy(frame[26:30], src4)
		copy(frame[30:34], dst4)
		csum := ipv4Checksum(frame[14:34])
		binary.BigEndian.PutUint16(frame[24:26], csum)

		// UDP
		binary.BigEndian.PutUint16(frame[34:36], rec.SrcPort)
		binary.BigEndian.PutUint16(frame[36:38], rec.DstPort)
		binary.BigEndian.PutUint16(frame[38:40], uint16(8+len(payload)))
		binary.BigEndian.PutUint16(frame[40:42], 0) // Checksum optional

		copy(frame[42:], payload)
		return frame, nil
	}

	src16 := rec.SrcIP.To16()
	dst16 := rec.DstIP.To16()
	if src16 != nil && dst16 != nil {
		// IPv6 UDP frame
		payload := rec.Payload
		if len(payload) > 65527 { // Max IPv6 UDP payload
			payload = payload[:65527]
		}

		udpLen := 8 + len(payload)
		frameLen := 14 + 40 + udpLen
		frame := make([]byte, frameLen)

		// Ethernet
		copy(frame[0:6], dstMAC[:])
		copy(frame[6:12], srcMAC[:])
		binary.BigEndian.PutUint16(frame[12:14], EtherTypeIPv6)

		// IPv6
		binary.BigEndian.PutUint32(frame[14:18], 0x60000000) // Version 6
		binary.BigEndian.PutUint16(frame[18:20], uint16(udpLen))
		frame[20] = IPProtoUDP
		frame[21] = 64 // Hop Limit
		copy(frame[22:38], src16)
		copy(frame[38:54], dst16)

		// UDP
		binary.BigEndian.PutUint16(frame[54:56], rec.SrcPort)
		binary.BigEndian.PutUint16(frame[56:58], rec.DstPort)
		binary.BigEndian.PutUint16(frame[58:60], uint16(udpLen))
		binary.BigEndian.PutUint16(frame[60:62], 0)

		copy(frame[62:], payload)
		return frame, nil
	}

	return nil, fmt.Errorf("invalid IP endpoints: src=%v, dst=%v", rec.SrcIP, rec.DstIP)
}

func ipv4Checksum(b []byte) uint16 {
	var sum uint32
	for i := 0; i < len(b)-1; i += 2 {
		sum += uint32(binary.BigEndian.Uint16(b[i : i+2]))
	}
	if len(b)%2 == 1 {
		sum += uint32(b[len(b)-1]) << 8
	}
	for sum > 0xffff {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	return ^uint16(sum)
}
