package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"strings"
	"time"
)

const (
	MagicByte0 byte = 0x52 // 'R'
	MagicByte1 byte = 0x42 // 'B'

	RecordTypeUDP    byte = 0x01
	RecordTypeHTTP   byte = 0x02
	RecordTypeSyslog byte = 0x03

	RBRPHeaderLen = 28
)

var (
	RBRPMagic = [4]byte{'R', 'B', 'R', 'P'}

	ErrCorruptRecord = errors.New("corrupt record: magic or crc32 mismatch")
	ErrShortBuffer   = errors.New("buffer too short")
)

// UDPRecord represents a captured UDP datagram stored in a WAL file.
type UDPRecord struct {
	CaptureTimeUTC time.Time
	Flags          byte
	SrcIP          net.IP
	DstIP          net.IP
	SrcPort        uint16
	DstPort        uint16
	Payload        []byte
}

// HTTPRecord represents an intercepted HTTP request stored in a WAL file.
type HTTPRecord struct {
	CaptureTimeUTC time.Time
	ClientIP       net.IP
	Method         string
	URI            string
	Headers        map[string][]string
	Body           []byte
}

// SyslogRecord represents an intercepted or captured Syslog message stored in a WAL file.
type SyslogRecord struct {
	TimestampNano int64  `json:"timestamp_nano"`
	ClientIP      []byte `json:"client_ip"`
	Transport     string `json:"transport"` // "udp" or "tcp"
	Message       []byte `json:"message"`
}

// MarshalBinary serializes a UDPRecord into its framed binary representation.
func (r *UDPRecord) MarshalBinary() ([]byte, error) {
	payloadLen := len(r.Payload)
	crc := crc32.ChecksumIEEE(r.Payload)

	buf := make([]byte, 56+payloadLen)
	buf[0] = MagicByte0
	buf[1] = MagicByte1
	buf[2] = RecordTypeUDP
	buf[3] = r.Flags

	binary.BigEndian.PutUint64(buf[4:12], uint64(r.CaptureTimeUTC.UnixNano()))

	src16 := r.SrcIP.To16()
	if src16 == nil {
		src16 = net.IPv6zero
	}
	copy(buf[12:28], src16)

	dst16 := r.DstIP.To16()
	if dst16 == nil {
		dst16 = net.IPv6zero
	}
	copy(buf[28:44], dst16)

	binary.BigEndian.PutUint16(buf[44:46], r.SrcPort)
	binary.BigEndian.PutUint16(buf[46:48], r.DstPort)
	binary.BigEndian.PutUint32(buf[48:52], uint32(payloadLen))
	binary.BigEndian.PutUint32(buf[52:56], crc)

	copy(buf[56:], r.Payload)
	return buf, nil
}

// UnmarshalFrom reads and parses a UDPRecord from an io.Reader.
func (r *UDPRecord) UnmarshalFrom(rd io.Reader) error {
	var hdr [56]byte
	if _, err := io.ReadFull(rd, hdr[:]); err != nil {
		return err
	}

	if hdr[0] != MagicByte0 || hdr[1] != MagicByte1 || hdr[2] != RecordTypeUDP {
		return fmt.Errorf("%w: invalid header magic or type", ErrCorruptRecord)
	}

	r.Flags = hdr[3]
	nano := int64(binary.BigEndian.Uint64(hdr[4:12]))
	r.CaptureTimeUTC = time.Unix(0, nano).UTC()

	srcIP := make([]byte, 16)
	copy(srcIP, hdr[12:28])
	r.SrcIP = net.IP(srcIP)

	dstIP := make([]byte, 16)
	copy(dstIP, hdr[28:44])
	r.DstIP = net.IP(dstIP)

	r.SrcPort = binary.BigEndian.Uint16(hdr[44:46])
	r.DstPort = binary.BigEndian.Uint16(hdr[46:48])
	payloadLen := binary.BigEndian.Uint32(hdr[48:52])
	expectedCRC := binary.BigEndian.Uint32(hdr[52:56])

	r.Payload = make([]byte, payloadLen)
	if _, err := io.ReadFull(rd, r.Payload); err != nil {
		return err
	}

	if actualCRC := crc32.ChecksumIEEE(r.Payload); actualCRC != expectedCRC {
		return fmt.Errorf("%w: crc32 mismatch (expected %08x, got %08x)", ErrCorruptRecord, expectedCRC, actualCRC)
	}

	return nil
}

// MarshalBinary serializes an HTTPRecord into its framed binary representation.
func (r *HTTPRecord) MarshalBinary() ([]byte, error) {
	methodBytes := []byte(r.Method)
	uriBytes := []byte(r.URI)

	headersJSON, err := json.Marshal(r.Headers)
	if err != nil {
		headersJSON = []byte("{}")
	}

	bodyCRC := crc32.ChecksumIEEE(r.Body)

	// Fixed header:
	// Magic (2) + Type (1) + TimeNano (8) + ClientIP (16) + MethodLen (2) + URILen (2) + HeadersLen (4) + BodyLen (4) + BodyCRC (4) = 43 bytes
	fixedHeaderLen := 43
	totalLen := fixedHeaderLen + len(methodBytes) + len(uriBytes) + len(headersJSON) + len(r.Body)

	buf := make([]byte, totalLen)
	buf[0] = MagicByte0
	buf[1] = MagicByte1
	buf[2] = RecordTypeHTTP

	binary.BigEndian.PutUint64(buf[3:11], uint64(r.CaptureTimeUTC.UnixNano()))

	c16 := r.ClientIP.To16()
	if c16 == nil {
		c16 = net.IPv6zero
	}
	copy(buf[11:27], c16)

	binary.BigEndian.PutUint16(buf[27:29], uint16(len(methodBytes)))
	binary.BigEndian.PutUint16(buf[29:31], uint16(len(uriBytes)))
	binary.BigEndian.PutUint32(buf[31:35], uint32(len(headersJSON)))
	binary.BigEndian.PutUint32(buf[35:39], uint32(len(r.Body)))
	binary.BigEndian.PutUint32(buf[39:43], bodyCRC)

	offset := 43
	copy(buf[offset:], methodBytes)
	offset += len(methodBytes)

	copy(buf[offset:], uriBytes)
	offset += len(uriBytes)

	copy(buf[offset:], headersJSON)
	offset += len(headersJSON)

	copy(buf[offset:], r.Body)

	return buf, nil
}

// UnmarshalFrom reads and parses an HTTPRecord from an io.Reader.
func (r *HTTPRecord) UnmarshalFrom(rd io.Reader) error {
	var hdr [43]byte
	if _, err := io.ReadFull(rd, hdr[:]); err != nil {
		return err
	}

	if hdr[0] != MagicByte0 || hdr[1] != MagicByte1 || hdr[2] != RecordTypeHTTP {
		return fmt.Errorf("%w: invalid http header magic or type", ErrCorruptRecord)
	}

	nano := int64(binary.BigEndian.Uint64(hdr[3:11]))
	r.CaptureTimeUTC = time.Unix(0, nano).UTC()

	clientIP := make([]byte, 16)
	copy(clientIP, hdr[11:27])
	r.ClientIP = net.IP(clientIP)

	methodLen := int(binary.BigEndian.Uint16(hdr[27:29]))
	uriLen := int(binary.BigEndian.Uint16(hdr[29:31]))
	headersLen := int(binary.BigEndian.Uint32(hdr[31:35]))
	bodyLen := int(binary.BigEndian.Uint32(hdr[35:39]))
	expectedCRC := binary.BigEndian.Uint32(hdr[39:43])

	methodBuf := make([]byte, methodLen)
	if _, err := io.ReadFull(rd, methodBuf); err != nil {
		return err
	}
	r.Method = string(methodBuf)

	uriBuf := make([]byte, uriLen)
	if _, err := io.ReadFull(rd, uriBuf); err != nil {
		return err
	}
	r.URI = string(uriBuf)

	headersBuf := make([]byte, headersLen)
	if _, err := io.ReadFull(rd, headersBuf); err != nil {
		return err
	}
	if err := json.Unmarshal(headersBuf, &r.Headers); err != nil {
		return fmt.Errorf("failed to decode http headers json: %w", err)
	}

	r.Body = make([]byte, bodyLen)
	if _, err := io.ReadFull(rd, r.Body); err != nil {
		return err
	}

	if actualCRC := crc32.ChecksumIEEE(r.Body); actualCRC != expectedCRC {
		return fmt.Errorf("%w: http body crc32 mismatch", ErrCorruptRecord)
	}

	return nil
}

// MarshalBinary serializes a SyslogRecord into its framed binary representation.
func (r *SyslogRecord) MarshalBinary() ([]byte, error) {
	msgLen := len(r.Message)
	crc := crc32.ChecksumIEEE(r.Message)

	var transByte byte = 't'
	if strings.ToLower(r.Transport) == "udp" {
		transByte = 'u'
	}

	buf := make([]byte, 36+msgLen)
	buf[0] = MagicByte0
	buf[1] = MagicByte1
	buf[2] = RecordTypeSyslog
	buf[3] = transByte

	binary.BigEndian.PutUint64(buf[4:12], uint64(r.TimestampNano))

	ip16 := net.IP(r.ClientIP).To16()
	if ip16 == nil {
		ip16 = net.IPv6zero
	}
	copy(buf[12:28], ip16)

	binary.BigEndian.PutUint32(buf[28:32], uint32(msgLen))
	binary.BigEndian.PutUint32(buf[32:36], crc)

	copy(buf[36:], r.Message)
	return buf, nil
}

// UnmarshalFrom reads and parses a SyslogRecord from an io.Reader.
func (r *SyslogRecord) UnmarshalFrom(rd io.Reader) error {
	var hdr [36]byte
	if _, err := io.ReadFull(rd, hdr[:]); err != nil {
		return err
	}

	if hdr[0] != MagicByte0 || hdr[1] != MagicByte1 || hdr[2] != RecordTypeSyslog {
		return fmt.Errorf("%w: invalid syslog header magic or type", ErrCorruptRecord)
	}

	if hdr[3] == 'u' {
		r.Transport = "udp"
	} else {
		r.Transport = "tcp"
	}

	r.TimestampNano = int64(binary.BigEndian.Uint64(hdr[4:12]))

	clientIP := make([]byte, 16)
	copy(clientIP, hdr[12:28])
	ip := net.IP(clientIP)
	if v4 := ip.To4(); v4 != nil {
		r.ClientIP = v4
	} else {
		r.ClientIP = ip
	}

	msgLen := binary.BigEndian.Uint32(hdr[28:32])
	expectedCRC := binary.BigEndian.Uint32(hdr[32:36])

	r.Message = make([]byte, msgLen)
	if _, err := io.ReadFull(rd, r.Message); err != nil {
		return err
	}

	if actualCRC := crc32.ChecksumIEEE(r.Message); actualCRC != expectedCRC {
		return fmt.Errorf("%w: syslog message crc32 mismatch", ErrCorruptRecord)
	}

	return nil
}

// UnmarshalBinary decodes a byte slice into a SyslogRecord.
func (r *SyslogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 36 {
		return ErrShortBuffer
	}
	return r.UnmarshalFrom(bytes.NewReader(data))
}

// WrapRBRP wraps a UDP payload with a 28-byte RBRP replay header.
// Layout:
// [0:4]   - "RBRP"
// [4:12]  - CaptureTimestampUTC in Unix seconds (int64 big-endian)
// [12:28] - OriginalExporterIP (16 bytes net.IP)
func WrapRBRP(payload []byte, captureTimeUTC time.Time, exporterIP net.IP) []byte {
	out := make([]byte, RBRPHeaderLen+len(payload))
	copy(out[0:4], RBRPMagic[:])

	binary.BigEndian.PutUint64(out[4:12], uint64(captureTimeUTC.Unix()))

	ip16 := exporterIP.To16()
	if ip16 == nil {
		ip16 = net.IPv6zero
	}
	copy(out[12:28], ip16)

	copy(out[28:], payload)
	return out
}

// UnwrapRBRP unpacks an RBRP wrapped datagram if magic and length match.
func UnwrapRBRP(data []byte) (payload []byte, captureTimeUTC time.Time, exporterIP net.IP, ok bool) {
	if len(data) < RBRPHeaderLen {
		return nil, time.Time{}, nil, false
	}

	if data[0] != 'R' || data[1] != 'B' || data[2] != 'R' || data[3] != 'P' {
		return nil, time.Time{}, nil, false
	}

	sec := int64(binary.BigEndian.Uint64(data[4:12]))
	captureTimeUTC = time.Unix(sec, 0).UTC()

	rawIP := make([]byte, 16)
	copy(rawIP, data[12:28])
	ip := net.IP(rawIP)
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}

	return data[28:], captureTimeUTC, ip, true
}
