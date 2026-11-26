package capture

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redborder/redborder-spooler/pkg/storage"
)

// AFPacketListener captures raw packets across network interfaces using AF_PACKET sockets.
type AFPacketListener struct {
	ifaces          []string
	ports           map[uint16]struct{}
	store           *storage.HourlyStore
	dedup           *DedupRing
	fd              int
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	trafficPaused   atomic.Bool
	syslogUDPPaused atomic.Bool
}

func htons(i uint16) uint16 {
	return (i<<8)&0xff00 | (i>>8)&0x00ff
}

// NewAFPacketListener creates an AF_PACKET capture listener.
func NewAFPacketListener(ifaces []string, ports []int, store *storage.HourlyStore, dedupWindow time.Duration) *AFPacketListener {
	portMap := make(map[uint16]struct{})
	for _, p := range ports {
		portMap[uint16(p)] = struct{}{}
	}

	l := &AFPacketListener{
		ifaces: ifaces,
		ports:  portMap,
		store:  store,
		fd:     -1,
	}

	handler := func(pkt *CapturedPacket) {
		if store != nil {
			if pkt.DstPort == 514 {
				if l.syslogUDPPaused.Load() {
					return
				}
				_ = store.WriteSyslog(pkt.CaptureTimeUTC, pkt.SrcIP, "udp", pkt.Payload)
			} else {
				if l.trafficPaused.Load() {
					return
				}
				_ = store.WriteUDP(pkt.CaptureTimeUTC, pkt.SrcIP, pkt.DstIP, pkt.SrcPort, pkt.DstPort, pkt.Payload)
			}
		}
	}
	l.dedup = NewDedupRing(dedupWindow, handler)

	return l
}

// SetTrafficPaused toggles capture of flow traffic packets.
func (l *AFPacketListener) SetTrafficPaused(paused bool) {
	l.trafficPaused.Store(paused)
}

// IsTrafficPaused reports whether flow traffic capture is paused.
func (l *AFPacketListener) IsTrafficPaused() bool {
	return l.trafficPaused.Load()
}

// SetSyslogUDPPaused toggles capture of UDP syslog datagrams.
func (l *AFPacketListener) SetSyslogUDPPaused(paused bool) {
	l.syslogUDPPaused.Store(paused)
}

// IsSyslogUDPPaused reports whether UDP syslog capture is paused.
func (l *AFPacketListener) IsSyslogUDPPaused() bool {
	return l.syslogUDPPaused.Load()
}

// Start begins raw packet capture.
func (l *AFPacketListener) Start(ctx context.Context) error {
	l.ctx, l.cancel = context.WithCancel(ctx)

	// Create AF_PACKET raw socket for ETH_P_ALL
	proto := htons(syscall.ETH_P_ALL)
	fd, err := syscall.Socket(syscall.AF_PACKET, syscall.SOCK_RAW, int(proto))
	if err != nil {
		return fmt.Errorf("failed to open AF_PACKET socket (requires CAP_NET_RAW): %w", err)
	}
	l.fd = fd

	// Bind to specific interface if configured and not "any"
	if len(l.ifaces) > 0 && l.ifaces[0] != "any" && l.ifaces[0] != "" {
		ifi, err := net.InterfaceByName(l.ifaces[0])
		if err == nil {
			sll := &syscall.SockaddrLinklayer{
				Protocol: proto,
				Ifindex:  ifi.Index,
			}
			_ = syscall.Bind(fd, sll)
		}
	}

	l.wg.Add(1)
	go l.captureLoop()

	return nil
}

func (l *AFPacketListener) captureLoop() {
	defer l.wg.Done()

	buf := make([]byte, 65536)

	for {
		select {
		case <-l.ctx.Done():
			return
		default:
		}

		n, from, err := syscall.Recvfrom(l.fd, buf, 0)
		if err != nil {
			select {
			case <-l.ctx.Done():
				return
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		if n < 14 { // Less than Ethernet header
			continue
		}

		var direction PacketDirection = DirectionHost
		if sll, ok := from.(*syscall.SockaddrLinklayer); ok {
			if sll.Pkttype == syscall.PACKET_OUTGOING {
				direction = DirectionOutgoing
			} else if sll.Pkttype != syscall.PACKET_HOST && sll.Pkttype != syscall.PACKET_BROADCAST && sll.Pkttype != syscall.PACKET_MULTICAST {
				// We care about incoming packets (HOST, BROADCAST, MULTICAST) and OUTGOING for LVS-DR dedup
				continue
			}
		}

		pkt, ok := l.parsePacket(buf[:n], direction)
		if ok && pkt != nil {
			l.dedup.Push(pkt)
		}
	}
}

func (l *AFPacketListener) parsePacket(data []byte, dir PacketDirection) (*CapturedPacket, bool) {
	if len(data) < 14 {
		return nil, false
	}

	ethType := binary.BigEndian.Uint16(data[12:14])
	offset := 14

	// Handle 802.1Q VLAN tag
	if ethType == 0x8100 && len(data) >= 18 {
		ethType = binary.BigEndian.Uint16(data[16:18])
		offset = 18
	}

	var (
		srcIP, dstIP net.IP
		proto        byte
		ipHdrLen     int
	)

	switch ethType {
	case 0x0800: // IPv4
		if len(data) < offset+20 {
			return nil, false
		}
		versionIHL := data[offset]
		if versionIHL>>4 != 4 {
			return nil, false
		}
		ipHdrLen = int(versionIHL&0x0F) * 4
		if len(data) < offset+ipHdrLen {
			return nil, false
		}
		proto = data[offset+9]
		srcIP = append(net.IP(nil), data[offset+12:offset+16]...)
		dstIP = append(net.IP(nil), data[offset+16:offset+20]...)
		offset += ipHdrLen

	case 0x86DD: // IPv6
		if len(data) < offset+40 {
			return nil, false
		}
		proto = data[offset+6] // NextHeader
		srcIP = append(net.IP(nil), data[offset+8:offset+24]...)
		dstIP = append(net.IP(nil), data[offset+24:offset+40]...)
		offset += 40

	default:
		return nil, false
	}

	// Must be UDP (protocol 17)
	if proto != 17 || len(data) < offset+8 {
		return nil, false
	}

	// Filter out loopback traffic (127.0.0.0/8, ::1)
	if srcIP.IsLoopback() || dstIP.IsLoopback() {
		return nil, false
	}

	srcPort := binary.BigEndian.Uint16(data[offset : offset+2])
	dstPort := binary.BigEndian.Uint16(data[offset+2 : offset+4])
	udpLen := int(binary.BigEndian.Uint16(data[offset+4 : offset+6]))
	udpChecksum := binary.BigEndian.Uint16(data[offset+6 : offset+8])

	// Check if dstPort matches monitored ports
	if _, matches := l.ports[dstPort]; !matches {
		return nil, false
	}

	if udpLen < 8 || offset+udpLen > len(data) {
		udpLen = len(data) - offset
	}

	payload := data[offset+8 : offset+udpLen]
	// Filter out replayed traffic wrapped in RBRP envelope
	if len(payload) >= storage.RBRPHeaderLen && bytes.HasPrefix(payload, storage.RBRPMagic[:]) {
		return nil, false
	}

	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)

	return &CapturedPacket{
		CaptureTimeUTC: time.Now().UTC(),
		SrcIP:          srcIP,
		DstIP:          dstIP,
		SrcPort:        srcPort,
		DstPort:        dstPort,
		UDPChecksum:    udpChecksum,
		Payload:        payloadCopy,
		Direction:      dir,
	}, true
}

// Stop terminates packet capture and flushes buffers.
func (l *AFPacketListener) Stop() {
	if l.cancel != nil {
		l.cancel()
	}
	if l.fd >= 0 {
		_ = syscall.Close(l.fd)
		l.fd = -1
	}
	l.wg.Wait()
	if l.dedup != nil {
		l.dedup.Stop()
	}
}
