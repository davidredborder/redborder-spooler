package capture

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redborder/redborder-spooler/pkg/storage"
)

// UDPSocketListener receives UDP datagrams on specified ports using standard OS sockets.
type UDPSocketListener struct {
	ports           []int
	bindAddr        string
	store           *storage.HourlyStore
	dedup           *DedupRing
	conns           []*net.UDPConn
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	FilterLoopback  bool
	trafficPaused   atomic.Bool
	syslogUDPPaused atomic.Bool
}

// NewUDPSocketListener constructs a new socket listener.
func NewUDPSocketListener(ports []int, bindAddr string, store *storage.HourlyStore, dedupWindow time.Duration) *UDPSocketListener {
	l := &UDPSocketListener{
		ports:          ports,
		bindAddr:       bindAddr,
		store:          store,
		FilterLoopback: true,
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
func (l *UDPSocketListener) SetTrafficPaused(paused bool) {
	l.trafficPaused.Store(paused)
}

// IsTrafficPaused reports whether flow traffic capture is paused.
func (l *UDPSocketListener) IsTrafficPaused() bool {
	return l.trafficPaused.Load()
}

// SetSyslogUDPPaused toggles capture of UDP syslog datagrams.
func (l *UDPSocketListener) SetSyslogUDPPaused(paused bool) {
	l.syslogUDPPaused.Store(paused)
}

// IsSyslogUDPPaused reports whether UDP syslog capture is paused.
func (l *UDPSocketListener) IsSyslogUDPPaused() bool {
	return l.syslogUDPPaused.Load()
}

// Start begins listening on all configured UDP ports.
func (l *UDPSocketListener) Start(ctx context.Context) error {
	l.ctx, l.cancel = context.WithCancel(ctx)

	for _, port := range l.ports {
		addrStr := fmt.Sprintf("%s:%d", l.bindAddr, port)
		udpAddr, err := net.ResolveUDPAddr("udp", addrStr)
		if err != nil {
			l.Stop()
			return fmt.Errorf("failed to resolve UDP addr %s: %w", addrStr, err)
		}

		conn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			l.Stop()
			return fmt.Errorf("failed to listen on UDP %s: %w", addrStr, err)
		}
		_ = conn.SetReadBuffer(4 * 1024 * 1024)

		l.conns = append(l.conns, conn)
		l.wg.Add(1)
		go l.readLoop(conn, uint16(port))
	}

	return nil
}

func (l *UDPSocketListener) readLoop(conn *net.UDPConn, listenPort uint16) {
	defer l.wg.Done()

	buf := make([]byte, 65536)
	dstIP := net.ParseIP(l.bindAddr)
	if dstIP == nil {
		dstIP = net.IPv4zero
	}

	for {
		select {
		case <-l.ctx.Done():
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			select {
			case <-l.ctx.Done():
				return
			default:
				continue
			}
		}

		captureTime := time.Now().UTC()
		payload := make([]byte, n)
		copy(payload, buf[:n])

		// Filter out loopback unless explicitly disabled
		if l.FilterLoopback && (remoteAddr.IP.IsLoopback() || dstIP.IsLoopback()) {
			continue
		}
		// Filter out replayed traffic wrapped in RBRP envelope
		if len(payload) >= storage.RBRPHeaderLen && bytes.HasPrefix(payload, storage.RBRPMagic[:]) {
			continue
		}

		pkt := &CapturedPacket{
			CaptureTimeUTC: captureTime,
			SrcIP:          remoteAddr.IP,
			DstIP:          dstIP,
			SrcPort:        uint16(remoteAddr.Port),
			DstPort:        listenPort,
			Payload:        payload,
			Direction:      DirectionHost,
		}

		l.dedup.Push(pkt)
	}
}

// Stop terminates all listener loops and flushes deduplication buffers.
func (l *UDPSocketListener) Stop() {
	if l.cancel != nil {
		l.cancel()
	}
	for _, conn := range l.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
	l.wg.Wait()
	if l.dedup != nil {
		l.dedup.Stop()
	}
}
