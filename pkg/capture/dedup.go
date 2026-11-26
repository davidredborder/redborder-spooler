package capture

import (
	"encoding/binary"
	"hash/fnv"
	"net"
	"sync"
	"time"
)

// PacketDirection defines whether packet is incoming to host or outgoing.
type PacketDirection int

const (
	DirectionHost PacketDirection = iota
	DirectionOutgoing
)

// CapturedPacket encapsulates a captured raw UDP datagram.
type CapturedPacket struct {
	CaptureTimeUTC time.Time
	SrcIP          net.IP
	DstIP          net.IP
	SrcPort        uint16
	DstPort        uint16
	UDPChecksum    uint16
	Payload        []byte
	Direction      PacketDirection
}

// PacketHandler is the callback invoked when a packet is ready for persistence.
type PacketHandler func(pkt *CapturedPacket)

type stagedItem struct {
	packets []*CapturedPacket
	timer   *time.Timer
}

// DedupRing manages LVS-DR packet deduplication within a sliding time window.
type DedupRing struct {
	mu       sync.Mutex
	window   time.Duration
	handler  PacketHandler
	staged   map[uint64]*stagedItem
	stopChan chan struct{}
}

// NewDedupRing creates a new deduplication manager with the specified window.
func NewDedupRing(window time.Duration, handler PacketHandler) *DedupRing {
	return &DedupRing{
		window:   window,
		handler:  handler,
		staged:   make(map[uint64]*stagedItem),
		stopChan: make(chan struct{}),
	}
}

func packetHash(pkt *CapturedPacket) uint64 {
	h := fnv.New64a()

	src16 := pkt.SrcIP.To16()
	if src16 == nil {
		src16 = net.IPv6zero
	}
	h.Write(src16)

	dst16 := pkt.DstIP.To16()
	if dst16 == nil {
		dst16 = net.IPv6zero
	}
	h.Write(dst16)

	var portBuf [6]byte
	binary.BigEndian.PutUint16(portBuf[0:2], pkt.SrcPort)
	binary.BigEndian.PutUint16(portBuf[2:4], pkt.DstPort)
	binary.BigEndian.PutUint16(portBuf[4:6], pkt.UDPChecksum)
	h.Write(portBuf[:])

	h.Write(pkt.Payload)
	return h.Sum64()
}

// Push ingests a packet into the deduplication ring.
func (r *DedupRing) Push(pkt *CapturedPacket) {
	if pkt.Direction == DirectionOutgoing {
		// Outgoing packets are never persisted to storage
		if r.window > 0 {
			key := packetHash(pkt)
			r.mu.Lock()
			if item, found := r.staged[key]; found {
				item.timer.Stop()
				delete(r.staged, key)
			}
			r.mu.Unlock()
		}
		return
	}

	if r.window <= 0 {
		r.handler(pkt)
		return
	}

	key := packetHash(pkt)

	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.stopChan:
		return
	default:
	}

	// Host packet: if already staged under this key, append it to avoid losing duplicates or leaking timers
	if existing, found := r.staged[key]; found {
		existing.packets = append(existing.packets, pkt)
		return
	}

	item := &stagedItem{
		packets: []*CapturedPacket{pkt},
	}
	item.timer = time.AfterFunc(r.window, func() {
		r.mu.Lock()
		cur, found := r.staged[key]
		if found {
			delete(r.staged, key)
		}
		r.mu.Unlock()

		if found && cur != nil {
			for _, p := range cur.packets {
				r.handler(p)
			}
		}
	})

	r.staged[key] = item
}

// Flush delivers all currently staged packets and empties the ring.
func (r *DedupRing) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, item := range r.staged {
		item.timer.Stop()
		delete(r.staged, key)
		for _, p := range item.packets {
			r.handler(p)
		}
	}
}

// Stop flushes and halts the dedup ring.
func (r *DedupRing) Stop() {
	r.mu.Lock()
	select {
	case <-r.stopChan:
		r.mu.Unlock()
		return
	default:
		close(r.stopChan)
	}
	r.mu.Unlock()

	r.Flush()
}
