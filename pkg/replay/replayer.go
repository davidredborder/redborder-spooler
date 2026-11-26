package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/redborder/redborder-spooler/pkg/storage"
)

// Options holds parameters controlling a replay session.
type Options struct {
	FromTimeUTC          time.Time
	ToTimeUTC            time.Time
	SpeedMultiplier      float64
	TargetHost           string
	UseRBRP              bool
	HTTPTargetURL        string
	SyslogTargetHost     string // default: "127.0.0.1"
	SyslogTargetPort     int    // default: 514
	SyslogInjectClientIP bool   // rewrite hostname in syslog header to recorded ClientIP (default: true for 127.0.0.1)
	PortsFilter          []uint16
	PortRemap            map[uint16]uint16 // optional destination port remapping
	ReplayType           string            // "udp", "http", "syslog", or "all"
	TimeShift            bool              // shift timestamps forward relative to current time
	Follow               bool
	OnProgress           func(packetsReplayed uint64)
	OnPacket             func(packetsReplayed uint64, captureTime time.Time)
}

// Stats summarizes the result of a completed replay session.
type Stats struct {
	PacketsReplayed        uint64
	BytesReplayed          uint64
	TemplatesSent          uint64
	HTTPRequestsReplayed   uint64
	HTTPBytesReplayed      uint64
	SyslogMessagesReplayed uint64
	SyslogMessagesDropped  uint64
	Duration               time.Duration
}

// Replayer coordinates catching up and streaming archived traffic to localhost or a target receiver.
type Replayer struct {
	store      *storage.HourlyStore
	httpClient *http.Client
}

// NewReplayer creates a new Replayer.
func NewReplayer(store *storage.HourlyStore) *Replayer {
	return &Replayer{
		store: store,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Run executes the replay loop for the given time range and settings.
func (r *Replayer) Run(ctx context.Context, opts Options) (Stats, error) {
	start := time.Now()
	var stats Stats

	targetHost := opts.TargetHost
	if targetHost == "" {
		targetHost = "127.0.0.1"
	}

	var reader *storage.RangeReader
	var err error
	if opts.Follow {
		reader, err = r.store.OpenRangeFollow(ctx, opts.FromTimeUTC)
	} else {
		reader, err = r.store.OpenRange(opts.FromTimeUTC, opts.ToTimeUTC)
	}
	if err != nil {
		return stats, fmt.Errorf("failed to open range: %w", err)
	}
	defer reader.Close()

	replayTraffic := opts.ReplayType == "" || opts.ReplayType == "all" || opts.ReplayType == "traffic" || opts.ReplayType == "udp"
	replayHTTP := (opts.ReplayType == "" || opts.ReplayType == "all" || opts.ReplayType == "http") && opts.HTTPTargetURL != ""

	var shiftDelta time.Duration
	if opts.TimeShift && !opts.FromTimeUTC.IsZero() {
		shiftDelta = time.Now().UTC().Sub(opts.FromTimeUTC)
	}

	if replayTraffic {
		// 1. Send template snapshot(s) from starting hour so downstream decoder is ready
		r.sendTemplates(opts, &stats)

		// Map of open UDP connection sockets per destination port
		udpConns := make(map[uint16]*net.UDPConn)
		defer func() {
			for _, c := range udpConns {
				_ = c.Close()
			}
		}()

		getConn := func(port uint16) (*net.UDPConn, error) {
			if c, ok := udpConns[port]; ok {
				return c, nil
			}
			targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", targetHost, port))
			if err != nil {
				return nil, err
			}
			conn, err := net.DialUDP("udp", nil, targetAddr)
			if err != nil {
				return nil, err
			}
			udpConns[port] = conn
			return conn, nil
		}

		// 2. Stream UDP WAL records with timestamp pacing
		var prevTime time.Time
		for {
			select {
			case <-ctx.Done():
				stats.Duration = time.Since(start)
				return stats, ctx.Err()
			default:
			}

			rec, err := reader.NextUDP()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				stats.Duration = time.Since(start)
				return stats, fmt.Errorf("reading next udp record failed: %w", err)
			}

			// Pacing calculation
			if opts.SpeedMultiplier > 0 && !prevTime.IsZero() {
				delta := rec.CaptureTimeUTC.Sub(prevTime)
				if delta > 0 {
					multiplier := opts.SpeedMultiplier
					if opts.Follow && time.Since(prevTime) < 3*time.Second && time.Since(rec.CaptureTimeUTC) < 3*time.Second {
						multiplier = 1.0 // Caught up to real time: stream at 1.0x
					}
					sleepDuration := time.Duration(float64(delta) / multiplier)
					select {
					case <-ctx.Done():
						stats.Duration = time.Since(start)
						return stats, ctx.Err()
					case <-time.After(sleepDuration):
					}
				}
			}
			prevTime = rec.CaptureTimeUTC

			// Filter by UDP destination port if PortsFilter is set
			if len(opts.PortsFilter) > 0 {
				matched := false
				for _, p := range opts.PortsFilter {
					if p == rec.DstPort {
						matched = true
						break
					}
				}
				if !matched {
					continue
				}
			}

			captureTime := rec.CaptureTimeUTC
			if opts.TimeShift {
				if shiftDelta == 0 && !rec.CaptureTimeUTC.IsZero() {
					shiftDelta = time.Now().UTC().Sub(rec.CaptureTimeUTC)
				}
				captureTime = captureTime.Add(shiftDelta)
				if captureTime.After(time.Now().UTC()) {
					captureTime = time.Now().UTC()
				}
			}

			var packetToSend []byte
			if opts.UseRBRP && rec.DstPort != 514 {
				packetToSend = storage.WrapRBRP(rec.Payload, captureTime, rec.SrcIP)
			} else {
				packetToSend = rec.Payload
			}

			dstPort := rec.DstPort
			if len(opts.PortRemap) > 0 {
				if mapped, ok := opts.PortRemap[dstPort]; ok {
					dstPort = mapped
				}
			}

			conn, err := getConn(dstPort)
			if err != nil {
				continue
			}

			n, err := conn.Write(packetToSend)
			if err == nil {
				stats.PacketsReplayed++
				stats.BytesReplayed += uint64(n)
				if opts.OnProgress != nil && (stats.PacketsReplayed%100 == 0 || stats.PacketsReplayed == 1) {
					opts.OnProgress(stats.PacketsReplayed)
				}
				if opts.OnPacket != nil {
					opts.OnPacket(stats.PacketsReplayed, captureTime)
				}
			}
		}

		if opts.OnProgress != nil {
			opts.OnProgress(stats.PacketsReplayed)
		}
	}

	// 3. Stream HTTP records if replayHTTP is true
	if replayHTTP {
		for {
			select {
			case <-ctx.Done():
				stats.Duration = time.Since(start)
				return stats, ctx.Err()
			default:
			}

			hRec, err := reader.NextHTTP()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				break
			}

			body := hRec.Body
			if opts.TimeShift {
				if shiftDelta == 0 {
					if !opts.FromTimeUTC.IsZero() {
						shiftDelta = time.Now().UTC().Sub(opts.FromTimeUTC)
					} else if !hRec.CaptureTimeUTC.IsZero() {
						shiftDelta = time.Now().UTC().Sub(hRec.CaptureTimeUTC)
					}
				}
				var parsed map[string]interface{}
				if err := json.Unmarshal(body, &parsed); err == nil && parsed != nil {
					if origTS, ok := parsed["timestamp"]; ok {
						switch v := origTS.(type) {
						case float64:
							parsed["timestamp"] = int64(v) + int64(shiftDelta.Seconds())
						case int64:
							parsed["timestamp"] = v + int64(shiftDelta.Seconds())
						}
						if newBody, err := json.Marshal(parsed); err == nil {
							body = newBody
						}
					}
				}
			}

			targetURL := strings.TrimRight(opts.HTTPTargetURL, "/") + hRec.URI
			req, err := http.NewRequestWithContext(ctx, hRec.Method, targetURL, bytes.NewReader(body))
			if err != nil {
				continue
			}

			for k, vv := range hRec.Headers {
				for _, v := range vv {
					req.Header.Add(k, v)
				}
			}
			req.Header.Set("X-Redborder-Replay", "true")

			resp, err := r.httpClient.Do(req)
			if err == nil {
				_ = resp.Body.Close()
				stats.HTTPRequestsReplayed++
				stats.HTTPBytesReplayed += uint64(len(body))
				if opts.OnProgress != nil && (stats.HTTPRequestsReplayed%10 == 0 || stats.HTTPRequestsReplayed == 1) {
					opts.OnProgress(stats.PacketsReplayed + stats.HTTPRequestsReplayed)
				}
			}
		}

		if opts.OnProgress != nil {
			opts.OnProgress(stats.PacketsReplayed + stats.HTTPRequestsReplayed)
		}
	}

	// 4. Stream Syslog records if ReplayType is syslog or (all and there are syslog records)
	replaySyslog := opts.ReplayType == "syslog" || (opts.ReplayType == "all" && r.store.EstimateSyslogMessages(opts.FromTimeUTC, opts.ToTimeUTC) > 0)
	if replaySyslog {
		if err := r.replaySyslog(ctx, opts, &stats); err != nil {
			stats.Duration = time.Since(start)
			return stats, err
		}
	}

	stats.Duration = time.Since(start)
	return stats, nil
}

// Replay executes the replay loop (alias for Run).
func (r *Replayer) Replay(ctx context.Context, opts Options) (Stats, error) {
	return r.Run(ctx, opts)
}

func (r *Replayer) replaySyslog(ctx context.Context, opts Options, stats *Stats) error {
	syslogHost := opts.SyslogTargetHost
	if syslogHost == "" {
		syslogHost = "127.0.0.1"
	}
	syslogPort := opts.SyslogTargetPort
	if syslogPort == 0 {
		syslogPort = 514
	}

	reader, err := r.store.NewSyslogRangeReader(opts.FromTimeUTC, opts.ToTimeUTC)
	if err != nil {
		return fmt.Errorf("failed to open syslog range reader: %w", err)
	}
	defer reader.Close()

	targetAddr := net.JoinHostPort(syslogHost, strconv.Itoa(syslogPort))
	var tcpConn net.Conn
	var udpConn *net.UDPConn
	defer func() {
		if tcpConn != nil {
			_ = tcpConn.Close()
		}
		if udpConn != nil {
			_ = udpConn.Close()
		}
	}()

	getTCPConn := func() (net.Conn, error) {
		if tcpConn != nil {
			return tcpConn, nil
		}
		var dialer net.Dialer
		c, err := dialer.DialContext(ctx, "tcp", targetAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to TCP syslog target %s: %w", targetAddr, err)
		}
		tcpConn = c
		return tcpConn, nil
	}

	getUDPConn := func() (*net.UDPConn, error) {
		if udpConn != nil {
			return udpConn, nil
		}
		uAddr, err := net.ResolveUDPAddr("udp", targetAddr)
		if err != nil {
			return nil, err
		}
		c, err := net.DialUDP("udp", nil, uAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to UDP syslog target %s: %w", targetAddr, err)
		}
		udpConn = c
		return udpConn, nil
	}

	formatSyslogMsg := func(raw []byte) []byte {
		if bytes.Contains(raw, []byte("X-Redborder-Replay")) {
			return append(append([]byte(nil), raw...), '\n')
		}
		data := make([]byte, 0, len("X-Redborder-Replay: true ")+len(raw)+1)
		data = append(data, []byte("X-Redborder-Replay: true ")...)
		data = append(data, raw...)
		data = append(data, '\n')
		return data
	}

	var shiftDelta time.Duration
	if opts.TimeShift && !opts.FromTimeUTC.IsZero() {
		shiftDelta = time.Now().UTC().Sub(opts.FromTimeUTC)
	}

	var prevTime time.Time
	var adaptiveThrottle time.Duration
	var warnedRateLimit bool
	var consecutiveSuccesses int

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if adaptiveThrottle > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(adaptiveThrottle):
			}
		}

		rec, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reading next syslog record failed: %w", err)
		}

		recTime := time.Unix(0, rec.TimestampNano).UTC()
		if opts.TimeShift {
			if shiftDelta == 0 && !recTime.IsZero() {
				shiftDelta = time.Now().UTC().Sub(recTime)
			}
			recTime = recTime.Add(shiftDelta)
		}
		if opts.SpeedMultiplier > 0 && !prevTime.IsZero() {
			delta := recTime.Sub(prevTime)
			if delta > 0 {
				sleepDuration := time.Duration(float64(delta) / opts.SpeedMultiplier)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleepDuration):
				}
			}
		}
		prevTime = recTime

		payload := rec.Message
		if opts.TimeShift {
			payload = RewriteSyslogTimestamp(payload, recTime)
		}
		if opts.SyslogInjectClientIP && len(rec.ClientIP) > 0 {
			payload = RewriteSyslogHostname(payload, net.IP(rec.ClientIP))
		}

		if strings.ToLower(rec.Transport) == "udp" {
			uConn, err := getUDPConn()
			if err != nil {
				if udpConn != nil {
					_ = udpConn.Close()
					udpConn = nil
				}
				uConn, err = getUDPConn()
				if err != nil {
					stats.SyslogMessagesDropped++
					consecutiveSuccesses = 0
					continue
				}
			}

			var writeErr error
			for attempt := 0; attempt < 3; attempt++ {
				_, writeErr = uConn.Write(payload)
				if writeErr == nil {
					break
				}

				consecutiveSuccesses = 0
				if !warnedRateLimit {
					fmt.Fprintf(os.Stderr, "\n[WARN] Target rate limit / firewall rejection detected (%v). Dynamically throttling replay speed...\n", writeErr)
					warnedRateLimit = true
				}

				// Increase adaptive throttle to smooth out burst
				if adaptiveThrottle < 50*time.Millisecond {
					adaptiveThrottle += 10 * time.Millisecond
				}

				// Close socket to clear any asynchronous ICMP EPERM/ECONNREFUSED error latches
				if udpConn != nil {
					_ = udpConn.Close()
					udpConn = nil
				}
				backoff := time.Duration((attempt+1)*50) * time.Millisecond
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
				uConn, err = getUDPConn()
				if err != nil || uConn == nil {
					break
				}
			}

			if writeErr != nil {
				stats.SyslogMessagesDropped++
				consecutiveSuccesses = 0
				continue
			}
		} else {
			tConn, err := getTCPConn()
			if err != nil {
				if tcpConn != nil {
					_ = tcpConn.Close()
					tcpConn = nil
				}
				tConn, err = getTCPConn()
				if err != nil {
					stats.SyslogMessagesDropped++
					consecutiveSuccesses = 0
					continue
				}
			}
			data := formatSyslogMsg(payload)
			var writeErr error
			for attempt := 0; attempt < 3; attempt++ {
				_, writeErr = tConn.Write(data)
				if writeErr == nil {
					break
				}

				consecutiveSuccesses = 0
				if !warnedRateLimit {
					fmt.Fprintf(os.Stderr, "\n[WARN] Target rate limit / firewall rejection detected (%v). Dynamically throttling replay speed...\n", writeErr)
					warnedRateLimit = true
				}

				if adaptiveThrottle < 50*time.Millisecond {
					adaptiveThrottle += 10 * time.Millisecond
				}

				if tcpConn != nil {
					_ = tcpConn.Close()
					tcpConn = nil
				}
				backoff := time.Duration((attempt+1)*50) * time.Millisecond
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
				tConn, err = getTCPConn()
				if err != nil || tConn == nil {
					break
				}
			}
			if writeErr != nil {
				stats.SyslogMessagesDropped++
				consecutiveSuccesses = 0
				continue
			}
		}

		consecutiveSuccesses++
		if consecutiveSuccesses%200 == 0 && adaptiveThrottle > 0 {
			adaptiveThrottle -= 2 * time.Millisecond
			if adaptiveThrottle < 0 {
				adaptiveThrottle = 0
			}
		}

		stats.SyslogMessagesReplayed++
		if opts.OnProgress != nil && (stats.SyslogMessagesReplayed%100 == 0 || stats.SyslogMessagesReplayed == 1) {
			opts.OnProgress(stats.PacketsReplayed + stats.HTTPRequestsReplayed + stats.SyslogMessagesReplayed)
		}
	}

	if opts.OnProgress != nil {
		opts.OnProgress(stats.PacketsReplayed + stats.HTTPRequestsReplayed + stats.SyslogMessagesReplayed)
	}

	return nil
}

func (r *Replayer) sendTemplates(opts Options, stats *Stats) {
	buckets, err := r.store.ListBuckets()
	if err != nil || len(buckets) == 0 {
		return
	}

	targetHost := opts.TargetHost
	if targetHost == "" {
		targetHost = "127.0.0.1"
	}

	// Look for the first bucket in the requested time range
	for _, b := range buckets {
		if b.EndTimeUTC.Before(opts.FromTimeUTC) {
			continue
		}

		snapPath := filepath.Join(r.store.SpoolDir(), b.HourPath, storage.TemplateSnapName)
		tracker := storage.NewTemplateTracker()
		if err := tracker.LoadSnapshot(snapPath); err == nil {
			templates := tracker.Snapshot()
			if len(templates) > 0 {
				refTime := b.StartTimeUTC
				if opts.TimeShift {
					refTime = time.Now().UTC()
				}
				r.transmitTemplates(targetHost, templates, opts.UseRBRP, refTime, stats)
				return
			}
		}
	}

	// Fallback to active template tracker snapshot
	if r.store.TemplateTracker() != nil {
		activeSnaps := r.store.TemplateTracker().Snapshot()
		if len(activeSnaps) > 0 {
			refTime := opts.FromTimeUTC
			if opts.TimeShift || refTime.IsZero() {
				refTime = time.Now().UTC()
			}
			r.transmitTemplates(targetHost, activeSnaps, opts.UseRBRP, refTime, stats)
		}
	}
}

func (r *Replayer) transmitTemplates(targetHost string, templates []*storage.TemplateRecord, useRBRP bool, refTime time.Time, stats *Stats) {
	conns := make(map[uint16]*net.UDPConn)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	for _, tmplRec := range templates {
		port := tmplRec.DstPort
		if port == 0 {
			port = 2055
		}

		conn, ok := conns[port]
		if !ok {
			targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", targetHost, port))
			if err != nil {
				continue
			}
			c, err := net.DialUDP("udp", nil, targetAddr)
			if err != nil {
				continue
			}
			conns[port] = c
			conn = c
		}

		data := tmplRec.Packet
		if useRBRP {
			expIP := tmplRec.ExporterIP
			if expIP == nil {
				expIP = net.ParseIP("127.0.0.1")
			}
			data = storage.WrapRBRP(tmplRec.Packet, refTime, expIP)
		}

		if _, err := conn.Write(data); err == nil {
			stats.TemplatesSent++
		}
	}
}
