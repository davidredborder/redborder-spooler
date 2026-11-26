package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
)

const (
	UDPWalName       = "udp.wal"
	UDPIdxName       = "udp.idx"
	HTTPWalName      = "http.wal"
	SyslogFileName   = "syslog.wal"
	SyslogWalName    = SyslogFileName
	MetaFileName     = "meta.json"
	TemplateSnapName = "templates.snap"
	IdxEntries       = 60
	UnsetOffset      = math.MaxUint64
)

// BucketMeta stores statistics for a single hourly bucket.
type BucketMeta struct {
	HourPath       string    `json:"hour_path"` // e.g. "2026-11-24/10"
	StartTimeUTC   time.Time `json:"start_time_utc"`
	EndTimeUTC     time.Time `json:"end_time_utc"`
	FirstPacketUTC time.Time `json:"first_packet_utc,omitempty"`
	LastPacketUTC  time.Time `json:"last_packet_utc,omitempty"`
	UDPPackets     uint64    `json:"udp_packets"`
	UDPBytes       uint64    `json:"udp_bytes"`
	DiskBytes      int64     `json:"disk_bytes,omitempty"`
	HTTPRequests   uint64    `json:"http_requests"`
	HTTPBytes      uint64    `json:"http_bytes"`
	SyslogMessages uint64    `json:"syslog_messages"`
	SyslogBytes    uint64    `json:"syslog_bytes"`
	Exporters      []string  `json:"exporters"`
}

type activeHour struct {
	hourKey     string // "YYYY-MM-DD/HH"
	dirPath     string
	udpWalFile  *os.File
	httpFile    *os.File
	syslogFile  *os.File
	udpOffset   uint64
	udpIdx      [IdxEntries]uint64
	meta        BucketMeta
	exporterSet map[string]struct{}
	dirty       bool
}

// HourlyStore manages writing and reading hourly partitioned traffic logs.
type HourlyStore struct {
	mu         sync.Mutex
	cfg        *config.Config
	active     *activeHour
	templates  *TemplateTracker
}

// NewHourlyStore creates a new HourlyStore instance.
func NewHourlyStore(cfg *config.Config) (*HourlyStore, error) {
	if err := os.MkdirAll(cfg.SpoolDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create spool dir %s: %w", cfg.SpoolDir, err)
	}

	return &HourlyStore{
		cfg:       cfg,
		templates: NewTemplateTracker(),
	}, nil
}

// TemplateTracker returns the template tracker instance.
func (s *HourlyStore) TemplateTracker() *TemplateTracker {
	return s.templates
}

// SpoolDir returns the configured base spool directory.
func (s *HourlyStore) SpoolDir() string {
	return s.cfg.SpoolDir
}

// getOrOpenHour returns the activeHour for timestamp t (in UTC).
func (s *HourlyStore) getOrOpenHour(t time.Time) (*activeHour, error) {
	tUTC := t.UTC()
	hourKey := tUTC.Format("2006-01-02/15")

	if s.active != nil && s.active.hourKey == hourKey {
		return s.active, nil
	}

	// Close existing active hour if open
	if s.active != nil {
		if err := s.closeActiveHour(); err != nil {
			return nil, err
		}
	}

	dirPath := filepath.Join(s.cfg.SpoolDir, hourKey)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create hour dir %s: %w", dirPath, err)
	}

	udpWalPath := filepath.Join(dirPath, UDPWalName)
	udpF, err := os.OpenFile(udpWalPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", udpWalPath, err)
	}

	fi, err := udpF.Stat()
	if err != nil {
		udpF.Close()
		return nil, err
	}
	initialOffset := uint64(fi.Size())

	httpWalPath := filepath.Join(dirPath, HTTPWalName)
	httpF, err := os.OpenFile(httpWalPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		udpF.Close()
		return nil, fmt.Errorf("failed to open %s: %w", httpWalPath, err)
	}

	syslogWalPath := filepath.Join(dirPath, SyslogFileName)
	syslogF, err := os.OpenFile(syslogWalPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		udpF.Close()
		httpF.Close()
		return nil, fmt.Errorf("failed to open %s: %w", syslogWalPath, err)
	}

	// Load or initialize udp.idx
	var udpIdx [IdxEntries]uint64
	idxPath := filepath.Join(dirPath, UDPIdxName)
	if idxData, err := os.ReadFile(idxPath); err == nil && len(idxData) == IdxEntries*8 {
		for i := 0; i < IdxEntries; i++ {
			udpIdx[i] = binary.BigEndian.Uint64(idxData[i*8 : (i+1)*8])
		}
	} else {
		for i := 0; i < IdxEntries; i++ {
			udpIdx[i] = UnsetOffset
		}
	}

	// Load meta if existing
	var meta BucketMeta
	metaPath := filepath.Join(dirPath, MetaFileName)
	if mData, err := os.ReadFile(metaPath); err == nil {
		_ = json.Unmarshal(mData, &meta)
	} else {
		hourStart := tUTC.Truncate(time.Hour)
		meta = BucketMeta{
			HourPath:     hourKey,
			StartTimeUTC: hourStart,
			EndTimeUTC:   hourStart.Add(time.Hour),
			Exporters:    make([]string, 0),
		}
	}

	expSet := make(map[string]struct{})
	for _, e := range meta.Exporters {
		expSet[e] = struct{}{}
	}

	s.active = &activeHour{
		hourKey:     hourKey,
		dirPath:     dirPath,
		udpWalFile:  udpF,
		httpFile:    httpF,
		syslogFile:  syslogF,
		udpOffset:   initialOffset,
		udpIdx:      udpIdx,
		meta:        meta,
		exporterSet: expSet,
	}

	return s.active, nil
}

func (s *HourlyStore) closeActiveHour() error {
	if s.active == nil {
		return nil
	}

	a := s.active
	s.active = nil

	// Flush and close files
	if a.udpWalFile != nil {
		_ = a.udpWalFile.Sync()
		_ = a.udpWalFile.Close()
	}
	if a.httpFile != nil {
		_ = a.httpFile.Sync()
		_ = a.httpFile.Close()
	}
	if a.syslogFile != nil {
		_ = a.syslogFile.Sync()
		_ = a.syslogFile.Close()
	}

	// Write udp.idx
	idxPath := filepath.Join(a.dirPath, UDPIdxName)
	var idxBuf [IdxEntries * 8]byte
	for i := 0; i < IdxEntries; i++ {
		binary.BigEndian.PutUint64(idxBuf[i*8:(i+1)*8], a.udpIdx[i])
	}
	_ = os.WriteFile(idxPath, idxBuf[:], 0644)

	// Update exporters list in meta
	a.meta.Exporters = make([]string, 0, len(a.exporterSet))
	for e := range a.exporterSet {
		a.meta.Exporters = append(a.meta.Exporters, e)
	}
	sort.Strings(a.meta.Exporters)

	metaPath := filepath.Join(a.dirPath, MetaFileName)
	tmpPath := metaPath + ".tmp"
	if metaJSON, err := json.MarshalIndent(a.meta, "", "  "); err == nil {
		if err := os.WriteFile(tmpPath, metaJSON, 0644); err == nil {
			_ = os.Rename(tmpPath, metaPath)
		} else {
			_ = os.WriteFile(metaPath, metaJSON, 0644)
		}
	}
	a.dirty = false

	// Save templates snapshot
	snapPath := filepath.Join(a.dirPath, TemplateSnapName)
	_ = s.templates.SaveSnapshot(snapPath)

	return nil
}

// SyncMeta flushes active hour's meta.json and udp.idx to disk if dirty.
func (s *HourlyStore) SyncMeta() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active == nil || !s.active.dirty {
		return nil
	}

	a := s.active

	// 1. Write udp.idx
	idxPath := filepath.Join(a.dirPath, UDPIdxName)
	var idxBuf [IdxEntries * 8]byte
	for i := 0; i < IdxEntries; i++ {
		binary.BigEndian.PutUint64(idxBuf[i*8:(i+1)*8], a.udpIdx[i])
	}
	_ = os.WriteFile(idxPath, idxBuf[:], 0644)

	// 2. Update exporters list in meta
	a.meta.Exporters = make([]string, 0, len(a.exporterSet))
	for e := range a.exporterSet {
		a.meta.Exporters = append(a.meta.Exporters, e)
	}
	sort.Strings(a.meta.Exporters)

	// 3. Atomically write meta.json
	metaPath := filepath.Join(a.dirPath, MetaFileName)
	tmpPath := metaPath + ".tmp"
	if metaJSON, err := json.MarshalIndent(a.meta, "", "  "); err == nil {
		if err := os.WriteFile(tmpPath, metaJSON, 0644); err == nil {
			_ = os.Rename(tmpPath, metaPath)
		} else {
			_ = os.WriteFile(metaPath, metaJSON, 0644)
		}
	}

	a.dirty = false
	return nil
}

// WriteUDP writes a framed UDP datagram record to the active hour.
func (s *HourlyStore) WriteUDP(t time.Time, srcIP, dstIP net.IP, srcPort, dstPort uint16, payload []byte) error {
	if dstPort == 514 {
		return s.WriteSyslog(t, srcIP, "udp", payload)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Track template if applicable
	s.templates.ProcessPacket(srcIP, dstPort, payload)

	act, err := s.getOrOpenHour(t)
	if err != nil {
		return err
	}

	rec := UDPRecord{
		CaptureTimeUTC: t.UTC(),
		SrcIP:          srcIP,
		DstIP:          dstIP,
		SrcPort:        srcPort,
		DstPort:        dstPort,
		Payload:        payload,
	}

	data, err := rec.MarshalBinary()
	if err != nil {
		return err
	}

	recLen := uint64(len(data))
	minute := t.UTC().Minute()
	if minute >= 0 && minute < IdxEntries {
		if act.udpIdx[minute] == UnsetOffset {
			act.udpIdx[minute] = act.udpOffset
		}
	}

	if _, err := act.udpWalFile.Write(data); err != nil {
		return err
	}

	act.udpOffset += recLen
	act.meta.UDPPackets++
	act.meta.UDPBytes += uint64(len(payload))
	if act.meta.FirstPacketUTC.IsZero() || t.UTC().Before(act.meta.FirstPacketUTC) {
		act.meta.FirstPacketUTC = t.UTC()
	}
	if t.UTC().After(act.meta.LastPacketUTC) {
		act.meta.LastPacketUTC = t.UTC()
	}
	act.exporterSet[srcIP.String()] = struct{}{}
	act.dirty = true

	return nil
}

// WriteHTTP writes a framed HTTP request record to the active hour.
func (s *HourlyStore) WriteHTTP(t time.Time, clientIP net.IP, method, uri string, headers map[string][]string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	act, err := s.getOrOpenHour(t)
	if err != nil {
		return err
	}

	rec := HTTPRecord{
		CaptureTimeUTC: t.UTC(),
		ClientIP:       clientIP,
		Method:         method,
		URI:            uri,
		Headers:        headers,
		Body:           body,
	}

	data, err := rec.MarshalBinary()
	if err != nil {
		return err
	}

	if _, err := act.httpFile.Write(data); err != nil {
		return err
	}

	act.meta.HTTPRequests++
	act.meta.HTTPBytes += uint64(len(body))
	if act.meta.FirstPacketUTC.IsZero() || t.UTC().Before(act.meta.FirstPacketUTC) {
		act.meta.FirstPacketUTC = t.UTC()
	}
	if t.UTC().After(act.meta.LastPacketUTC) {
		act.meta.LastPacketUTC = t.UTC()
	}
	act.dirty = true

	return nil
}

// WriteSyslog writes a framed Syslog record to the active hour.
func (s *HourlyStore) WriteSyslog(t time.Time, clientIP net.IP, transport string, msg []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	act, err := s.getOrOpenHour(t)
	if err != nil {
		return err
	}

	ipBytes := clientIP
	if v4 := clientIP.To4(); v4 != nil {
		ipBytes = v4
	}

	rec := SyslogRecord{
		TimestampNano: t.UTC().UnixNano(),
		ClientIP:      ipBytes,
		Transport:     transport,
		Message:       msg,
	}

	data, err := rec.MarshalBinary()
	if err != nil {
		return err
	}

	if _, err := act.syslogFile.Write(data); err != nil {
		return err
	}

	act.meta.SyslogMessages++
	act.meta.SyslogBytes += uint64(len(msg))
	if act.meta.FirstPacketUTC.IsZero() || t.UTC().Before(act.meta.FirstPacketUTC) {
		act.meta.FirstPacketUTC = t.UTC()
	}
	if t.UTC().After(act.meta.LastPacketUTC) {
		act.meta.LastPacketUTC = t.UTC()
	}
	act.exporterSet[clientIP.String()] = struct{}{}
	act.dirty = true

	return nil
}

// CloseHour manually flushes and closes the active hour.
func (s *HourlyStore) CloseHour() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeActiveHour()
}

// Close flushes and shuts down the store.
func (s *HourlyStore) Close() error {
	return s.CloseHour()
}

// ListBuckets returns metadata for all available hourly buckets sorted chronologically.
func (s *HourlyStore) ListBuckets() ([]BucketMeta, error) {
	s.mu.Lock()
	if s.active != nil {
		_ = s.closeActiveHour()
	}
	s.mu.Unlock()

	var buckets []BucketMeta

	dayDirs, err := os.ReadDir(s.cfg.SpoolDir)
	if err != nil {
		return nil, err
	}

	for _, dayEntry := range dayDirs {
		if !dayEntry.IsDir() {
			continue
		}
		dayName := dayEntry.Name() // e.g. "2026-11-24"
		dayPath := filepath.Join(s.cfg.SpoolDir, dayName)

		hourDirs, err := os.ReadDir(dayPath)
		if err != nil {
			continue
		}

		for _, hourEntry := range hourDirs {
			if !hourEntry.IsDir() {
				continue
			}
			hourName := hourEntry.Name() // e.g. "10"
			hourPath := filepath.Join(dayPath, hourName)

			var meta BucketMeta
			metaFilePath := filepath.Join(hourPath, MetaFileName)
			if mData, err := os.ReadFile(metaFilePath); err == nil {
				if err := json.Unmarshal(mData, &meta); err == nil {
					meta.DiskBytes = calculateDirSize(hourPath)
					buckets = append(buckets, meta)
					continue
				}
			}

			// Fallback: construct basic meta
			parsedTime, _ := time.Parse("2006-01-02/15", fmt.Sprintf("%s/%s", dayName, hourName))
			buckets = append(buckets, BucketMeta{
				HourPath:     fmt.Sprintf("%s/%s", dayName, hourName),
				StartTimeUTC: parsedTime.UTC(),
				EndTimeUTC:   parsedTime.Add(time.Hour).UTC(),
				DiskBytes:    calculateDirSize(hourPath),
			})
		}
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].StartTimeUTC.Before(buckets[j].StartTimeUTC)
	})

	return buckets, nil
}

func calculateDirSize(dir string) int64 {
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if !e.IsDir() {
			if info, err := e.Info(); err == nil {
				total += info.Size()
			}
		}
	}
	return total
}

// TotalDiskUsage returns the total bytes occupied by all buckets in the spool dir.
func (s *HourlyStore) TotalDiskUsage() (int64, error) {
	buckets, err := s.ListBuckets()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, b := range buckets {
		total += b.DiskBytes
	}
	return total, nil
}

// FormatBytes formats byte counts into human-readable strings (e.g. "12.4 MB").
func FormatBytes(b int64) string {
	if b <= 0 {
		return "0 B"
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// RangeReader reads UDP and HTTP records across hourly buckets.
type RangeReader struct {
	store          *HourlyStore
	ctx            context.Context
	fromUTC        time.Time
	toUTC          time.Time
	bucketPaths    []string
	currentIdx     int
	udpWalFile     *os.File
	httpCurrentIdx int
	httpWalFile    *os.File
	follow         bool
}

// EstimatePackets calculates the approximate total number of UDP packets within [fromTimeUTC, toTimeUTC].
// For full intermediate hours, 100% of the bucket packets are counted.
// For boundary hours (start and end), it reads udp.idx for exact byte proportions, falling back
// to minute-based time ratios if the index is absent or empty.
func (s *HourlyStore) EstimatePackets(fromTimeUTC, toTimeUTC time.Time) uint64 {
	s.mu.Lock()
	if s.active != nil {
		_ = s.closeActiveHour()
	}
	s.mu.Unlock()

	if fromTimeUTC.After(toTimeUTC) {
		return 0
	}

	buckets, err := s.ListBuckets()
	if err != nil || len(buckets) == 0 {
		return 0
	}

	fromTrunc := fromTimeUTC.Truncate(time.Hour)
	toTrunc := toTimeUTC.Truncate(time.Hour)

	var total uint64
	for _, b := range buckets {
		if b.StartTimeUTC.Before(fromTrunc) || b.StartTimeUTC.After(toTrunc) {
			continue
		}
		if b.UDPPackets == 0 {
			continue
		}

		isStart := b.StartTimeUTC.Equal(fromTrunc)
		isEnd := b.StartTimeUTC.Equal(toTrunc)

		if isStart && isEnd {
			// Range is entirely within this single hour
			startMin := fromTimeUTC.Minute()
			endMin := toTimeUTC.Minute()
			if endMin < startMin {
				continue
			}
			ratio := s.estimateBucketRatio(b.HourPath, startMin, endMin)
			total += uint64(float64(b.UDPPackets) * ratio)
		} else if isStart {
			startMin := fromTimeUTC.Minute()
			if startMin == 0 && fromTimeUTC.Second() == 0 {
				total += b.UDPPackets
			} else {
				ratio := s.estimateBucketRatio(b.HourPath, startMin, 60)
				total += uint64(float64(b.UDPPackets) * ratio)
			}
		} else if isEnd {
			endMin := toTimeUTC.Minute()
			if endMin == 0 && toTimeUTC.Second() == 0 {
				continue
			}
			ratio := s.estimateBucketRatio(b.HourPath, 0, endMin)
			total += uint64(float64(b.UDPPackets) * ratio)
		} else {
			// Fully contained intermediate hour
			total += b.UDPPackets
		}
	}

	return total
}

// EstimateHTTPRequests calculates the approximate total number of HTTP requests within [fromTimeUTC, toTimeUTC].
func (s *HourlyStore) EstimateHTTPRequests(fromTimeUTC, toTimeUTC time.Time) uint64 {
	s.mu.Lock()
	if s.active != nil {
		_ = s.closeActiveHour()
	}
	s.mu.Unlock()

	if fromTimeUTC.After(toTimeUTC) {
		return 0
	}

	buckets, err := s.ListBuckets()
	if err != nil || len(buckets) == 0 {
		return 0
	}

	fromTrunc := fromTimeUTC.Truncate(time.Hour)
	toTrunc := toTimeUTC.Truncate(time.Hour)

	var total uint64
	for _, b := range buckets {
		if b.StartTimeUTC.Before(fromTrunc) || b.StartTimeUTC.After(toTrunc) {
			continue
		}
		if b.HTTPRequests == 0 {
			continue
		}

		isStart := b.StartTimeUTC.Equal(fromTrunc)
		isEnd := b.StartTimeUTC.Equal(toTrunc)

		if isStart && isEnd {
			startMin := fromTimeUTC.Minute()
			endMin := toTimeUTC.Minute()
			if endMin < startMin {
				continue
			}
			ratio := float64(endMin-startMin) / 60.0
			total += uint64(float64(b.HTTPRequests) * ratio)
		} else if isStart {
			startMin := fromTimeUTC.Minute()
			ratio := float64(60-startMin) / 60.0
			total += uint64(float64(b.HTTPRequests) * ratio)
		} else if isEnd {
			endMin := toTimeUTC.Minute()
			ratio := float64(endMin) / 60.0
			total += uint64(float64(b.HTTPRequests) * ratio)
		} else {
			total += b.HTTPRequests
		}
	}
	return total
}

// EstimateSyslogMessages calculates the approximate total number of Syslog messages within [fromTimeUTC, toTimeUTC].
func (s *HourlyStore) EstimateSyslogMessages(fromTimeUTC, toTimeUTC time.Time) uint64 {
	s.mu.Lock()
	if s.active != nil {
		_ = s.closeActiveHour()
	}
	s.mu.Unlock()

	if fromTimeUTC.After(toTimeUTC) {
		return 0
	}

	buckets, err := s.ListBuckets()
	if err != nil || len(buckets) == 0 {
		return 0
	}

	fromTrunc := fromTimeUTC.Truncate(time.Hour)
	toTrunc := toTimeUTC.Truncate(time.Hour)

	var total uint64
	for _, b := range buckets {
		if b.StartTimeUTC.Before(fromTrunc) || b.StartTimeUTC.After(toTrunc) {
			continue
		}
		if b.SyslogMessages == 0 {
			continue
		}

		isStart := b.StartTimeUTC.Equal(fromTrunc)
		isEnd := b.StartTimeUTC.Equal(toTrunc)

		if isStart && isEnd {
			startMin := fromTimeUTC.Minute()
			endMin := toTimeUTC.Minute()
			if endMin < startMin {
				continue
			}
			ratio := float64(endMin-startMin) / 60.0
			total += uint64(float64(b.SyslogMessages) * ratio)
		} else if isStart {
			startMin := fromTimeUTC.Minute()
			ratio := float64(60-startMin) / 60.0
			total += uint64(float64(b.SyslogMessages) * ratio)
		} else if isEnd {
			endMin := toTimeUTC.Minute()
			ratio := float64(endMin) / 60.0
			total += uint64(float64(b.SyslogMessages) * ratio)
		} else {
			total += b.SyslogMessages
		}
	}
	return total
}

func (s *HourlyStore) estimateBucketRatio(hourPath string, startMin, endMin int) float64 {
	if startMin < 0 {
		startMin = 0
	}
	if endMin > 60 {
		endMin = 60
	}
	if startMin >= endMin {
		return 0.0
	}

	bucketDir := filepath.Join(s.cfg.SpoolDir, hourPath)
	idxPath := filepath.Join(bucketDir, UDPIdxName)
	walPath := filepath.Join(bucketDir, UDPWalName)

	idxData, err := os.ReadFile(idxPath)
	fi, statErr := os.Stat(walPath)
	if err == nil && len(idxData) == IdxEntries*8 && statErr == nil && fi.Size() > 0 {
		walSize := uint64(fi.Size())
		var offsets [IdxEntries]uint64
		for i := 0; i < IdxEntries; i++ {
			offsets[i] = binary.BigEndian.Uint64(idxData[i*8 : (i+1)*8])
		}

		startOffset := uint64(0)
		if startMin > 0 {
			startOffset = walSize
			for m := startMin; m < IdxEntries; m++ {
				if offsets[m] != UnsetOffset {
					startOffset = offsets[m]
					break
				}
			}
		}

		endOffset := walSize
		if endMin < 60 {
			found := false
			for m := endMin; m < IdxEntries; m++ {
				if offsets[m] != UnsetOffset {
					endOffset = offsets[m]
					found = true
					break
				}
			}
			if !found {
				endOffset = walSize
			}
		}

		if endOffset > startOffset && walSize > 0 {
			ratio := float64(endOffset-startOffset) / float64(walSize)
			if ratio > 1.0 {
				ratio = 1.0
			}
			return ratio
		}
	}

	// Fallback to minute-based proportional ratio
	return float64(endMin-startMin) / 60.0
}

// OpenRange opens a reader for records within [fromTimeUTC, toTimeUTC].
func (s *HourlyStore) OpenRange(fromTimeUTC, toTimeUTC time.Time) (*RangeReader, error) {
	s.mu.Lock()
	if s.active != nil {
		_ = s.closeActiveHour()
	}
	s.mu.Unlock()

	buckets, err := s.ListBuckets()
	if err != nil {
		return nil, err
	}

	var matchingPaths []string
	fromTrunc := fromTimeUTC.Truncate(time.Hour)
	toTrunc := toTimeUTC.Truncate(time.Hour)

	for _, b := range buckets {
		if b.StartTimeUTC.Before(fromTrunc) {
			continue
		}
		if !toTimeUTC.IsZero() && b.StartTimeUTC.After(toTrunc) {
			continue
		}
		matchingPaths = append(matchingPaths, filepath.Join(s.cfg.SpoolDir, b.HourPath))
	}

	rr := &RangeReader{
		store:       s,
		fromUTC:     fromTimeUTC,
		toUTC:       toTimeUTC,
		bucketPaths: matchingPaths,
		currentIdx:  0,
		follow:      false,
	}

	return rr, nil
}

// OpenRangeFollow opens a streaming reader starting at fromTimeUTC that continuously
// follows active writes across WAL files and hourly bucket rollovers until ctx is canceled.
func (s *HourlyStore) OpenRangeFollow(ctx context.Context, fromTimeUTC time.Time) (*RangeReader, error) {
	buckets, err := s.ListBuckets()
	if err != nil {
		return nil, err
	}

	var matchingPaths []string
	fromTrunc := fromTimeUTC.Truncate(time.Hour)

	for _, b := range buckets {
		if !b.StartTimeUTC.Before(fromTrunc) {
			matchingPaths = append(matchingPaths, filepath.Join(s.cfg.SpoolDir, b.HourPath))
		}
	}

	rr := &RangeReader{
		store:       s,
		ctx:         ctx,
		fromUTC:     fromTimeUTC,
		toUTC:       time.Time{}, // Zero time means no upper bound
		bucketPaths: matchingPaths,
		currentIdx:  0,
		follow:      true,
	}

	return rr, nil
}

func (r *RangeReader) hasNewerBucket() bool {
	if r.store == nil {
		return false
	}
	buckets, err := r.store.ListBuckets()
	if err != nil || len(buckets) == 0 {
		return false
	}
	latestBucketPath := filepath.Join(r.store.cfg.SpoolDir, buckets[len(buckets)-1].HourPath)
	if len(r.bucketPaths) == 0 {
		r.bucketPaths = append(r.bucketPaths, latestBucketPath)
		return true
	}
	currentBucketPath := ""
	if r.currentIdx < len(r.bucketPaths) {
		currentBucketPath = r.bucketPaths[r.currentIdx]
	}
	if latestBucketPath != currentBucketPath {
		existing := make(map[string]bool)
		for _, p := range r.bucketPaths {
			existing[p] = true
		}
		foundNew := false
		for _, b := range buckets {
			bp := filepath.Join(r.store.cfg.SpoolDir, b.HourPath)
			if !existing[bp] && !b.StartTimeUTC.Before(r.fromUTC.Truncate(time.Hour)) {
				r.bucketPaths = append(r.bucketPaths, bp)
				existing[bp] = true
				foundNew = true
			}
		}
		return foundNew
	}
	return false
}

func (r *RangeReader) waitForNewBucket() error {
	for {
		if r.hasNewerBucket() {
			return nil
		}
		if err := r.sleepWithContext(50 * time.Millisecond); err != nil {
			return err
		}
	}
}

func (r *RangeReader) sleepWithContext(d time.Duration) error {
	if r.ctx == nil {
		time.Sleep(d)
		return nil
	}
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// Close closes any open files in the RangeReader.
func (r *RangeReader) Close() error {
	if r.udpWalFile != nil {
		_ = r.udpWalFile.Close()
		r.udpWalFile = nil
	}
	if r.httpWalFile != nil {
		_ = r.httpWalFile.Close()
		r.httpWalFile = nil
	}
	return nil
}

// NextUDP reads the next UDPRecord within the requested time range.
func (r *RangeReader) NextUDP() (*UDPRecord, error) {
	for {
		if r.ctx != nil {
			select {
			case <-r.ctx.Done():
				return nil, r.ctx.Err()
			default:
			}
		}

		if r.udpWalFile == nil {
			if r.currentIdx >= len(r.bucketPaths) {
				if !r.follow {
					return nil, io.EOF
				}
				if err := r.waitForNewBucket(); err != nil {
					return nil, err
				}
				continue
			}
			path := filepath.Join(r.bucketPaths[r.currentIdx], UDPWalName)
			f, err := os.Open(path)
			if err != nil {
				if r.follow && r.currentIdx == len(r.bucketPaths)-1 {
					if err := r.sleepWithContext(50 * time.Millisecond); err != nil {
						return nil, err
					}
					continue
				}
				r.currentIdx++
				continue
			}

			// Minute seek only if this bucket is the exact hour of r.fromUTC
			bucketRelPath := filepath.Join(filepath.Base(filepath.Dir(r.bucketPaths[r.currentIdx])), filepath.Base(r.bucketPaths[r.currentIdx]))
			if bucketRelPath == r.fromUTC.Format("2006-01-02/15") {
				idxPath := filepath.Join(r.bucketPaths[r.currentIdx], UDPIdxName)
				if idxData, err := os.ReadFile(idxPath); err == nil && len(idxData) == IdxEntries*8 {
					startMinute := r.fromUTC.Minute()
					for m := startMinute; m < IdxEntries; m++ {
						off := binary.BigEndian.Uint64(idxData[m*8 : (m+1)*8])
						if off != UnsetOffset {
							_, _ = f.Seek(int64(off), io.SeekStart)
							break
						}
					}
				}
			}

			r.udpWalFile = f
		}

		currOffset, _ := r.udpWalFile.Seek(0, io.SeekCurrent)
		var rec UDPRecord
		err := rec.UnmarshalFrom(r.udpWalFile)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if r.follow {
					if r.currentIdx < len(r.bucketPaths)-1 || r.hasNewerBucket() {
						_ = r.udpWalFile.Close()
						r.udpWalFile = nil
						r.currentIdx++
						continue
					}
					_, _ = r.udpWalFile.Seek(currOffset, io.SeekStart)
					if sleepErr := r.sleepWithContext(50 * time.Millisecond); sleepErr != nil {
						return nil, sleepErr
					}
					continue
				}

				_ = r.udpWalFile.Close()
				r.udpWalFile = nil
				r.currentIdx++
				continue
			}
			return nil, err
		}

		if rec.CaptureTimeUTC.Before(r.fromUTC) {
			continue
		}
		if !r.toUTC.IsZero() && rec.CaptureTimeUTC.After(r.toUTC) {
			return nil, io.EOF
		}

		return &rec, nil
	}
}

// NextHTTP reads the next HTTPRecord within the requested time range.
func (r *RangeReader) NextHTTP() (*HTTPRecord, error) {
	for {
		if r.httpWalFile == nil {
			if r.httpCurrentIdx >= len(r.bucketPaths) {
				return nil, io.EOF
			}
			path := filepath.Join(r.bucketPaths[r.httpCurrentIdx], HTTPWalName)
			f, err := os.Open(path)
			if err != nil {
				r.httpCurrentIdx++
				continue
			}
			r.httpWalFile = f
		}

		var rec HTTPRecord
		err := rec.UnmarshalFrom(r.httpWalFile)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				_ = r.httpWalFile.Close()
				r.httpWalFile = nil
				r.httpCurrentIdx++
				continue
			}
			return nil, err
		}

		if rec.CaptureTimeUTC.Before(r.fromUTC) {
			continue
		}
		if rec.CaptureTimeUTC.After(r.toUTC) {
			return nil, io.EOF
		}

		return &rec, nil
	}
}

// FirstAvailableTime returns the timestamp of the earliest recorded packet across all buckets.
func (s *HourlyStore) FirstAvailableTime() (time.Time, bool) {
	buckets, err := s.ListBuckets()
	if err != nil || len(buckets) == 0 {
		return time.Time{}, false
	}

	for _, b := range buckets {
		if !b.FirstPacketUTC.IsZero() {
			return b.FirstPacketUTC, true
		}
		// Fallback: peek first record of udp.wal
		walPath := filepath.Join(s.cfg.SpoolDir, b.HourPath, UDPWalName)
		if f, err := os.Open(walPath); err == nil {
			var uRec UDPRecord
			err := uRec.UnmarshalFrom(f)
			_ = f.Close()
			if err == nil {
				return uRec.CaptureTimeUTC, true
			}
		}
		// Fallback: peek first record of http.wal
		hPath := filepath.Join(s.cfg.SpoolDir, b.HourPath, HTTPWalName)
		if f, err := os.Open(hPath); err == nil {
			var hRec HTTPRecord
			err := hRec.UnmarshalFrom(f)
			_ = f.Close()
			if err == nil {
				return hRec.CaptureTimeUTC, true
			}
		}
		// Fallback: peek first record of syslog.wal
		sLogPath := filepath.Join(s.cfg.SpoolDir, b.HourPath, SyslogFileName)
		if f, err := os.Open(sLogPath); err == nil {
			var sRec SyslogRecord
			err := sRec.UnmarshalFrom(f)
			_ = f.Close()
			if err == nil {
				return time.Unix(0, sRec.TimestampNano).UTC(), true
			}
		}
	}
	return time.Time{}, false
}

// LastAvailableTime returns the timestamp of the latest recorded packet across all buckets.
func (s *HourlyStore) LastAvailableTime() (time.Time, bool) {
	buckets, err := s.ListBuckets()
	if err != nil || len(buckets) == 0 {
		return time.Time{}, false
	}

	for i := len(buckets) - 1; i >= 0; i-- {
		b := buckets[i]
		if !b.LastPacketUTC.IsZero() {
			return b.LastPacketUTC, true
		}
	}
	return time.Time{}, false
}

// SyslogRangeReader reads Syslog records across hourly buckets chronologically.
type SyslogRangeReader struct {
	store       *HourlyStore
	fromUTC     time.Time
	toUTC       time.Time
	bucketPaths []string
	currentIdx  int
	file        *os.File
}

// NewSyslogRangeReader creates a new SyslogRangeReader for records within [fromTimeUTC, toTimeUTC].
func (s *HourlyStore) NewSyslogRangeReader(fromTimeUTC, toTimeUTC time.Time) (*SyslogRangeReader, error) {
	s.mu.Lock()
	if s.active != nil {
		_ = s.closeActiveHour()
	}
	s.mu.Unlock()

	buckets, err := s.ListBuckets()
	if err != nil {
		return nil, err
	}

	var matchingPaths []string
	fromTrunc := fromTimeUTC.Truncate(time.Hour)
	toTrunc := toTimeUTC.Truncate(time.Hour)

	for _, b := range buckets {
		if b.StartTimeUTC.Before(fromTrunc) {
			continue
		}
		if !toTimeUTC.IsZero() && b.StartTimeUTC.After(toTrunc) {
			continue
		}
		matchingPaths = append(matchingPaths, filepath.Join(s.cfg.SpoolDir, b.HourPath))
	}

	return &SyslogRangeReader{
		store:       s,
		fromUTC:     fromTimeUTC,
		toUTC:       toTimeUTC,
		bucketPaths: matchingPaths,
		currentIdx:  0,
	}, nil
}

// Close closes any open file in the SyslogRangeReader.
func (r *SyslogRangeReader) Close() error {
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}
	return nil
}

// Next reads the next SyslogRecord within the requested time range.
func (r *SyslogRangeReader) Next() (*SyslogRecord, error) {
	for {
		if r.file == nil {
			if r.currentIdx >= len(r.bucketPaths) {
				return nil, io.EOF
			}
			path := filepath.Join(r.bucketPaths[r.currentIdx], SyslogFileName)
			f, err := os.Open(path)
			if err != nil {
				r.currentIdx++
				continue
			}
			r.file = f
		}

		var rec SyslogRecord
		err := rec.UnmarshalFrom(r.file)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				_ = r.file.Close()
				r.file = nil
				r.currentIdx++
				continue
			}
			return nil, err
		}

		recTime := time.Unix(0, rec.TimestampNano).UTC()
		if recTime.Before(r.fromUTC) {
			continue
		}
		if !r.toUTC.IsZero() && recTime.After(r.toUTC) {
			return nil, io.EOF
		}

		return &rec, nil
	}
}

// NextSyslog is an alias to Next().
func (r *SyslogRangeReader) NextSyslog() (*SyslogRecord, error) {
	return r.Next()
}
