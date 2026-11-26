package storage

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
)

var (
	hourPathRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}/\d{2}$`)
	filePathRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}/\d{2}/[a-z0-9_.]+$`)
)

const (
	MaxArchiveEntryBytes = 2 * 1024 * 1024 * 1024 // 2 GB max per entry (decompression bomb protection)
)

// ArchiveManifest contains metadata stored at the root of an exported tar.gz.
type ArchiveManifest struct {
	Version       string    `json:"version"`
	ExportTimeUTC time.Time `json:"export_time_utc"`
	FromTimeUTC   time.Time `json:"from_time_utc"`
	ToTimeUTC     time.Time `json:"to_time_utc"`
	BucketsCount  int       `json:"buckets_count"`
	FilesCount    int       `json:"files_count"`
	TotalBytes    uint64    `json:"total_bytes"`
}

// ArchiveStats summarizes an export operation.
type ArchiveStats struct {
	BucketsExported int
	FilesExported   int
	TotalBytes      uint64
}

// ImportStats summarizes an import operation.
type ImportStats struct {
	BucketsImported int
	FilesImported   int
	TotalBytes      uint64
}

// ExportArchive writes all hourly directories within [fromUTC, toUTC] into a compressed tar.gz stream.
func ExportArchive(spoolDir string, fromUTC, toUTC time.Time, out io.Writer) (*ArchiveStats, error) {
	if fromUTC.After(toUTC) {
		return nil, fmt.Errorf("from time %v cannot be after to time %v", fromUTC, toUTC)
	}

	gw := gzip.NewWriter(out)
	tw := tar.NewWriter(gw)

	store, err := NewHourlyStore(&config.Config{SpoolDir: spoolDir})
	if err != nil {
		_ = tw.Close()
		_ = gw.Close()
		return nil, fmt.Errorf("failed to open spool store: %w", err)
	}
	defer store.Close()

	buckets, err := store.ListBuckets()
	if err != nil {
		_ = tw.Close()
		_ = gw.Close()
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	fromTrunc := fromUTC.Truncate(time.Hour)
	toTrunc := toUTC.Truncate(time.Hour)

	var matchingBuckets []BucketMeta
	for _, b := range buckets {
		if !hourPathRegex.MatchString(b.HourPath) {
			continue
		}
		if !b.StartTimeUTC.Before(fromTrunc) && !b.StartTimeUTC.After(toTrunc) {
			matchingBuckets = append(matchingBuckets, b)
		}
	}

	stats := &ArchiveStats{}
	bucketFiles := []string{UDPWalName, UDPIdxName, HTTPWalName, SyslogFileName, MetaFileName, TemplateSnapName}

	for _, b := range matchingBuckets {
		bucketDir := filepath.Join(spoolDir, b.HourPath)
		exportedInBucket := 0

		for _, fname := range bucketFiles {
			fpath := filepath.Join(bucketDir, fname)
			fi, err := os.Stat(fpath)
			if err != nil || fi.IsDir() {
				continue
			}
			if fi.Size() > MaxArchiveEntryBytes {
				_ = tw.Close()
				_ = gw.Close()
				return nil, fmt.Errorf("file %s exceeds maximum allowed archive entry size (%d bytes)", fpath, MaxArchiveEntryBytes)
			}

			hdr, err := tar.FileInfoHeader(fi, "")
			if err != nil {
				_ = tw.Close()
				_ = gw.Close()
				return nil, fmt.Errorf("failed to create tar header for %s: %w", fpath, err)
			}
			// Store path relative to spool (e.g. "2026-11-24/10/udp.wal")
			hdr.Name = filepath.ToSlash(filepath.Join(b.HourPath, fname))

			if err := tw.WriteHeader(hdr); err != nil {
				_ = tw.Close()
				_ = gw.Close()
				return nil, fmt.Errorf("failed to write tar header: %w", err)
			}

			f, err := os.Open(fpath)
			if err != nil {
				_ = tw.Close()
				_ = gw.Close()
				return nil, fmt.Errorf("failed to read file %s: %w", fpath, err)
			}
			n, err := io.Copy(tw, f)
			_ = f.Close()
			if err != nil {
				_ = tw.Close()
				_ = gw.Close()
				return nil, fmt.Errorf("failed to write tar entry %s: %w", hdr.Name, err)
			}

			stats.FilesExported++
			stats.TotalBytes += uint64(n)
			exportedInBucket++
		}

		if exportedInBucket > 0 {
			stats.BucketsExported++
		}
	}

	// Write manifest.json
	manifest := ArchiveManifest{
		Version:       "1.0.0",
		ExportTimeUTC: time.Now().UTC(),
		FromTimeUTC:   fromUTC,
		ToTimeUTC:     toUTC,
		BucketsCount:  stats.BucketsExported,
		FilesCount:    stats.FilesExported,
		TotalBytes:    stats.TotalBytes,
	}

	mBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err == nil {
		hdr := &tar.Header{
			Name:    "manifest.json",
			Mode:    0644,
			Size:    int64(len(mBytes)),
			ModTime: time.Now().UTC(),
		}
		if err := tw.WriteHeader(hdr); err == nil {
			_, _ = tw.Write(mBytes)
		}
	}

	if err := tw.Close(); err != nil {
		_ = gw.Close()
		return nil, fmt.Errorf("failed to flush tar archive: %w", err)
	}
	if err := gw.Close(); err != nil {
		return nil, fmt.Errorf("failed to flush gzip archive: %w", err)
	}

	return stats, nil
}

// ImportArchive reads an exported tar.gz stream and safely unpacks buckets into spoolDir.
func ImportArchive(spoolDir string, in io.Reader, overwrite bool) (*ImportStats, error) {
	cleanSpool, err := filepath.Abs(spoolDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve spool directory: %w", err)
	}

	if err := os.MkdirAll(cleanSpool, 0755); err != nil {
		return nil, fmt.Errorf("failed to create spool directory: %w", err)
	}

	gr, err := gzip.NewReader(in)
	if err != nil {
		return nil, fmt.Errorf("invalid or corrupt gzip archive: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	stats := &ImportStats{}
	bucketSet := make(map[string]struct{})
	var createdFiles []string
	cleanupOnError := func() {
		for _, f := range createdFiles {
			_ = os.Remove(f)
		}
	}

	allowedFiles := map[string]struct{}{
		UDPWalName:       {},
		UDPIdxName:       {},
		HTTPWalName:      {},
		SyslogFileName:   {},
		MetaFileName:     {},
		TemplateSnapName: {},
	}

	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			cleanupOnError()
			return nil, fmt.Errorf("error reading tar archive: %w", err)
		}

		cleanName := filepath.Clean(hdr.Name)
		if cleanName == "manifest.json" {
			continue
		}
		if hdr.Typeflag == tar.TypeDir {
			continue
		}

		targetPath := filepath.Join(cleanSpool, cleanName)
		// Security 1: Path containment & traversal check
		if !strings.HasPrefix(targetPath, cleanSpool+string(filepath.Separator)) || strings.HasPrefix(cleanName, "..") || filepath.IsAbs(hdr.Name) {
			cleanupOnError()
			return nil, fmt.Errorf("archive entry escapes spool directory: %s", hdr.Name)
		}

		// Security 2: Only unpack regular files (no symlinks, device files, hard links)
		if hdr.Typeflag != tar.TypeReg {
			cleanupOnError()
			return nil, fmt.Errorf("refusing non-regular file in archive: %s (type=%c)", hdr.Name, hdr.Typeflag)
		}

		// Security 3: Enforce strict path structure (YYYY-MM-DD/HH/<allowed_file>)
		if !filePathRegex.MatchString(cleanName) {
			cleanupOnError()
			return nil, fmt.Errorf("invalid archive entry path format: %s", hdr.Name)
		}

		fname := filepath.Base(cleanName)
		if _, ok := allowedFiles[fname]; !ok {
			cleanupOnError()
			return nil, fmt.Errorf("unexpected file in archive: %s", fname)
		}

		fileExisted := false
		// Security 4: Check if destination is a symlink
		if fi, err := os.Lstat(targetPath); err == nil {
			if fi.Mode()&os.ModeSymlink != 0 {
				cleanupOnError()
				return nil, fmt.Errorf("refusing to write to symlink %s", targetPath)
			}
			if !overwrite {
				continue // Skip existing file unless overwrite requested
			}
			fileExisted = true
		}

		bucketDir := filepath.Dir(targetPath)
		// Security 5: Check if parent dir is a symlink
		if fi, err := os.Lstat(bucketDir); err == nil && fi.Mode()&os.ModeSymlink != 0 {
			cleanupOnError()
			return nil, fmt.Errorf("refusing to write into symlinked directory %s", bucketDir)
		}
		if err := os.MkdirAll(bucketDir, 0755); err != nil {
			cleanupOnError()
			return nil, fmt.Errorf("failed to create bucket directory %s: %w", bucketDir, err)
		}

		if hdr.Size > MaxArchiveEntryBytes {
			cleanupOnError()
			return nil, fmt.Errorf("archive entry %s exceeds maximum allowed size (%d bytes)", hdr.Name, MaxArchiveEntryBytes)
		}

		outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			cleanupOnError()
			return nil, fmt.Errorf("failed to open destination file %s: %w", targetPath, err)
		}

		// Security 6: Limit decompressed size to prevent zip bombs
		n, err := io.Copy(outFile, io.LimitReader(tr, MaxArchiveEntryBytes+1))
		_ = outFile.Close()
		if err != nil {
			cleanupOnError()
			return nil, fmt.Errorf("failed to extract file %s: %w", targetPath, err)
		}
		if n > MaxArchiveEntryBytes {
			_ = os.Remove(targetPath)
			cleanupOnError()
			return nil, fmt.Errorf("archive entry %s decompressed size exceeded limit (%d bytes)", hdr.Name, MaxArchiveEntryBytes)
		}

		if !fileExisted {
			createdFiles = append(createdFiles, targetPath)
		}
		relBucket, _ := filepath.Rel(cleanSpool, bucketDir)
		bucketSet[relBucket] = struct{}{}
		stats.FilesImported++
		stats.TotalBytes += uint64(n)
	}

	// Drain remaining gzip stream to validate trailer/CRC
	_, _ = io.Copy(io.Discard, gr)
	if err := gr.Close(); err != nil {
		cleanupOnError()
		return nil, fmt.Errorf("corrupt gzip archive trailer: %w", err)
	}

	stats.BucketsImported = len(bucketSet)
	return stats, nil
}
