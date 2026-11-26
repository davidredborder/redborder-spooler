package storage

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

type bucketInfo struct {
	path      string    // absolute path to bucket dir
	relPath   string    // "YYYY-MM-DD/HH"
	startTime time.Time
	sizeBytes int64
}

// Prune enforces retention policies (max age, max storage MB, and min free disk pct).
// It returns a slice of relative paths of deleted buckets.
// The current UTC hour is NEVER deleted.
func (s *HourlyStore) Prune(now time.Time) ([]string, error) {
	s.mu.Lock()
	if s.active != nil {
		_ = s.closeActiveHour()
	}
	s.mu.Unlock()

	currentHourKey := now.UTC().Format("2006-01-02/15")

	buckets, err := s.scanBuckets()
	if err != nil {
		return nil, err
	}

	var pruned []string
	deletedSet := make(map[string]struct{})

	deleteBucket := func(b *bucketInfo) error {
		if b.relPath == currentHourKey {
			return nil // Never delete current active hour
		}
		if _, already := deletedSet[b.relPath]; already {
			return nil
		}
		if err := os.RemoveAll(b.path); err != nil {
			return fmt.Errorf("failed to remove bucket %s: %w", b.path, err)
		}
		deletedSet[b.relPath] = struct{}{}
		pruned = append(pruned, b.relPath)

		// Try removing parent day directory if now empty
		dayDir := filepath.Dir(b.path)
		entries, err := os.ReadDir(dayDir)
		if err == nil && len(entries) == 0 {
			_ = os.Remove(dayDir)
		}
		return nil
	}

	// 1. Check MaxAge
	if s.cfg.Retention.MaxAge > 0 {
		cutoff := now.UTC().Add(-s.cfg.Retention.MaxAge)
		for i := range buckets {
			b := &buckets[i]
			if b.startTime.Before(cutoff) && b.relPath != currentHourKey {
				if err := deleteBucket(b); err != nil {
					return pruned, err
				}
			}
		}
	}

	// Re-calculate remaining buckets and total size
	remaining := make([]bucketInfo, 0, len(buckets))
	var totalBytes int64
	for _, b := range buckets {
		if _, deleted := deletedSet[b.relPath]; !deleted {
			remaining = append(remaining, b)
			totalBytes += b.sizeBytes
		}
	}

	// 2. Check MaxStorageMB
	if s.cfg.Retention.MaxStorageMB > 0 {
		maxBytes := s.cfg.Retention.MaxStorageMB * 1024 * 1024
		for _, b := range remaining {
			if totalBytes <= maxBytes {
				break
			}
			if b.relPath == currentHourKey {
				continue
			}
			if err := deleteBucket(&b); err != nil {
				return pruned, err
			}
			totalBytes -= b.sizeBytes
		}
	}

	// 3. Check MinFreeDiskPct
	if s.cfg.Retention.MinFreeDiskPct > 0 {
		for _, b := range remaining {
			if _, deleted := deletedSet[b.relPath]; deleted || b.relPath == currentHourKey {
				continue
			}

			freePct, err := s.getFreeDiskPct()
			if err != nil {
				break
			}
			if freePct >= s.cfg.Retention.MinFreeDiskPct {
				break
			}

			if err := deleteBucket(&b); err != nil {
				return pruned, err
			}
		}
	}

	return pruned, nil
}

func (s *HourlyStore) getFreeDiskPct() (float64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(s.cfg.SpoolDir, &stat); err != nil {
		return 0, err
	}
	if stat.Blocks == 0 {
		return 100.0, nil
	}
	return float64(stat.Bavail) / float64(stat.Blocks) * 100.0, nil
}

func (s *HourlyStore) scanBuckets() ([]bucketInfo, error) {
	var buckets []bucketInfo

	dayDirs, err := os.ReadDir(s.cfg.SpoolDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	for _, dayEntry := range dayDirs {
		if !dayEntry.IsDir() {
			continue
		}
		dayName := dayEntry.Name()
		dayPath := filepath.Join(s.cfg.SpoolDir, dayName)

		hourDirs, err := os.ReadDir(dayPath)
		if err != nil {
			continue
		}

		for _, hourEntry := range hourDirs {
			if !hourEntry.IsDir() {
				continue
			}
			hourName := hourEntry.Name()
			hourDir := filepath.Join(dayPath, hourName)
			relPath := fmt.Sprintf("%s/%s", dayName, hourName)

			parsedTime, err := time.Parse("2006-01-02/15", relPath)
			if err != nil {
				continue
			}

			var size int64
			_ = filepath.WalkDir(hourDir, func(path string, d fs.DirEntry, err error) error {
				if err == nil && !d.IsDir() {
					info, err := d.Info()
					if err == nil {
						size += info.Size()
					}
				}
				return nil
			})

			buckets = append(buckets, bucketInfo{
				path:      hourDir,
				relPath:   relPath,
				startTime: parsedTime.UTC(),
				sizeBytes: size,
			})
		}
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].startTime.Before(buckets[j].startTime)
	})

	return buckets, nil
}
