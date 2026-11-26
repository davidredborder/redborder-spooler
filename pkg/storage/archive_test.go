package storage_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

func TestExportAndImportArchive(t *testing.T) {
	srcDir := t.TempDir()
	cfg := &config.Config{SpoolDir: srcDir}
	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		t.Fatalf("NewHourlyStore failed: %v", err)
	}

	t1 := time.Date(2026, 11, 24, 10, 15, 0, 0, time.UTC)
	t2 := time.Date(2026, 11, 24, 11, 20, 0, 0, time.UTC)

	// Write UDP records in two different hours
	if err := store.WriteUDP(t1, net.ParseIP("192.168.1.1"), net.ParseIP("10.0.0.1"), 1234, 2055, []byte("pkt-hour-10")); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteSyslog(t1, net.ParseIP("192.168.1.1"), "udp", []byte("<14>syslog archive test")); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteUDP(t2, net.ParseIP("192.168.1.2"), net.ParseIP("10.0.0.1"), 1234, 2055, []byte("pkt-hour-11")); err != nil {
		t.Fatal(err)
	}
	_ = store.CloseHour()
	_ = store.Close()

	// 1. Export archive containing both hours
	var archiveBuf bytes.Buffer
	fromTime := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	toTime := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	stats, err := storage.ExportArchive(srcDir, fromTime, toTime, &archiveBuf)
	if err != nil {
		t.Fatalf("ExportArchive failed: %v", err)
	}
	if stats.BucketsExported != 2 {
		t.Errorf("expected 2 buckets exported, got %d", stats.BucketsExported)
	}
	if stats.FilesExported == 0 {
		t.Error("expected >0 files exported")
	}

	// 2. Import into a clean target directory
	targetDir := t.TempDir()
	importStats, err := storage.ImportArchive(targetDir, bytes.NewReader(archiveBuf.Bytes()), false)
	if err != nil {
		t.Fatalf("ImportArchive failed: %v", err)
	}
	if importStats.BucketsImported != 2 {
		t.Errorf("expected 2 buckets imported, got %d", importStats.BucketsImported)
	}

	// Verify imported store has identical buckets
	targetCfg := &config.Config{SpoolDir: targetDir}
	targetStore, err := storage.NewHourlyStore(targetCfg)
	if err != nil {
		t.Fatalf("target NewHourlyStore failed: %v", err)
	}
	defer targetStore.Close()

	buckets, err := targetStore.ListBuckets()
	if err != nil {
		t.Fatalf("target ListBuckets failed: %v", err)
	}
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets in imported store, got %d", len(buckets))
	}
	importedSyslog := filepath.Join(targetDir, "2026-11-24", "10", storage.SyslogFileName)
	if _, err := os.Stat(importedSyslog); err != nil {
		t.Fatalf("expected imported syslog.wal at %s: %v", importedSyslog, err)
	}
	if buckets[0].HourPath != "2026-11-24/10" || buckets[1].HourPath != "2026-11-24/11" {
		t.Errorf("bucket paths mismatch: %v, %v", buckets[0].HourPath, buckets[1].HourPath)
	}

	// Verify reading imported packets
	rr, err := targetStore.OpenRange(fromTime, toTime)
	if err != nil {
		t.Fatalf("OpenRange on target failed: %v", err)
	}
	defer rr.Close()

	rec1, err := rr.NextUDP()
	if err != nil || string(rec1.Payload) != "pkt-hour-10" {
		t.Fatalf("rec1 mismatch: %v (err=%v)", rec1, err)
	}
	rec2, err := rr.NextUDP()
	if err != nil || string(rec2.Payload) != "pkt-hour-11" {
		t.Fatalf("rec2 mismatch: %v (err=%v)", rec2, err)
	}
}

type tarEntry struct {
	name     string
	typeFlag byte
	linkName string
	content  []byte
}

func createTarGz(t *testing.T, entries []tarEntry) []byte {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	for _, e := range entries {
		hdr := &tar.Header{
			Name:     e.name,
			Mode:     0644,
			Size:     int64(len(e.content)),
			Typeflag: e.typeFlag,
			Linkname: e.linkName,
		}
		if hdr.Typeflag == 0 {
			hdr.Typeflag = tar.TypeReg
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}
		if len(e.content) > 0 {
			if _, err := tw.Write(e.content); err != nil {
				t.Fatalf("Write content failed: %v", err)
			}
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("tw.Close failed: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gw.Close failed: %v", err)
	}
	return buf.Bytes()
}

func TestImportArchiveZipSlipProtection(t *testing.T) {
	testCases := []struct {
		name    string
		entries []tarEntry
	}{
		{
			name: "relative traversal ../",
			entries: []tarEntry{
				{name: "../traversal.wal", content: []byte("malicious")},
			},
		},
		{
			name: "traversal inside bucket path",
			entries: []tarEntry{
				{name: "2026-11-24/10/../../escape.wal", content: []byte("malicious")},
			},
		},
		{
			name: "absolute path /tmp",
			entries: []tarEntry{
				{name: "/tmp/evil.wal", content: []byte("malicious")},
			},
		},
		{
			name: "symlink rejection",
			entries: []tarEntry{
				{name: "2026-11-24/10/symlink.wal", typeFlag: tar.TypeSymlink, linkName: "/etc/passwd"},
			},
		},
		{
			name: "invalid bucket format",
			entries: []tarEntry{
				{name: "invalid-bucket-format/wal.data", content: []byte("malicious")},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parentDir := t.TempDir()
			targetDir := filepath.Join(parentDir, "target")
			if err := os.MkdirAll(targetDir, 0755); err != nil {
				t.Fatal(err)
			}

			archiveData := createTarGz(t, tc.entries)
			_, err := storage.ImportArchive(targetDir, bytes.NewReader(archiveData), false)
			if err == nil {
				t.Errorf("expected error importing malicious archive %q, got nil", tc.name)
			}

			// Ensure nothing escaped outside targetDir
			entries, err := os.ReadDir(parentDir)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				if e.Name() != "target" {
					t.Errorf("file escaped into parentDir: %s", e.Name())
				}
			}
		})
	}

	t.Run("corrupt stream", func(t *testing.T) {
		targetDir := t.TempDir()
		badData := []byte("not-a-valid-tar-stream")
		_, err := storage.ImportArchive(targetDir, bytes.NewReader(badData), false)
		if err == nil {
			t.Error("expected error on corrupt archive import")
		}
	})
}
