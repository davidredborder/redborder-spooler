package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// HTTPConfig defines the configuration for the HTTP spooler and proxy.
type HTTPConfig struct {
	Enabled     bool   `yaml:"enabled" json:"enabled"`
	ListenAddr  string `yaml:"listen_addr" json:"listen_addr"`
	UpstreamURL string `yaml:"upstream_url" json:"upstream_url"`
}

// TCPSyslogConfig defines the configuration for the dedicated TCP Syslog spooler.
type TCPSyslogConfig struct {
	Enabled      bool   `yaml:"enabled" json:"enabled"`
	ListenAddr   string `yaml:"listen_addr" json:"listen_addr"`
	UpstreamAddr string `yaml:"upstream_addr" json:"upstream_addr"`
}

// CaptureConfig defines the capture interface and filter configuration.
type CaptureConfig struct {
	Interfaces         []string        `yaml:"interfaces" json:"interfaces"`
	UDPPorts           []int           `yaml:"udp_ports" json:"udp_ports"`
	Mode               string          `yaml:"mode" json:"mode"` // "af_packet" or "socket"
	LVSDRDedupWindowMs int             `yaml:"lvs_dr_dedup_window_ms" json:"lvs_dr_dedup_window_ms"`
	HTTP               HTTPConfig      `yaml:"http" json:"http"`
	SyslogTCP          TCPSyslogConfig `yaml:"syslog_tcp" json:"syslog_tcp"`
}

// RetentionConfig defines disk and age limits for stored data.
type RetentionConfig struct {
	MaxAge         time.Duration `yaml:"max_age" json:"max_age"`
	MaxStorageMB   int64         `yaml:"max_storage_mb" json:"max_storage_mb"`
	MinFreeDiskPct float64       `yaml:"min_free_disk_pct" json:"min_free_disk_pct"`
	CheckInterval  time.Duration `yaml:"check_interval" json:"check_interval"`
}

// ReplayConfig defines default parameters for the replay engine.
type ReplayConfig struct {
	TargetHost      string  `yaml:"target_host" json:"target_host"`
	DefaultSpeed    float64 `yaml:"default_speed" json:"default_speed"`
	UseRBRPEnvelope bool    `yaml:"use_rbrp_envelope" json:"use_rbrp_envelope"`
}

// Config is the main configuration object.
type Config struct {
	SpoolDir         string          `yaml:"spool_dir" json:"spool_dir"`
	ControlSocket    string          `yaml:"control_socket" json:"control_socket"`
	MetaSyncInterval time.Duration   `yaml:"meta_sync_interval" json:"meta_sync_interval"`
	Capture          CaptureConfig   `yaml:"capture" json:"capture"`
	Retention        RetentionConfig `yaml:"retention" json:"retention"`
	Replay           ReplayConfig    `yaml:"replay" json:"replay"`
}

// DefaultConfig returns a Config initialized with production defaults.
func DefaultConfig() *Config {
	return &Config{
		SpoolDir:         "/var/spool/redborder-spooler",
		ControlSocket:    "/run/redborder-spooler/control.sock",
		MetaSyncInterval: 5 * time.Second,
		Capture: CaptureConfig{
			Interfaces:         []string{"any"},
			UDPPorts:           []int{2055, 6343, 514},
			Mode:               "af_packet",
			LVSDRDedupWindowMs: 20,
			HTTP: HTTPConfig{
				Enabled:    false,
				ListenAddr: ":18080",
			},
			SyslogTCP: TCPSyslogConfig{
				Enabled:      false,
				ListenAddr:   ":10514",
				UpstreamAddr: "",
			},
		},
		Retention: RetentionConfig{
			MaxAge:         72 * time.Hour,
			MaxStorageMB:   20480, // 20 GB
			MinFreeDiskPct: 15.0,
			CheckInterval:  5 * time.Minute,
		},
		Replay: ReplayConfig{
			TargetHost:      "127.0.0.1",
			DefaultSpeed:    10.0,
			UseRBRPEnvelope: true,
		},
	}
}

// rawRetentionConfig aids in parsing string durations like "72h" or "5m".
type rawRetentionConfig struct {
	MaxAge         interface{} `yaml:"max_age" json:"max_age"`
	MaxStorageMB   *int64      `yaml:"max_storage_mb" json:"max_storage_mb"`
	MinFreeDiskPct *float64    `yaml:"min_free_disk_pct" json:"min_free_disk_pct"`
	CheckInterval  interface{} `yaml:"check_interval" json:"check_interval"`
}

type rawConfig struct {
	SpoolDir         *string             `yaml:"spool_dir" json:"spool_dir"`
	ControlSocket    *string             `yaml:"control_socket" json:"control_socket"`
	MetaSyncInterval interface{}         `yaml:"meta_sync_interval" json:"meta_sync_interval"`
	Capture          *CaptureConfig      `yaml:"capture" json:"capture"`
	Retention        *rawRetentionConfig `yaml:"retention" json:"retention"`
	Replay           *ReplayConfig       `yaml:"replay" json:"replay"`
}

func parseDuration(v interface{}, defaultVal time.Duration) (time.Duration, error) {
	if v == nil {
		return defaultVal, nil
	}
	switch val := v.(type) {
	case string:
		return time.ParseDuration(val)
	case int:
		return time.Duration(val) * time.Second, nil
	case int64:
		return time.Duration(val) * time.Second, nil
	case float64:
		return time.Duration(val) * time.Second, nil
	default:
		return defaultVal, fmt.Errorf("invalid duration format: %v", v)
	}
}

// LoadConfig reads and parses configuration from a file (YAML or JSON).
// If path is empty, DefaultConfig() is returned.
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var raw rawConfig
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".json" {
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("failed to parse json config: %w", err)
		}
	} else {
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("failed to parse yaml config: %w", err)
		}
	}

	if raw.SpoolDir != nil {
		cfg.SpoolDir = *raw.SpoolDir
	}
	if raw.ControlSocket != nil {
		cfg.ControlSocket = *raw.ControlSocket
	}
	if raw.MetaSyncInterval != nil {
		d, err := parseDuration(raw.MetaSyncInterval, cfg.MetaSyncInterval)
		if err != nil {
			return nil, fmt.Errorf("invalid meta_sync_interval: %w", err)
		}
		cfg.MetaSyncInterval = d
	}
	if raw.Capture != nil {
		if len(raw.Capture.Interfaces) > 0 {
			cfg.Capture.Interfaces = raw.Capture.Interfaces
		}
		if len(raw.Capture.UDPPorts) > 0 {
			cfg.Capture.UDPPorts = raw.Capture.UDPPorts
		}
		if raw.Capture.Mode != "" {
			cfg.Capture.Mode = raw.Capture.Mode
		}
		if raw.Capture.LVSDRDedupWindowMs > 0 {
			cfg.Capture.LVSDRDedupWindowMs = raw.Capture.LVSDRDedupWindowMs
		}
		cfg.Capture.HTTP = raw.Capture.HTTP
		if raw.Capture.SyslogTCP.ListenAddr != "" {
			cfg.Capture.SyslogTCP.ListenAddr = raw.Capture.SyslogTCP.ListenAddr
		}
		cfg.Capture.SyslogTCP.Enabled = raw.Capture.SyslogTCP.Enabled
		if raw.Capture.SyslogTCP.UpstreamAddr != "" {
			cfg.Capture.SyslogTCP.UpstreamAddr = raw.Capture.SyslogTCP.UpstreamAddr
		}
	}
	if raw.Retention != nil {
		if raw.Retention.MaxAge != nil {
			d, err := parseDuration(raw.Retention.MaxAge, cfg.Retention.MaxAge)
			if err != nil {
				return nil, fmt.Errorf("invalid retention.max_age: %w", err)
			}
			cfg.Retention.MaxAge = d
		}
		if raw.Retention.MaxStorageMB != nil {
			cfg.Retention.MaxStorageMB = *raw.Retention.MaxStorageMB
		}
		if raw.Retention.MinFreeDiskPct != nil {
			cfg.Retention.MinFreeDiskPct = *raw.Retention.MinFreeDiskPct
		}
		if raw.Retention.CheckInterval != nil {
			d, err := parseDuration(raw.Retention.CheckInterval, cfg.Retention.CheckInterval)
			if err != nil {
				return nil, fmt.Errorf("invalid retention.check_interval: %w", err)
			}
			cfg.Retention.CheckInterval = d
		}
	}
	if raw.Replay != nil {
		cfg.Replay = *raw.Replay
	}

	return cfg, nil
}
