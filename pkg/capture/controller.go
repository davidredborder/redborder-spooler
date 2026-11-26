package capture

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

// ListenerStatus represents the operational status of an individual capture listener.
type ListenerStatus struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Enabled     bool   `json:"enabled"`
	ListenAddr  string `json:"listen_addr"`
	Description string `json:"description"`
}

// TrafficListener provides an interface for the underlying UDP traffic capture (AF_PACKET or socket).
type TrafficListener interface {
	SetTrafficPaused(paused bool)
	IsTrafficPaused() bool
	SetSyslogUDPPaused(paused bool)
	IsSyslogUDPPaused() bool
	Stop()
}

// Controller coordinates and controls the runtime lifecycle of all capture components.
type Controller struct {
	cfg       *config.Config
	store     *storage.HourlyStore
	traffic   TrafficListener
	http      *HTTPSpooler
	syslogTCP *TCPSyslogSpooler
	mu        sync.Mutex
	startTime time.Time
}

// NewController creates a new Controller.
func NewController(cfg *config.Config, store *storage.HourlyStore, traffic TrafficListener, httpSpooler *HTTPSpooler, syslogTCP *TCPSyslogSpooler) *Controller {
	return &Controller{
		cfg:       cfg,
		store:     store,
		traffic:   traffic,
		http:      httpSpooler,
		syslogTCP: syslogTCP,
		startTime: time.Now(),
	}
}

// StartInitial starts any configured listeners that have enabled: true.
func (c *Controller) StartInitial() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []string
	if c.cfg.Capture.HTTP.Enabled && c.http != nil {
		if err := c.http.Start(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to start http spooler: %v", err))
		}
	}
	if c.cfg.Capture.SyslogTCP.Enabled && c.syslogTCP != nil {
		if err := c.syslogTCP.Start(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to start syslog tcp spooler: %v", err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// Enable activates the specified component: "syslog_tcp", "http", "traffic", "syslog_udp", or "all".
func (c *Controller) Enable(target string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	target = strings.ToLower(strings.TrimSpace(target))
	switch target {
	case "syslog_tcp", "tcp_syslog":
		return c.enableSyslogTCP()
	case "http":
		return c.enableHTTP()
	case "traffic", "udp":
		if c.traffic != nil {
			c.traffic.SetTrafficPaused(false)
		}
		return nil
	case "syslog_udp", "udp_syslog":
		if c.traffic != nil {
			c.traffic.SetSyslogUDPPaused(false)
		}
		return nil
	case "all":
		var errs []string
		if c.traffic != nil {
			c.traffic.SetTrafficPaused(false)
			c.traffic.SetSyslogUDPPaused(false)
		}
		if c.cfg.Capture.HTTP.ListenAddr != "" {
			if err := c.enableHTTP(); err != nil {
				errs = append(errs, err.Error())
			}
		}
		if c.cfg.Capture.SyslogTCP.ListenAddr != "" {
			if err := c.enableSyslogTCP(); err != nil {
				errs = append(errs, err.Error())
			}
		}
		if len(errs) > 0 {
			return errors.New(strings.Join(errs, "; "))
		}
		return nil
	default:
		return fmt.Errorf("unknown target '%s' (expected: 'traffic', 'syslog_tcp', 'syslog_udp', 'http', or 'all')", target)
	}
}

// Disable deactivates the specified component: "syslog_tcp", "http", "traffic", "syslog_udp", or "all".
func (c *Controller) Disable(target string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	target = strings.ToLower(strings.TrimSpace(target))
	switch target {
	case "syslog_tcp", "tcp_syslog":
		return c.disableSyslogTCP()
	case "http":
		return c.disableHTTP()
	case "traffic", "udp":
		if c.traffic != nil {
			c.traffic.SetTrafficPaused(true)
		}
		return nil
	case "syslog_udp", "udp_syslog":
		if c.traffic != nil {
			c.traffic.SetSyslogUDPPaused(true)
		}
		return nil
	case "all":
		var errs []string
		if c.traffic != nil {
			c.traffic.SetTrafficPaused(true)
			c.traffic.SetSyslogUDPPaused(true)
		}
		if err := c.disableHTTP(); err != nil {
			errs = append(errs, err.Error())
		}
		if err := c.disableSyslogTCP(); err != nil {
			errs = append(errs, err.Error())
		}
		if len(errs) > 0 {
			return errors.New(strings.Join(errs, "; "))
		}
		return nil
	default:
		return fmt.Errorf("unknown target '%s' (expected: 'traffic', 'syslog_tcp', 'syslog_udp', 'http', or 'all')", target)
	}
}

func (c *Controller) enableSyslogTCP() error {
	if c.cfg.Capture.SyslogTCP.ListenAddr == "" {
		return errors.New("syslog_tcp listen_addr is not configured")
	}
	if c.syslogTCP == nil {
		c.syslogTCP = NewTCPSyslogSpooler(c.cfg.Capture.SyslogTCP, c.store)
	}
	return c.syslogTCP.Start()
}

func (c *Controller) disableSyslogTCP() error {
	if c.syslogTCP != nil && c.syslogTCP.IsEnabled() {
		return c.syslogTCP.Stop()
	}
	return nil
}

func (c *Controller) enableHTTP() error {
	if c.cfg.Capture.HTTP.ListenAddr == "" {
		return errors.New("http listen_addr is not configured")
	}
	if c.http == nil {
		c.http = NewHTTPSpooler(c.cfg.Capture.HTTP, c.store)
	}
	return c.http.Start()
}

func (c *Controller) disableHTTP() error {
	if c.http != nil && c.http.IsEnabled() {
		return c.http.Stop()
	}
	return nil
}

// Status returns the current status of all listeners.
func (c *Controller) Status() []ListenerStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	var statuses []ListenerStatus

	// 1. Traffic (Flow UDP)
	trafficEnabled := true
	if c.traffic != nil && c.traffic.IsTrafficPaused() {
		trafficEnabled = false
	}
	var flowPorts []string
	for _, p := range c.cfg.Capture.UDPPorts {
		if p != 514 {
			flowPorts = append(flowPorts, fmt.Sprintf("%d", p))
		}
	}
	statuses = append(statuses, ListenerStatus{
		Name:        "traffic",
		Type:        "flow_udp",
		Enabled:     trafficEnabled,
		ListenAddr:  fmt.Sprintf("%s (ports %s)", c.cfg.Capture.Mode, strings.Join(flowPorts, ",")),
		Description: "NetFlow / IPFIX / sFlow capture",
	})

	// 2. Syslog UDP
	syslogUDPEnabled := true
	if c.traffic != nil && c.traffic.IsSyslogUDPPaused() {
		syslogUDPEnabled = false
	}
	has514 := false
	for _, p := range c.cfg.Capture.UDPPorts {
		if p == 514 {
			has514 = true
			break
		}
	}
	if !has514 {
		syslogUDPEnabled = false
	}
	statuses = append(statuses, ListenerStatus{
		Name:        "syslog_udp",
		Type:        "syslog_udp",
		Enabled:     syslogUDPEnabled,
		ListenAddr:  fmt.Sprintf("%s (port 514)", c.cfg.Capture.Mode),
		Description: "Passive UDP Syslog capture",
	})

	// 3. Syslog TCP
	syslogTCPEnabled := false
	if c.syslogTCP != nil && c.syslogTCP.IsEnabled() {
		syslogTCPEnabled = true
	}
	statuses = append(statuses, ListenerStatus{
		Name:        "syslog_tcp",
		Type:        "syslog_tcp",
		Enabled:     syslogTCPEnabled,
		ListenAddr:  c.cfg.Capture.SyslogTCP.ListenAddr,
		Description: "RFC 6587 / newline TCP Syslog spooler",
	})

	// 4. HTTP
	httpEnabled := false
	if c.http != nil && c.http.IsEnabled() {
		httpEnabled = true
	}
	statuses = append(statuses, ListenerStatus{
		Name:        "http",
		Type:        "http",
		Enabled:     httpEnabled,
		ListenAddr:  c.cfg.Capture.HTTP.ListenAddr,
		Description: "HTTP event ingest spooler",
	})

	return statuses
}

// Stop stops all active listeners.
func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.traffic != nil {
		c.traffic.Stop()
	}
	if c.http != nil {
		_ = c.http.Stop()
	}
	if c.syslogTCP != nil {
		_ = c.syslogTCP.Stop()
	}
}
