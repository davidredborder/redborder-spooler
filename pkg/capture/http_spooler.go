package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

// HTTPSpooler intercepts incoming HTTP POST events, injects missing timestamps, and logs to WAL.
type HTTPSpooler struct {
	cfg        config.HTTPConfig
	store      *storage.HourlyStore
	server     *http.Server
	listener   net.Listener
	httpClient *http.Client
	mu         sync.Mutex
	running    bool
}

// NewHTTPSpooler constructs an HTTPSpooler.
func NewHTTPSpooler(cfg config.HTTPConfig, store *storage.HourlyStore) *HTTPSpooler {
	return &HTTPSpooler{
		cfg:   cfg,
		store: store,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// IsEnabled reports whether the HTTP server is currently running.
func (s *HTTPSpooler) IsEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// Start launches the HTTP server.
func (s *HTTPSpooler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRequest)

	s.server = &http.Server{
		Addr:    s.cfg.ListenAddr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on HTTP addr %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = ln
	s.running = true

	go func(srv *http.Server, ln net.Listener) {
		_ = srv.Serve(ln)
		s.mu.Lock()
		if s.server == srv {
			s.running = false
			s.server = nil
			s.listener = nil
		}
		s.mu.Unlock()
	}(s.server, ln)

	return nil
}

func (s *HTTPSpooler) handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Drop replayed HTTP traffic to avoid feedback loops
	if r.Header.Get("X-Redborder-Replay") == "true" {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"ignored_replay"}`))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	now := time.Now().UTC()

	// Check if body is JSON and missing "timestamp"
	var parsed map[string]interface{}
	if err := json.Unmarshal(body, &parsed); err == nil && parsed != nil {
		if _, hasTS := parsed["timestamp"]; !hasTS {
			parsed["timestamp"] = now.Unix()
			if newBody, err := json.Marshal(parsed); err == nil {
				body = newBody
			}
		}
	}

	// Extract client IP
	clientIPStr, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		clientIPStr = r.RemoteAddr
	}
	clientIP := net.ParseIP(clientIPStr)
	if clientIP == nil {
		clientIP = net.IPv4zero
	}

	// Write to HourlyStore
	if s.store != nil {
		headers := make(map[string][]string)
		for k, v := range r.Header {
			headers[k] = v
		}
		_ = s.store.WriteHTTP(now, clientIP, r.Method, r.URL.RequestURI(), headers, body)
	}

	// Forward to UpstreamURL if configured
	if s.cfg.UpstreamURL != "" {
		method := r.Method
		requestURI := r.URL.RequestURI()
		headersClone := r.Header.Clone()
		bodyClone := make([]byte, len(body))
		copy(bodyClone, body)
		go s.forwardUpstream(method, requestURI, headersClone, bodyClone)
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"accepted"}`))
}

func (s *HTTPSpooler) forwardUpstream(method, requestURI string, headers http.Header, body []byte) {
	upstreamURL := strings.TrimRight(s.cfg.UpstreamURL, "/") + requestURI
	req, err := http.NewRequestWithContext(context.Background(), method, upstreamURL, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header = headers
	resp, err := s.httpClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
}

// Stop gracefully shuts down the HTTP server.
func (s *HTTPSpooler) Stop() error {
	s.mu.Lock()
	if !s.running || s.server == nil {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	srv := s.server
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}
