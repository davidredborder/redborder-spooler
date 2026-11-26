package capture

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

const (
	// MaxSyslogMsgLen sets the upper limit (64 KB) on octet-counted syslog frames.
	MaxSyslogMsgLen = 65536
)

// TCPSyslogSpooler listens on a TCP port, parses syslog frames, and records them to WAL.
type TCPSyslogSpooler struct {
	cfg      config.TCPSyslogConfig
	store    *storage.HourlyStore
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex
	closed   bool
	conns    map[net.Conn]struct{}
}

// NewTCPSyslogSpooler constructs a new TCPSyslogSpooler.
func NewTCPSyslogSpooler(cfg config.TCPSyslogConfig, store *storage.HourlyStore) *TCPSyslogSpooler {
	return &TCPSyslogSpooler{
		cfg:   cfg,
		store: store,
	}
}

// Addr returns the listening network address if running.
func (s *TCPSyslogSpooler) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// IsEnabled returns true if the spooler is currently listening.
func (s *TCPSyslogSpooler) IsEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener != nil && !s.closed
}

// Start opens the TCP listener and starts the accept loop.
func (s *TCPSyslogSpooler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener != nil && !s.closed {
		return nil
	}

	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on TCP syslog addr %s: %w", s.cfg.ListenAddr, err)
	}

	s.listener = ln
	s.quit = make(chan struct{})
	s.conns = make(map[net.Conn]struct{})
	s.closed = false

	s.wg.Add(1)
	go s.acceptLoop(ln)

	return nil
}

func (s *TCPSyslogSpooler) acceptLoop(ln net.Listener) {
	defer s.wg.Done()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}

		s.mu.Lock()
		if s.closed {
			_ = conn.Close()
			s.mu.Unlock()
			return
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()

		go s.handleConn(conn)
	}
}

func (s *TCPSyslogSpooler) handleConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()

	clientIPStr, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		clientIPStr = conn.RemoteAddr().String()
	}
	clientIP := net.ParseIP(clientIPStr)
	if clientIP == nil {
		clientIP = net.IPv4zero
	}

	reader := bufio.NewReader(conn)

	for {
		// Read single leading byte
		b, err := reader.ReadByte()
		if err != nil {
			return
		}
		if b == '\r' || b == '\n' {
			continue
		}

		var rawMsg []byte
		if b >= '0' && b <= '9' {
			// Initial digit: could be RFC 6587 octet counting
			digits := []byte{b}
			isOctet := false
			for {
				nb, err := reader.ReadByte()
				if err != nil {
					rawMsg = bytes.TrimRight(digits, "\r\n")
					break
				}
				if nb >= '0' && nb <= '9' {
					digits = append(digits, nb)
					if len(digits) > 10 { // longer than 32-bit int
						rest, _ := reader.ReadBytes('\n')
						line := append(digits, rest...)
						rawMsg = bytes.TrimRight(line, "\r\n")
						break
					}
				} else if nb == ' ' {
					isOctet = true
					break
				} else {
					// Character is not digit and not space
					rest, _ := reader.ReadBytes('\n')
					line := append(append(digits, nb), rest...)
					rawMsg = bytes.TrimRight(line, "\r\n")
					break
				}
			}

			if isOctet {
				parsedLen, parseErr := strconv.Atoi(string(digits))
				if parseErr == nil && parsedLen > 0 && parsedLen <= MaxSyslogMsgLen {
					buf := make([]byte, parsedLen)
					if _, err := io.ReadFull(reader, buf); err != nil {
						return
					}
					rawMsg = bytes.TrimRight(buf, "\r\n")
				}
			}
		} else {
			// Starts with non-digit: standard newline-delimited stream
			rest, err := reader.ReadBytes('\n')
			line := append([]byte{b}, rest...)
			rawMsg = bytes.TrimRight(line, "\r\n")
			if err != nil && len(rawMsg) == 0 {
				return
			}
		}

		if len(rawMsg) > 0 {
			s.processMessage(clientIP, rawMsg)
		}
	}
}

func (s *TCPSyslogSpooler) processMessage(clientIP net.IP, msg []byte) {
	// Drop replayed syslog messages to avoid feedback loops
	if bytes.Contains(msg, []byte("X-Redborder-Replay")) || bytes.HasPrefix(msg, []byte("[REPLAY]")) {
		return
	}

	now := time.Now().UTC()

	// Write to HourlyStore
	if s.store != nil {
		_ = s.store.WriteSyslog(now, clientIP, "tcp", msg)
	}

	// Asynchronously forward raw line to upstream if configured
	if s.cfg.UpstreamAddr != "" {
		fwdData := make([]byte, len(msg)+1)
		copy(fwdData, msg)
		fwdData[len(msg)] = '\n'
		s.mu.Lock()
		if !s.closed {
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.forwardUpstream(fwdData)
			}()
		}
		s.mu.Unlock()
	}
}

func (s *TCPSyslogSpooler) forwardUpstream(data []byte) {
	var dialer net.Dialer
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", s.cfg.UpstreamAddr)
	if err != nil {
		return
	}
	defer conn.Close()

	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
}

// Stop gracefully shuts down the listener and active connections.
func (s *TCPSyslogSpooler) Stop() error {
	s.mu.Lock()
	if s.closed || s.listener == nil {
		s.closed = true
		s.mu.Unlock()
		return nil
	}
	s.closed = true

	close(s.quit)
	ln := s.listener
	s.listener = nil
	_ = ln.Close()

	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	return nil
}
