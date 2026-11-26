package control

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/redborder/redborder-spooler/pkg/capture"
)

// Server listens on a UNIX domain socket and dispatches runtime control commands.
type Server struct {
	socketPath string
	controller *capture.Controller
	listener   net.Listener
	quit       chan struct{}
	wg         sync.WaitGroup
	startTime  time.Time
	mu         sync.Mutex
	closed     bool
}

// NewServer creates a new control server.
func NewServer(socketPath string, controller *capture.Controller) *Server {
	return &Server{
		socketPath: socketPath,
		controller: controller,
		startTime:  time.Now(),
		quit:       make(chan struct{}),
	}
}

// Start opens the UNIX socket and starts serving requests.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure parent directory exists
	if dir := filepath.Dir(s.socketPath); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}

	// Check if another daemon is already actively listening on the socket
	conn, err := net.DialTimeout("unix", s.socketPath, 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return fmt.Errorf("control socket %s is already in use by another running instance", s.socketPath)
	}

	// Remove stale or inactive socket file
	_ = os.Remove(s.socketPath)

	ln, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on control socket %s: %w", s.socketPath, err)
	}

	// Set socket file permissions (0660) so daemon group/user can interact
	if err := os.Chmod(s.socketPath, 0660); err != nil {
		_ = ln.Close()
		_ = os.Remove(s.socketPath)
		return fmt.Errorf("failed to set permissions on control socket %s: %w", s.socketPath, err)
	}

	s.listener = ln
	s.closed = false

	s.wg.Add(1)
	go s.acceptLoop(ln)

	return nil
}

func (s *Server) acceptLoop(ln net.Listener) {
	defer s.wg.Done()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.wg.Done()
	}()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		line := scanner.Bytes()
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			s.sendResp(conn, Response{Success: false, Message: fmt.Sprintf("invalid request format: %v", err)})
			return
		}
		resp := s.processRequest(req)
		s.sendResp(conn, resp)
	}
}

func (s *Server) processRequest(req Request) Response {
	switch req.Command {
	case "status":
		var listeners []capture.ListenerStatus
		if s.controller != nil {
			listeners = s.controller.Status()
		}
		return Response{
			Success:   true,
			Message:   "OK",
			PID:       os.Getpid(),
			UptimeSec: int64(time.Since(s.startTime).Seconds()),
			Listeners: listeners,
		}
	case "enable":
		if s.controller == nil {
			return Response{Success: false, Message: "controller not available"}
		}
		if err := s.controller.Enable(req.Target); err != nil {
			return Response{Success: false, Message: err.Error()}
		}
		return Response{
			Success:   true,
			Message:   fmt.Sprintf("enabled %s successfully", req.Target),
			PID:       os.Getpid(),
			UptimeSec: int64(time.Since(s.startTime).Seconds()),
			Listeners: s.controller.Status(),
		}
	case "disable":
		if s.controller == nil {
			return Response{Success: false, Message: "controller not available"}
		}
		if err := s.controller.Disable(req.Target); err != nil {
			return Response{Success: false, Message: err.Error()}
		}
		return Response{
			Success:   true,
			Message:   fmt.Sprintf("disabled %s successfully", req.Target),
			PID:       os.Getpid(),
			UptimeSec: int64(time.Since(s.startTime).Seconds()),
			Listeners: s.controller.Status(),
		}
	default:
		return Response{Success: false, Message: fmt.Sprintf("unknown command '%s'", req.Command)}
	}
}

func (s *Server) sendResp(conn net.Conn, resp Response) {
	data, _ := json.Marshal(resp)
	data = append(data, '\n')
	_, _ = conn.Write(data)
}

// Stop cleanly terminates the control server and removes the socket file.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.closed || s.listener == nil {
		s.closed = true
		s.mu.Unlock()
		return
	}
	s.closed = true
	ln := s.listener
	s.listener = nil
	_ = ln.Close()
	_ = os.Remove(s.socketPath)
	s.mu.Unlock()

	s.wg.Wait()
}
