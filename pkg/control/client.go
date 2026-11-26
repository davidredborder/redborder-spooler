package control

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// Client sends commands to a running spooler daemon over the control socket.
type Client struct {
	socketPath string
	timeout    time.Duration
}

// NewClient constructs a new control client.
func NewClient(socketPath string) *Client {
	return &Client{
		socketPath: socketPath,
		timeout:    3 * time.Second,
	}
}

// Send dispatches a command and optional target to the daemon.
func (c *Client) Send(cmd, target string) (*Response, error) {
	conn, err := net.DialTimeout("unix", c.socketPath, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to daemon control socket at %s (is daemon running?): %w", c.socketPath, err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(c.timeout))

	req := Request{Command: cmd, Target: target}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')

	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("failed to send command: %w", err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read response from daemon: %w", err)
	}

	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, fmt.Errorf("invalid response json: %w", err)
	}

	return &resp, nil
}
