package control

import "github.com/redborder/redborder-spooler/pkg/capture"

// Request is the JSON command payload sent across the control socket.
type Request struct {
	Command string `json:"command"` // "status", "enable", "disable"
	Target  string `json:"target"`  // "syslog_tcp", "http", "traffic", "syslog_udp", "all"
}

// Response is the JSON reply from the spooler daemon.
type Response struct {
	Success   bool                     `json:"success"`
	Message   string                   `json:"message"`
	PID       int                      `json:"pid,omitempty"`
	UptimeSec int64                    `json:"uptime_sec,omitempty"`
	Listeners []capture.ListenerStatus `json:"listeners,omitempty"`
}
