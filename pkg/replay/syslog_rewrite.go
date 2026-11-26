package replay

import (
	"bytes"
	"net"
	"time"
)

var rfc3164Months = [][]byte{
	[]byte("Jan"), []byte("Feb"), []byte("Mar"), []byte("Apr"),
	[]byte("May"), []byte("Jun"), []byte("Jul"), []byte("Aug"),
	[]byte("Sep"), []byte("Oct"), []byte("Nov"), []byte("Dec"),
}

// RewriteSyslogHostname replaces the hostname token in an RFC 3164 or RFC 5424 syslog
// message with the provided client IP string, allowing local replay over 127.0.0.1 to be
// uniquely routed to per-sensor rules in rsyslog.
func RewriteSyslogHostname(raw []byte, clientIP net.IP) []byte {
	if len(clientIP) == 0 || clientIP.IsUnspecified() {
		return raw
	}
	ipStr := clientIP.String()
	if ipStr == "" || ipStr == "<nil>" || ipStr == "0.0.0.0" || ipStr == "::" {
		return raw
	}

	if len(raw) == 0 || raw[0] != '<' {
		return raw
	}

	priEnd := bytes.IndexByte(raw, '>')
	if priEnd <= 1 || priEnd > 5 {
		return raw
	}
	for i := 1; i < priEnd; i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return raw
		}
	}

	rest := raw[priEnd+1:]
	if len(rest) == 0 {
		return raw
	}

	// 1. Check RFC 5424: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME ...
	// VERSION is numeric followed by space, e.g. "1 "
	versionSpace := bytes.IndexByte(rest, ' ')
	if versionSpace > 0 && isAllDigits(rest[:versionSpace]) {
		// Found RFC 5424 version
		timePart := rest[versionSpace+1:]
		timeSpace := bytes.IndexByte(timePart, ' ')
		if timeSpace > 0 {
			hostStart := versionSpace + 1 + timeSpace + 1
			if hostStart < len(rest) {
				hostPart := rest[hostStart:]
				hostSpace := bytes.IndexByte(hostPart, ' ')
				var hostEnd int
				var afterHost []byte
				if hostSpace >= 0 {
					hostEnd = hostStart + hostSpace
					afterHost = rest[hostEnd:]
				} else {
					hostEnd = len(rest)
					afterHost = nil
				}

				buf := make([]byte, 0, len(raw)+len(ipStr))
				buf = append(buf, raw[:priEnd+1]...)
				buf = append(buf, rest[:hostStart]...)
				buf = append(buf, ipStr...)
				if afterHost != nil {
					buf = append(buf, afterHost...)
				}
				return buf
			}
		}
	}

	// 2. Check RFC 3164: <PRI>Mmm dd hh:mm:ss HOSTNAME ...
	// Standard timestamp is 15 bytes: "Mmm dd hh:mm:ss "
	if len(rest) >= 16 && rest[15] == ' ' && isRFC3164Month(rest[:3]) {
		hostStart := 16
		hostPart := rest[hostStart:]
		hostSpace := bytes.IndexByte(hostPart, ' ')
		var hostEnd int
		var afterHost []byte
		if hostSpace >= 0 {
			hostEnd = hostStart + hostSpace
			afterHost = rest[hostEnd:]
		} else {
			// Hostname might be terminated by a colon if there's no space before tag
			colonIdx := bytes.IndexByte(hostPart, ':')
			if colonIdx >= 0 {
				hostEnd = hostStart + colonIdx
				afterHost = rest[hostEnd:]
			} else {
				hostEnd = len(rest)
				afterHost = nil
			}
		}

		buf := make([]byte, 0, len(raw)+len(ipStr))
		buf = append(buf, raw[:priEnd+1]...)
		buf = append(buf, rest[:hostStart]...)
		buf = append(buf, ipStr...)
		if afterHost != nil {
			buf = append(buf, afterHost...)
		}
		return buf
	}

	return raw
}

func isAllDigits(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func isRFC3164Month(b []byte) bool {
	if len(b) < 3 {
		return false
	}
	for _, m := range rfc3164Months {
		if bytes.Equal(b[:3], m) {
			return true
		}
	}
	return false
}

// RewriteSyslogTimestamp replaces the timestamp in an RFC 3164 or RFC 5424 syslog message.
func RewriteSyslogTimestamp(raw []byte, newTime time.Time) []byte {
	if len(raw) == 0 || raw[0] != '<' {
		return raw
	}
	priEnd := bytes.IndexByte(raw, '>')
	if priEnd <= 1 || priEnd > 5 {
		return raw
	}
	for i := 1; i < priEnd; i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return raw
		}
	}

	rest := raw[priEnd+1:]
	if len(rest) == 0 {
		return raw
	}

	// 1. Check RFC 5424: <PRI>VERSION TIMESTAMP HOSTNAME ...
	versionSpace := bytes.IndexByte(rest, ' ')
	if versionSpace > 0 && isAllDigits(rest[:versionSpace]) {
		timePart := rest[versionSpace+1:]
		timeSpace := bytes.IndexByte(timePart, ' ')
		if timeSpace > 0 {
			afterTime := timePart[timeSpace:]
			newTS := newTime.Format(time.RFC3339Nano)
			buf := make([]byte, 0, len(raw)+len(newTS))
			buf = append(buf, raw[:priEnd+1]...)
			buf = append(buf, rest[:versionSpace+1]...)
			buf = append(buf, newTS...)
			buf = append(buf, afterTime...)
			return buf
		}
	}

	// 2. Check RFC 3164: <PRI>Mmm dd hh:mm:ss HOSTNAME ...
	if len(rest) >= 16 && rest[15] == ' ' && isRFC3164Month(rest[:3]) {
		afterTime := rest[15:]
		newTS := newTime.Format("Jan _2 15:04:05")
		buf := make([]byte, 0, len(raw)+len(newTS))
		buf = append(buf, raw[:priEnd+1]...)
		buf = append(buf, newTS...)
		buf = append(buf, afterTime...)
		return buf
	}

	return raw
}
