package replay_test

import (
	"net"
	"testing"
	"time"

	"github.com/redborder/redborder-spooler/pkg/replay"
)

func TestRewriteSyslogHostname_RFC3164(t *testing.T) {
	ip := net.ParseIP("10.1.33.158")

	// Standard RFC 3164 message
	raw := []byte("<14>Nov 25 14:13:00 debianDavid Fortitray.desktop[3620503]: /opt/forticlient/fortitray: error")
	rewritten := replay.RewriteSyslogHostname(raw, ip)
	expected := "<14>Nov 25 14:13:00 10.1.33.158 Fortitray.desktop[3620503]: /opt/forticlient/fortitray: error"
	if string(rewritten) != expected {
		t.Fatalf("expected:\n%s\ngot:\n%s", expected, string(rewritten))
	}

	// RFC 3164 with single digit day (two spaces between month and day)
	rawSingleDigit := []byte("<13>Dec  5 09:05:01 myhost CRON[1234]: (root) CMD")
	rewrittenSingleDigit := replay.RewriteSyslogHostname(rawSingleDigit, ip)
	expectedSingleDigit := "<13>Dec  5 09:05:01 10.1.33.158 CRON[1234]: (root) CMD"
	if string(rewrittenSingleDigit) != expectedSingleDigit {
		t.Fatalf("expected:\n%s\ngot:\n%s", expectedSingleDigit, string(rewrittenSingleDigit))
	}
}

func TestRewriteSyslogHostname_RFC5424(t *testing.T) {
	ip := net.ParseIP("10.1.33.158")

	raw := []byte("<165>1 2026-11-25T14:13:00.003Z debianDavid evntslog - ID47 [exampleSDID@32473 iut=\"3\"] application event")
	rewritten := replay.RewriteSyslogHostname(raw, ip)
	expected := "<165>1 2026-11-25T14:13:00.003Z 10.1.33.158 evntslog - ID47 [exampleSDID@32473 iut=\"3\"] application event"
	if string(rewritten) != expected {
		t.Fatalf("expected:\n%s\ngot:\n%s", expected, string(rewritten))
	}

	// RFC 5424 with nil hostname '-'
	rawNilHost := []byte("<165>1 2026-11-25T14:13:00.003Z - evntslog - ID47 - application event")
	rewrittenNilHost := replay.RewriteSyslogHostname(rawNilHost, ip)
	expectedNilHost := "<165>1 2026-11-25T14:13:00.003Z 10.1.33.158 evntslog - ID47 - application event"
	if string(rewrittenNilHost) != expectedNilHost {
		t.Fatalf("expected:\n%s\ngot:\n%s", expectedNilHost, string(rewrittenNilHost))
	}
}

func TestRewriteSyslogHostname_EdgeCases(t *testing.T) {
	ip := net.ParseIP("10.1.33.158")

	// Nil / empty IP returns original message
	raw := []byte("<14>Nov 25 14:13:00 debianDavid Fortitray.desktop: msg")
	if string(replay.RewriteSyslogHostname(raw, nil)) != string(raw) {
		t.Fatal("expected nil IP to return original message")
	}
	if string(replay.RewriteSyslogHostname(raw, net.IPv6zero)) != string(raw) {
		t.Fatal("expected zero IP to return original message")
	}

	// Non-syslog message without <PRI>
	nonSyslog := []byte("plain text without pri header")
	if string(replay.RewriteSyslogHostname(nonSyslog, ip)) != string(nonSyslog) {
		t.Fatal("expected non-syslog message to remain unchanged")
	}

	// IPv6 client IP
	ip6 := net.ParseIP("2001:db8::1")
	rewritten6 := replay.RewriteSyslogHostname(raw, ip6)
	expected6 := "<14>Nov 25 14:13:00 2001:db8::1 Fortitray.desktop: msg"
	if string(rewritten6) != expected6 {
		t.Fatalf("expected:\n%s\ngot:\n%s", expected6, string(rewritten6))
	}
}

func TestRewriteSyslogTimestamp(t *testing.T) {
	targetTime := time.Date(2026, 11, 26, 14, 30, 45, 0, time.UTC)

	// 1. RFC 3164
	raw3164 := []byte("<14>Nov 25 10:00:00 myhost app: something happened")
	rewritten3164 := replay.RewriteSyslogTimestamp(raw3164, targetTime)
	expected3164 := "<14>Nov 26 14:30:45 myhost app: something happened"
	if string(rewritten3164) != expected3164 {
		t.Fatalf("RFC 3164 rewrite mismatch: got %q, want %q", string(rewritten3164), expected3164)
	}

	// 2. RFC 5424
	raw5424 := []byte("<165>1 2026-11-25T10:00:00.000Z myhost app - ID47 msg")
	rewritten5424 := replay.RewriteSyslogTimestamp(raw5424, targetTime)
	expected5424 := "<165>1 2026-11-26T14:30:45Z myhost app - ID47 msg"
	if string(rewritten5424) != expected5424 {
		t.Fatalf("RFC 5424 rewrite mismatch: got %q, want %q", string(rewritten5424), expected5424)
	}
}
