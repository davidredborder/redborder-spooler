package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redborder/redborder-spooler/pkg/capture"
	"github.com/redborder/redborder-spooler/pkg/config"
	"github.com/redborder/redborder-spooler/pkg/control"
	"github.com/redborder/redborder-spooler/pkg/replay"
	"github.com/redborder/redborder-spooler/pkg/storage"
)

const (
	DefaultConfigPath = "/etc/redborder-spooler/config.yaml"
	DefaultSpoolDir   = "/var/spool/redborder-spooler"
)

func resolveSpoolDir(cfgDir string) string {
	if cfgDir != "" {
		return cfgDir
	}
	return DefaultSpoolDir
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	dispatch(command, args)
}

func dispatch(command string, args []string) {
	switch command {
	case "record":
		runRecord(args)
	case "replay":
		runReplay(args)
	case "list":
		runList(args)
	case "prune":
		runPrune(args)
	case "status":
		runStatus(args)
	case "export":
		runExport(args)
	case "import":
		runImport(args)
	case "pcap":
		runPCAP(args)
	case "ctl":
		runCtl(args)
	case "help":
		if len(args) > 0 {
			if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
				printUsage()
				return
			}
			dispatch(args[0], []string{"help"})
			return
		}
		printUsage()
	case "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`redborder-spooler - Traffic capture, spooling and replay daemon

Usage:
  rb-spooler <command> [arguments]

Commands:
  record    Start traffic spooler daemon (UDP + HTTP + Syslog capture + retention)
  replay    Replay spooled traffic records to target service
  list      Display stored UTC hourly buckets and statistics
  export    Export recorded traffic buckets to a compressed .tar.gz bundle
  import    Import traffic buckets from an archive into the spool directory
  pcap      Convert recorded traffic datagrams to a standard PCAP file
  prune     Manually execute retention cleanup
  status    Display spooler status and disk metrics
  ctl       Send runtime control commands to running daemon (status, enable, disable)
  help      Show help for a command (e.g. 'rb-spooler help replay')

Run 'rb-spooler help <command>' or 'rb-spooler <command> -h' for more information on a command.`)
}

func loadConfigWithFallback(cfgPath string) (*config.Config, error) {
	var cfg *config.Config
	var err error
	if cfgPath != "" {
		if _, statErr := os.Stat(cfgPath); statErr == nil {
			cfg, err = config.LoadConfig(cfgPath)
		} else if cfgPath != DefaultConfigPath {
			return nil, fmt.Errorf("config file not found: %s", cfgPath)
		}
	}
	if cfg == nil && err == nil {
		if _, statErr := os.Stat(DefaultConfigPath); statErr == nil {
			cfg, err = config.LoadConfig(DefaultConfigPath)
		} else {
			cfg = config.DefaultConfig()
		}
	}
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		cfg.SpoolDir = resolveSpoolDir(cfg.SpoolDir)
	}
	return cfg, nil
}

func runRecord(args []string) {
	fs := flag.NewFlagSet("record", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Println(`Usage:
  rb-spooler record [options]

Start background traffic spooler daemon (UDP + HTTP + Syslog capture + retention management).

Options:
  --config <path>       Path to configuration file (default: /etc/redborder-spooler/config.yaml)

Examples:
  rb-spooler record
  rb-spooler record --config /custom/config.yaml`)
	}
	if len(args) > 0 && args[0] == "help" {
		fs.Usage()
		return
	}
	configPath := fs.String("config", DefaultConfigPath, "Path to configuration file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(1)
	}

	cfg, err := loadConfigWithFallback(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize hourly store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start UDP Capture
	var trafficListener capture.TrafficListener
	dedupWindow := time.Duration(cfg.Capture.LVSDRDedupWindowMs) * time.Millisecond

	if cfg.Capture.Mode == "af_packet" {
		listener := capture.NewAFPacketListener(cfg.Capture.Interfaces, cfg.Capture.UDPPorts, store, dedupWindow)
		if err := listener.Start(ctx); err != nil {
			fmt.Printf("Notice: AF_PACKET unavailable (%v), falling back to standard UDP sockets\n", err)
			sockListener := capture.NewUDPSocketListener(cfg.Capture.UDPPorts, "0.0.0.0", store, dedupWindow)
			if err := sockListener.Start(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to start UDP socket capture: %v\n", err)
				os.Exit(1)
			}
			trafficListener = sockListener
		} else {
			fmt.Printf("AF_PACKET raw capture started on interfaces %v, ports %v\n", cfg.Capture.Interfaces, cfg.Capture.UDPPorts)
			trafficListener = listener
		}
	} else {
		sockListener := capture.NewUDPSocketListener(cfg.Capture.UDPPorts, "0.0.0.0", store, dedupWindow)
		if err := sockListener.Start(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to start UDP socket capture: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("UDP socket capture started on ports %v\n", cfg.Capture.UDPPorts)
		trafficListener = sockListener
	}

	httpSpooler := capture.NewHTTPSpooler(cfg.Capture.HTTP, store)
	tcpSyslogSpooler := capture.NewTCPSyslogSpooler(cfg.Capture.SyslogTCP, store)

	ctrl := capture.NewController(cfg, store, trafficListener, httpSpooler, tcpSyslogSpooler)
	if err := ctrl.StartInitial(); err != nil {
		fmt.Fprintf(os.Stderr, "Notice starting initial listeners: %v\n", err)
	}
	if httpSpooler != nil && httpSpooler.IsEnabled() {
		fmt.Printf("HTTP spooler listening on %s\n", cfg.Capture.HTTP.ListenAddr)
	}
	if tcpSyslogSpooler != nil && tcpSyslogSpooler.IsEnabled() {
		fmt.Printf("TCP syslog spooler listening on %s\n", cfg.Capture.SyslogTCP.ListenAddr)
	}

	var ctlServer *control.Server
	if cfg.ControlSocket != "" {
		ctlServer = control.NewServer(cfg.ControlSocket, ctrl)
		if err := ctlServer.Start(); err != nil {
			fmt.Printf("Notice: failed to start control socket (%v)\n", err)
		} else {
			fmt.Printf("Control socket listening on %s\n", cfg.ControlSocket)
		}
	}

	// Background retention ticker
	checkInterval := cfg.Retention.CheckInterval
	if checkInterval <= 0 {
		checkInterval = 5 * time.Minute
	}
	retentionTicker := time.NewTicker(checkInterval)
	defer retentionTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-retentionTicker.C:
				pruned, err := store.Prune(t)
				if err == nil && len(pruned) > 0 {
					fmt.Printf("[%s] Retention pruned %d buckets: %v\n", t.UTC().Format(time.RFC3339), len(pruned), pruned)
				}
			}
		}
	}()

	// Periodic metadata sync ticker (updates meta.json and udp.idx while daemon is active)
	syncInterval := cfg.MetaSyncInterval
	if syncInterval > 0 {
		metaTicker := time.NewTicker(syncInterval)
		defer metaTicker.Stop()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-metaTicker.C:
					_ = store.SyncMeta()
				}
			}
		}()
	}

	fmt.Printf("redborder-spooler daemon started (Spool: %s)\n", cfg.SpoolDir)
	<-sigChan
	fmt.Println("\nShutting down gracefully...")

	cancel()
	if ctlServer != nil {
		ctlServer.Stop()
	}
	ctrl.Stop()
	_ = store.Close()
	fmt.Println("redborder-spooler stopped.")
}

func parseTimeArg(val string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02 15",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, val); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse timestamp '%s'", val)
}

func parseSpeed(val string) float64 {
	val = strings.TrimSuffix(strings.TrimSpace(val), "x")
	val = strings.TrimSuffix(val, "X")
	if s, err := strconv.ParseFloat(val, 64); err == nil {
		return s
	}
	return 10.0
}

func runReplay(args []string) {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Println(`Usage:
  rb-spooler replay --from "<timestamp>" [options]

Replay spooled traffic records to target service (supports NetFlow, IPFIX, sFlow, HTTP, and Syslog).

Options:
  --from <time>           Start UTC time (required, format: 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD HH:MM:SS')
  --to <time>             End UTC time (optional, defaults to now)
  --type <type>           Replay traffic type: 'traffic' (or 'udp'), 'http', 'syslog', or 'all' (default: 'traffic', or 'all' if --http-target set)
  --target <host>         Target host for traffic replay (default: 127.0.0.1)
  --http-target <url>     Base URL for HTTP replay (required if --type is http/all, e.g. 'http://127.0.0.1:7980')
  --syslog-target <host:port> Target host:port for Syslog replay (default: 127.0.0.1:514)
  --syslog-inject-client-ip   Inject recorded Client IP as hostname in replayed Syslog messages (default: true)
  --ports <ports>         Comma-separated list of destination ports to replay (e.g. '2055,6343')
  --speed <multiplier>    Replay speed multiplier (e.g. 1.0, 5x, 10x; 0 for unthrottled burst) (default: 10x)
  --rbrp                  Wrap UDP packets in 28-byte RBRP replay envelope (default: true)
  --time-shift            Shift replay timestamps to the current time window (for testing/active segments)
  --follow                Continuously stream live traffic after replaying historical backlog (default: false)
  -y, --yes               Automatic yes to confirmation prompt (run non-interactively)
  --config <path>         Path to configuration file (default: /etc/redborder-spooler/config.yaml)

Examples:
  rb-spooler replay --from "2026-11-24 10:00" --speed 10x
  rb-spooler replay --from "2026-11-25 08:00" --type syslog --syslog-target 127.0.0.1:514
  rb-spooler replay --from "2026-11-25 08:00" --type http --http-target http://127.0.0.1:7980
  rb-spooler replay --from "2026-11-25 08:00" --type all --http-target http://127.0.0.1:7980 -y
  rb-spooler replay --from "2026-11-24 10:00" --ports 2055,6343
  rb-spooler replay --from "2026-11-24 10:00" --to "2026-11-24 10:30" --speed 0`)
	}
	if len(args) > 0 && args[0] == "help" {
		fs.Usage()
		return
	}
	configPath := fs.String("config", DefaultConfigPath, "Path to configuration file")
	fromStr := fs.String("from", "", "Start UTC time (e.g. '2026-11-24 10:00')")
	toStr := fs.String("to", "", "End UTC time (optional, defaults to now)")
	speedStr := fs.String("speed", "10x", "Replay speed multiplier (e.g. 1.0, 5x, 10x; 0 for burst)")
	target := fs.String("target", "127.0.0.1", "Replay target host (defaults to 127.0.0.1)")
	httpTarget := fs.String("http-target", "", "Replay HTTP target base URL (optional)")
	syslogTarget := fs.String("syslog-target", "127.0.0.1:514", "Target host:port for Syslog replay (default: 127.0.0.1:514)")
	syslogInjectClientIP := fs.Bool("syslog-inject-client-ip", true, "Inject recorded Client IP as hostname in replayed Syslog messages (default: true)")
	portsStr := fs.String("ports", "", "Comma-separated list of UDP destination ports to replay")
	typeStr := fs.String("type", "", "Replay traffic type: 'traffic', 'udp', 'http', 'syslog', or 'all' (defaults to 'all' if --http-target set, otherwise 'traffic')")
	rbrp := fs.Bool("rbrp", true, "Wrap packets in 28-byte RBRP replay envelope")
	timeShift := fs.Bool("time-shift", false, "Shift replay timestamps to the current time window")
	follow := fs.Bool("follow", false, "Continuously stream live traffic after replaying historical backlog")
	yes := fs.Bool("y", false, "Automatic yes to confirmation prompt (run non-interactively)")
	fs.BoolVar(yes, "yes", false, "Automatic yes to confirmation prompt (run non-interactively)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(1)
	}

	if *fromStr == "" {
		fmt.Fprintln(os.Stderr, "Error: --from argument is required")
		fs.Usage()
		os.Exit(1)
	}

	fromTime, err := parseTimeArg(*fromStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid --from time: %v\n", err)
		os.Exit(1)
	}

	toTime := time.Now().UTC()
	if *toStr != "" {
		t, err := parseTimeArg(*toStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid --to time: %v\n", err)
			os.Exit(1)
		}
		toTime = t
	}

	var portsFilter []uint16
	if *portsStr != "" {
		for _, pStr := range strings.Split(*portsStr, ",") {
			pStr = strings.TrimSpace(pStr)
			if pStr == "" {
				continue
			}
			p, err := strconv.ParseUint(pStr, 10, 16)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid port '%s' in --ports: %v\n", pStr, err)
				os.Exit(1)
			}
			portsFilter = append(portsFilter, uint16(p))
		}
	}

	syslogHost := "127.0.0.1"
	syslogPort := 514
	if *syslogTarget != "" {
		h, p, err := net.SplitHostPort(*syslogTarget)
		if err == nil {
			syslogHost = h
			parsedPort, err := strconv.Atoi(p)
			if err != nil || parsedPort < 1 || parsedPort > 65535 {
				fmt.Fprintf(os.Stderr, "Error: invalid port in --syslog-target '%s'\n", *syslogTarget)
				os.Exit(1)
			}
			syslogPort = parsedPort
		} else {
			syslogHost = *syslogTarget
		}
	}

	replayType := strings.ToLower(strings.TrimSpace(*typeStr))
	if replayType == "" {
		if *httpTarget != "" {
			replayType = "all"
		} else {
			replayType = "traffic"
		}
	}
	if replayType == "udp" {
		replayType = "traffic"
	}
	if replayType != "traffic" && replayType != "http" && replayType != "syslog" && replayType != "all" {
		fmt.Fprintf(os.Stderr, "Error: invalid --type '%s' (must be 'traffic', 'udp', 'http', 'syslog', or 'all')\n", *typeStr)
		os.Exit(1)
	}
	if (replayType == "http" || (replayType == "all" && *typeStr != "")) && *httpTarget == "" {
		fmt.Fprintf(os.Stderr, "Error: --http-target is required when --type is '%s'\n", replayType)
		os.Exit(1)
	}

	cfg, err := loadConfigWithFallback(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open hourly store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	speed := parseSpeed(*speedStr)
	opts := replay.Options{
		FromTimeUTC:          fromTime,
		ToTimeUTC:            toTime,
		SpeedMultiplier:      speed,
		TargetHost:           *target,
		UseRBRP:              *rbrp,
		HTTPTargetURL:        *httpTarget,
		SyslogTargetHost:     syslogHost,
		SyslogTargetPort:     syslogPort,
		SyslogInjectClientIP: *syslogInjectClientIP,
		PortsFilter:          portsFilter,
		ReplayType:           replayType,
		TimeShift:            *timeShift,
		Follow:               *follow,
	}

	totalExpectedPkts := store.EstimatePackets(fromTime, toTime)
	totalExpectedHTTP := store.EstimateHTTPRequests(fromTime, toTime)
	totalExpectedSyslog := store.EstimateSyslogMessages(fromTime, toTime)

	// Action summary
	fmt.Println("Replay Action Summary:")
	if *follow {
		fmt.Printf("  Time Range:     %s UTC to LIVE [Follow mode]\n", fromTime.Format("2006-01-02 15:04:05"))
	} else {
		fmt.Printf("  Time Range:     %s UTC to %s UTC\n", fromTime.Format("2006-01-02 15:04:05"), toTime.Format("2006-01-02 15:04:05"))
	}
	fmt.Printf("  Replay Type:    %s\n", strings.ToUpper(replayType))
	if len(portsFilter) > 0 {
		var portStrs []string
		for _, p := range portsFilter {
			portStrs = append(portStrs, strconv.Itoa(int(p)))
		}
		fmt.Printf("  Ports Filter:   %s\n", strings.Join(portStrs, ", "))
	}
	if replayType == "traffic" || replayType == "all" {
		fmt.Printf("  Traffic Target: %s (RBRP: %v)\n", *target, *rbrp)
	}
	if (replayType == "http" || replayType == "all") && *httpTarget != "" {
		fmt.Printf("  HTTP Target:    %s\n", *httpTarget)
	}
	if replayType == "syslog" || replayType == "all" {
		fmt.Printf("  Syslog Target:  %s:%d\n", syslogHost, syslogPort)
	}
	if speed == 0 {
		fmt.Printf("  Speed:          Burst (maximum wire speed)\n")
	} else {
		fmt.Printf("  Speed:          %.1fx\n", speed)
	}
	if *timeShift {
		fmt.Printf("  Time Shift:     Enabled (timestamps shifted to current window)\n")
	}

	switch replayType {
	case "traffic":
		fmt.Printf("  Estimated Data: %d traffic packets\n", totalExpectedPkts)
	case "http":
		fmt.Printf("  Estimated Data: %d HTTP requests\n", totalExpectedHTTP)
	case "syslog":
		fmt.Printf("  Estimated Data: %d Syslog messages\n", totalExpectedSyslog)
	case "all":
		fmt.Printf("  Estimated Data: %d traffic packets, %d HTTP requests, %d Syslog messages\n", totalExpectedPkts, totalExpectedHTTP, totalExpectedSyslog)
	}
	fmt.Println()

	if !*yes {
		fmt.Print("Proceed with replay? [y/N]: ")
		var answer string
		_, err := fmt.Scanln(&answer)
		if err != nil || (strings.ToLower(strings.TrimSpace(answer)) != "y" && strings.ToLower(strings.TrimSpace(answer)) != "yes") {
			fmt.Println("Replay canceled.")
			return
		}
		fmt.Println()
	}

	expectedCount := totalExpectedPkts
	switch replayType {
	case "http":
		expectedCount = totalExpectedHTTP
	case "syslog":
		expectedCount = totalExpectedSyslog
	case "all":
		expectedCount = totalExpectedPkts + totalExpectedHTTP + totalExpectedSyslog
	}

	progress := replay.NewProgressBar(os.Stdout, expectedCount, speed)
	switch replayType {
	case "syslog":
		progress.SetUnit("msgs")
	case "http":
		progress.SetUnit("reqs")
	case "all":
		progress.SetUnit("items")
	default:
		progress.SetUnit("pkts")
	}
	if *follow {
		progress.SetFollow(true)
		opts.OnPacket = func(pkts uint64, capTime time.Time) {
			progress.UpdateWithTime(pkts, capTime)
		}
	} else {
		opts.OnProgress = func(pkts uint64) {
			progress.Update(pkts)
		}
	}

	replayer := replay.NewReplayer(store)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	stats, err := replayer.Run(ctx, opts)
	progress.Finish(err)
	if err != nil && !errorsIsCanceled(err) {
		os.Exit(1)
	}

	fmt.Println("\nReplay Summary:")
	if replayType == "traffic" || replayType == "all" {
		fmt.Printf("  Traffic Packets Replayed: %d\n", stats.PacketsReplayed)
		fmt.Printf("  Traffic Bytes Replayed:   %d\n", stats.BytesReplayed)
		if stats.TemplatesSent > 0 {
			fmt.Printf("  Templates Sent:           %d\n", stats.TemplatesSent)
		}
	}
	if replayType == "http" || replayType == "all" {
		if stats.HTTPRequestsReplayed > 0 || replayType == "http" {
			fmt.Printf("  HTTP Requests:        %d\n", stats.HTTPRequestsReplayed)
			fmt.Printf("  HTTP Bytes:           %d\n", stats.HTTPBytesReplayed)
		}
	}
	if replayType == "syslog" || replayType == "all" {
		if stats.SyslogMessagesReplayed > 0 || stats.SyslogMessagesDropped > 0 || replayType == "syslog" {
			fmt.Printf("  Syslog Messages:      %d\n", stats.SyslogMessagesReplayed)
			if stats.SyslogMessagesDropped > 0 {
				fmt.Printf("  Syslog Dropped:       %d (target rejected write)\n", stats.SyslogMessagesDropped)
			}
		}
	}
	fmt.Printf("  Duration:             %s\n", stats.Duration.Round(time.Millisecond))
}

func errorsIsCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func runList(args []string) {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Println(`Usage:
  rb-spooler list [options]

Display stored UTC hourly buckets, packet/request counts, disk usage, and exporters.

Options:
  -v, --verbose         Verbose output: display unique exporter IP addresses per bucket
  --config <path>       Path to configuration file (default: /etc/redborder-spooler/config.yaml)

Examples:
  rb-spooler list
  rb-spooler list -v`)
	}
	if len(args) > 0 && args[0] == "help" {
		fs.Usage()
		return
	}
	configPath := fs.String("config", DefaultConfigPath, "Path to configuration file")
	verbose := fs.Bool("v", false, "Verbose output (show exporter IPs)")
	fs.BoolVar(verbose, "verbose", false, "Verbose output (show exporter IPs)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(1)
	}

	cfg, err := loadConfigWithFallback(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open hourly store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	buckets, err := store.ListBuckets()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to list buckets: %v\n", err)
		os.Exit(1)
	}

	if len(buckets) == 0 {
		fmt.Printf("No traffic buckets stored in %s\n", cfg.SpoolDir)
		return
	}

	fmt.Printf("Stored Traffic Buckets in %s (%d hours):\n", cfg.SpoolDir, len(buckets))
	firstTime, hasFirst := store.FirstAvailableTime()
	lastTime, hasLast := store.LastAvailableTime()
	if hasFirst {
		if hasLast && !lastTime.Equal(firstTime) {
			fmt.Printf("Available Data Range:   %s UTC to %s UTC\n\n", firstTime.Format("2006-01-02 15:04:05"), lastTime.Format("2006-01-02 15:04:05"))
		} else {
			fmt.Printf("First Available Packet: %s UTC\n\n", firstTime.Format("2006-01-02 15:04:05"))
		}
	} else {
		fmt.Println()
	}
	fmt.Printf("%-18s  %-12s  %-13s  %-12s  %-12s  %-12s  %-10s  %s\n",
		"HOUR (UTC)", "TRAFFIC PKTS", "TRAFFIC BYTES", "HTTP REQS", "SYSLOG MSGS", "DISK SIZE", "TEMPLATES", "EXPORTERS")
	fmt.Println(strings.Repeat("-", 110))

	var totalUDPPkts, totalUDPBytes, totalHTTPReqs, totalSyslogMsgs uint64
	var totalDiskBytes int64

	for _, b := range buckets {
		totalUDPPkts += b.UDPPackets
		totalUDPBytes += b.UDPBytes
		totalHTTPReqs += b.HTTPRequests
		totalSyslogMsgs += b.SyslogMessages
		totalDiskBytes += b.DiskBytes

		snapPath := filepath.Join(cfg.SpoolDir, b.HourPath, storage.TemplateSnapName)
		tmplCount := 0
		tracker := storage.NewTemplateTracker()
		if err := tracker.LoadSnapshot(snapPath); err == nil {
			tmplCount = len(tracker.Snapshot())
		}

		fmt.Printf("%-18s  %-12d  %-13d  %-12d  %-12d  %-12s  %-10d  %d\n",
			b.HourPath, b.UDPPackets, b.UDPBytes, b.HTTPRequests, b.SyslogMessages, storage.FormatBytes(b.DiskBytes), tmplCount, len(b.Exporters))
		if *verbose {
			if len(b.Exporters) > 0 {
				fmt.Printf("   └─ Exporters: %s\n", strings.Join(b.Exporters, ", "))
			} else {
				fmt.Printf("   └─ Exporters: none\n")
			}
		}
	}
	fmt.Println(strings.Repeat("-", 110))
	fmt.Printf("Total: %d traffic pkts (%s payload) | %d HTTP reqs | %d Syslog msgs | Disk: %s across %d buckets\n",
		totalUDPPkts, storage.FormatBytes(int64(totalUDPBytes)), totalHTTPReqs, totalSyslogMsgs, storage.FormatBytes(totalDiskBytes), len(buckets))
}

func runPrune(args []string) {
	fs := flag.NewFlagSet("prune", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Println(`Usage:
  rb-spooler prune [options]

Manually execute retention cleanup against configured max_age, max_storage_mb, and min_free_disk_pct.

Options:
  --config <path>       Path to configuration file (default: /etc/redborder-spooler/config.yaml)

Examples:
  rb-spooler prune`)
	}
	if len(args) > 0 && args[0] == "help" {
		fs.Usage()
		return
	}
	configPath := fs.String("config", DefaultConfigPath, "Path to configuration file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(1)
	}

	cfg, err := loadConfigWithFallback(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open hourly store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	now := time.Now().UTC()
	pruned, err := store.Prune(now)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Prune failed: %v\n", err)
		os.Exit(1)
	}

	if len(pruned) == 0 {
		fmt.Println("Retention check complete: no expired buckets to prune.")
	} else {
		fmt.Printf("Successfully pruned %d expired buckets:\n", len(pruned))
		for _, p := range pruned {
			fmt.Printf("  - %s\n", p)
		}
	}
}

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Println(`Usage:
  rb-spooler status [options]

Display spooler status, available time ranges, packet/request metrics, and retention policies.

Options:
  --config <path>       Path to configuration file (default: /etc/redborder-spooler/config.yaml)

Examples:
  rb-spooler status`)
	}
	if len(args) > 0 && args[0] == "help" {
		fs.Usage()
		return
	}
	configPath := fs.String("config", DefaultConfigPath, "Path to configuration file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(1)
	}

	cfg, err := loadConfigWithFallback(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	store, err := storage.NewHourlyStore(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open hourly store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	buckets, _ := store.ListBuckets()
	var totalUDPPkts, totalUDPBytes, totalHTTPReqs, totalSyslogMsgs uint64
	var totalDiskBytes int64
	for _, b := range buckets {
		totalUDPPkts += b.UDPPackets
		totalUDPBytes += b.UDPBytes
		totalHTTPReqs += b.HTTPRequests
		totalSyslogMsgs += b.SyslogMessages
		totalDiskBytes += b.DiskBytes
	}

	firstTime, hasFirst := store.FirstAvailableTime()
	lastTime, hasLast := store.LastAvailableTime()

	// Try querying control socket for live daemon status
	var liveStatus *control.Response
	if cfg.ControlSocket != "" {
		client := control.NewClient(cfg.ControlSocket)
		if resp, err := client.Send("status", ""); err == nil && resp.Success {
			liveStatus = resp
		}
	}

	fmt.Println("redborder-spooler Status:")
	if liveStatus != nil {
		fmt.Printf("  Daemon State:          RUNNING (pid: %d, uptime: %s)\n", liveStatus.PID, formatDuration(time.Duration(liveStatus.UptimeSec)*time.Second))
		fmt.Printf("  Control Socket:        %s\n", cfg.ControlSocket)
		fmt.Println("  Recording Listeners:")
		for _, l := range liveStatus.Listeners {
			stateStr := "ENABLED "
			if !l.Enabled {
				stateStr = "DISABLED"
			}
			fmt.Printf("    %-12s [%s]  %-26s (%s)\n", strings.ToUpper(l.Name)+":", stateStr, l.ListenAddr, l.Description)
		}
	} else if cfg.ControlSocket != "" {
		fmt.Printf("  Daemon State:          STOPPED (not connected to %s)\n", cfg.ControlSocket)
	}
	fmt.Printf("  Spool Directory:       %s\n", cfg.SpoolDir)
	fmt.Printf("  Stored Hourly Buckets: %d\n", len(buckets))
	if hasFirst {
		fmt.Printf("  First Available Data:  %s UTC\n", firstTime.Format("2006-01-02 15:04:05"))
	} else {
		fmt.Printf("  First Available Data:  None (spool empty)\n")
	}
	if hasLast {
		fmt.Printf("  Latest Available Data: %s UTC\n", lastTime.Format("2006-01-02 15:04:05"))
	}
	fmt.Printf("  Total Traffic Packets: %d (%d bytes, %s payload)\n", totalUDPPkts, totalUDPBytes, storage.FormatBytes(int64(totalUDPBytes)))
	fmt.Printf("  Total HTTP Requests:   %d\n", totalHTTPReqs)
	fmt.Printf("  Total Syslog Messages: %d\n", totalSyslogMsgs)
	fmt.Printf("  Total Disk Usage:      %s (%d bytes)\n", storage.FormatBytes(totalDiskBytes), totalDiskBytes)
	fmt.Printf("  Retention Max Age:     %s\n", cfg.Retention.MaxAge)
	fmt.Printf("  Retention Max Storage: %d MB\n", cfg.Retention.MaxStorageMB)
	fmt.Printf("  Min Free Disk %%:       %.1f%%\n", cfg.Retention.MinFreeDiskPct)
}

func runCtl(args []string) {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		printCtlUsage()
		return
	}

	cmd := strings.ToLower(args[0])
	if cmd != "status" && cmd != "enable" && cmd != "disable" {
		fmt.Fprintf(os.Stderr, "Error: unknown ctl command '%s' (expected: 'status', 'enable', 'disable')\n\n", cmd)
		printCtlUsage()
		os.Exit(1)
	}

	var target string
	fsArgs := args[1:]
	if cmd == "enable" || cmd == "disable" {
		if len(args) < 2 || strings.HasPrefix(args[1], "-") {
			fmt.Fprintf(os.Stderr, "Error: %s requires a target ('syslog_tcp', 'http', 'traffic', 'syslog_udp', or 'all')\n\n", cmd)
			printCtlUsage()
			os.Exit(1)
		}
		target = args[1]
		fsArgs = args[2:]
	}

	fs := flag.NewFlagSet("ctl", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	configPath := fs.String("config", DefaultConfigPath, "Path to configuration file")
	socketPath := fs.String("socket", "", "Path to control socket (overrides config)")
	if err := fs.Parse(fsArgs); err != nil {
		os.Exit(1)
	}

	sock := *socketPath
	if sock == "" {
		cfg, err := loadConfigWithFallback(*configPath)
		if err == nil && cfg.ControlSocket != "" {
			sock = cfg.ControlSocket
		} else {
			sock = "/run/redborder-spooler/control.sock"
		}
	}

	client := control.NewClient(sock)
	resp, err := client.Send(cmd, target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Command failed: %s\n", resp.Message)
		os.Exit(1)
	}

	if cmd == "enable" || cmd == "disable" {
		fmt.Printf("Success: %s\n\n", resp.Message)
	}

	printLiveStatus(resp)
}

func printCtlUsage() {
	fmt.Println(`Usage:
  rb-spooler ctl <status | enable <target> | disable <target>> [options]

Commands:
  status                Display daemon runtime state and recording listeners
  enable <target>       Dynamically enable/resume a recording component
  disable <target>      Dynamically disable/pause a recording component

Targets:
  syslog_tcp            TCP Syslog spooler (binds or releases listen port)
  http                  HTTP event ingest spooler
  traffic               Flow telemetry packet capture (NetFlow / sFlow)
  syslog_udp            UDP port 514 syslog capture
  all                   All recording listeners/spoolers

Options:
  --config <path>       Path to configuration file
  --socket <path>       Path to control socket (default: /run/redborder-spooler/control.sock)

Examples:
  rb-spooler ctl status
  rb-spooler ctl enable syslog_tcp
  rb-spooler ctl disable syslog_tcp
  rb-spooler ctl enable all
  rb-spooler ctl disable all`)
}

func printLiveStatus(resp *control.Response) {
	fmt.Println("redborder-spooler Daemon Status:")
	fmt.Printf("  State:       RUNNING (pid: %d, uptime: %s)\n", resp.PID, formatDuration(time.Duration(resp.UptimeSec)*time.Second))
	fmt.Println("\n  Recording Listeners:")
	for _, l := range resp.Listeners {
		stateStr := "ENABLED "
		if !l.Enabled {
			stateStr = "DISABLED"
		}
		fmt.Printf("    %-12s [%s]  %-26s (%s)\n", strings.ToUpper(l.Name)+":", stateStr, l.ListenAddr, l.Description)
	}
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func runExport(args []string) {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Println(`Usage:
  rb-spooler export --from "<timestamp>" [options]

Export recorded traffic buckets to a compressed .tar.gz bundle.

Options:
  --from <time>         Start UTC time (required, format: 'YYYY-MM-DD HH:MM')
  --to <time>           End UTC time (optional, defaults to now)
  --output <path>       Output tar.gz archive file path (default: auto-generated based on range)
  --spool-dir <path>    Override spool directory location (defaults to config value)
  --config <path>       Path to configuration file (default: /etc/redborder-spooler/config.yaml)

Examples:
  rb-spooler export --from "2026-11-24 10:00" --to "2026-11-24 12:00" --output /tmp/traffic.tar.gz`)
	}
	if len(args) > 0 && args[0] == "help" {
		fs.Usage()
		return
	}
	configPath := fs.String("config", DefaultConfigPath, "Path to configuration file")
	spoolDir := fs.String("spool-dir", "", "Spool directory override (defaults to config value)")
	fromStr := fs.String("from", "", "Start UTC time (e.g. '2026-11-24 10:00')")
	toStr := fs.String("to", "", "End UTC time (optional, defaults to now)")
	outputFile := fs.String("output", "", "Output tar.gz file path")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(1)
	}

	if *fromStr == "" {
		fmt.Fprintln(os.Stderr, "Error: --from argument is required")
		fs.Usage()
		os.Exit(1)
	}

	fromTime, err := parseTimeArg(*fromStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid --from time: %v\n", err)
		os.Exit(1)
	}

	toTime := time.Now().UTC()
	if *toStr != "" {
		t, err := parseTimeArg(*toStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid --to time: %v\n", err)
			os.Exit(1)
		}
		toTime = t
	}

	if fromTime.After(toTime) {
		fmt.Fprintf(os.Stderr, "Error: --from time (%s) cannot be after --to time (%s)\n",
			fromTime.Format("2006-01-02 15:04:05"), toTime.Format("2006-01-02 15:04:05"))
		os.Exit(1)
	}

	cfg, err := loadConfigWithFallback(*configPath)
	if err != nil && *spoolDir == "" {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}
	activeSpool := ""
	if cfg != nil {
		activeSpool = cfg.SpoolDir
	}
	if *spoolDir != "" {
		activeSpool = *spoolDir
	}
	if activeSpool == "" {
		activeSpool = resolveSpoolDir("")
	}

	outPath := *outputFile
	if outPath == "" {
		outPath = fmt.Sprintf("redborder-traffic-%s-to-%s.tar.gz",
			fromTime.Format("20060102_1504"), toTime.Format("20060102_1504"))
	}

	tmpOut := outPath + fmt.Sprintf(".tmp.%d", time.Now().UnixNano())
	f, err := os.Create(tmpOut)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output file %s: %v\n", tmpOut, err)
		os.Exit(1)
	}

	fmt.Printf("Exporting traffic from %s (Range: %s UTC to %s UTC)...\n",
		activeSpool, fromTime.Format("2006-01-02 15:04:05"), toTime.Format("2006-01-02 15:04:05"))

	stats, err := storage.ExportArchive(activeSpool, fromTime, toTime, f)
	_ = f.Close()
	if err != nil {
		_ = os.Remove(tmpOut)
		fmt.Fprintf(os.Stderr, "Export failed: %v\n", err)
		os.Exit(1)
	}

	if err := os.Rename(tmpOut, outPath); err != nil {
		_ = os.Remove(tmpOut)
		fmt.Fprintf(os.Stderr, "Failed to finalize archive file: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Export complete:")
	fmt.Printf("  Archive File:     %s\n", outPath)
	fmt.Printf("  Buckets Exported: %d\n", stats.BucketsExported)
	fmt.Printf("  Files Exported:   %d\n", stats.FilesExported)
	fmt.Printf("  Total Size:       %d bytes (uncompressed)\n", stats.TotalBytes)
}

func runImport(args []string) {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Println(`Usage:
  rb-spooler import --input <archive.tar.gz> [options]

Import traffic buckets from an archive bundle into the local spool directory.

Options:
  --input <path>        Path to .tar.gz archive file to import (required)
  --force               Overwrite existing hourly bucket directories in spool (default: false)
  --spool-dir <path>    Override target spool directory location (defaults to config value)
  --config <path>       Path to configuration file (default: /etc/redborder-spooler/config.yaml)

Examples:
  rb-spooler import --input /tmp/traffic.tar.gz
  rb-spooler import --input /tmp/traffic.tar.gz --force`)
	}
	if len(args) > 0 && args[0] == "help" {
		fs.Usage()
		return
	}
	configPath := fs.String("config", DefaultConfigPath, "Path to configuration file")
	spoolDir := fs.String("spool-dir", "", "Spool directory override (defaults to config value)")
	inputFile := fs.String("input", "", "Path to .tar.gz archive file to import")
	force := fs.Bool("force", false, "Overwrite existing hourly files in spool")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(1)
	}

	if *inputFile == "" {
		fmt.Fprintln(os.Stderr, "Error: --input argument is required")
		fs.Usage()
		os.Exit(1)
	}

	cfg, err := loadConfigWithFallback(*configPath)
	if err != nil && *spoolDir == "" {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}
	activeSpool := ""
	if cfg != nil {
		activeSpool = cfg.SpoolDir
	}
	if *spoolDir != "" {
		activeSpool = *spoolDir
	}
	if activeSpool == "" {
		activeSpool = resolveSpoolDir("")
	}

	f, err := os.Open(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open input file %s: %v\n", *inputFile, err)
		os.Exit(1)
	}
	defer f.Close()

	fmt.Printf("Importing archive %s into spool %s...\n", *inputFile, activeSpool)

	stats, err := storage.ImportArchive(activeSpool, f, *force)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Import failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Import complete:")
	fmt.Printf("  Target Spool:     %s\n", activeSpool)
	fmt.Printf("  Buckets Imported: %d\n", stats.BucketsImported)
	fmt.Printf("  Files Imported:   %d\n", stats.FilesImported)
	fmt.Printf("  Total Unpacked:   %d bytes\n", stats.TotalBytes)
}

func runPCAP(args []string) {
	fs := flag.NewFlagSet("pcap", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Println(`Usage:
  rb-spooler pcap --from "<timestamp>" [options]

Convert recorded traffic datagrams to a standard PCAP file for inspection with Wireshark/tcpdump.

Options:
  --from <time>         Start UTC time (required, format: 'YYYY-MM-DD HH:MM')
  --to <time>           End UTC time (optional, defaults to now)
  --output <path>       Output .pcap file path (default: auto-generated based on range)
  --spool-dir <path>    Override spool directory location (defaults to config value)
  --config <path>       Path to configuration file (default: /etc/redborder-spooler/config.yaml)

Examples:
  rb-spooler pcap --from "2026-11-24 10:00" --to "2026-11-24 11:00" --output /tmp/dump.pcap`)
	}
	if len(args) > 0 && args[0] == "help" {
		fs.Usage()
		return
	}
	configPath := fs.String("config", DefaultConfigPath, "Path to configuration file")
	spoolDir := fs.String("spool-dir", "", "Spool directory override (defaults to config value)")
	fromStr := fs.String("from", "", "Start UTC time (e.g. '2026-11-24 10:00')")
	toStr := fs.String("to", "", "End UTC time (optional, defaults to now)")
	outputFile := fs.String("output", "", "Output .pcap file path")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(1)
	}

	if *fromStr == "" {
		fmt.Fprintln(os.Stderr, "Error: --from argument is required")
		fs.Usage()
		os.Exit(1)
	}

	fromTime, err := parseTimeArg(*fromStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid --from time: %v\n", err)
		os.Exit(1)
	}

	toTime := time.Now().UTC()
	if *toStr != "" {
		t, err := parseTimeArg(*toStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid --to time: %v\n", err)
			os.Exit(1)
		}
		toTime = t
	}

	if fromTime.After(toTime) {
		fmt.Fprintf(os.Stderr, "Error: --from time (%s) cannot be after --to time (%s)\n",
			fromTime.Format("2006-01-02 15:04:05"), toTime.Format("2006-01-02 15:04:05"))
		os.Exit(1)
	}

	cfg, err := loadConfigWithFallback(*configPath)
	if err != nil && *spoolDir == "" {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}
	activeSpool := ""
	if cfg != nil {
		activeSpool = cfg.SpoolDir
	}
	if *spoolDir != "" {
		activeSpool = *spoolDir
	}
	if activeSpool == "" {
		activeSpool = resolveSpoolDir("")
	}

	outPath := *outputFile
	if outPath == "" {
		outPath = fmt.Sprintf("traffic-%s-to-%s.pcap",
			fromTime.Format("20060102_1504"), toTime.Format("20060102_1504"))
	}

	tmpOut := outPath + fmt.Sprintf(".tmp.%d", time.Now().UnixNano())
	f, err := os.Create(tmpOut)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output file %s: %v\n", tmpOut, err)
		os.Exit(1)
	}

	fmt.Printf("Exporting PCAP from %s (Range: %s UTC to %s UTC)...\n",
		activeSpool, fromTime.Format("2006-01-02 15:04:05"), toTime.Format("2006-01-02 15:04:05"))

	stats, err := storage.ExportPCAP(activeSpool, fromTime, toTime, f)
	_ = f.Close()
	if err != nil {
		_ = os.Remove(tmpOut)
		fmt.Fprintf(os.Stderr, "PCAP export failed: %v\n", err)
		os.Exit(1)
	}

	if err := os.Rename(tmpOut, outPath); err != nil {
		_ = os.Remove(tmpOut)
		fmt.Fprintf(os.Stderr, "Failed to finalize PCAP file: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("PCAP export complete:")
	fmt.Printf("  PCAP File:        %s\n", outPath)
	fmt.Printf("  Packets Exported: %d\n", stats.PacketsExported)
	fmt.Printf("  Bytes Exported:   %d bytes\n", stats.BytesExported)
}
