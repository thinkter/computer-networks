package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rudp-go/internal/metrics"
	"rudp-go/internal/transport"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "rudp:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: rudp send|receive|bench [options] (use <command> --help)")
	}
	if args[0] == "bench" {
		return benchmark(ctx, args[1:], out, errOut)
	}
	if args[0] != "send" && args[0] != "receive" {
		return fmt.Errorf("unknown command %q; use send, receive, or bench", args[0])
	}
	receive := args[0] == "receive"
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(errOut)
	cfg := transport.DefaultConfig()
	window := 32
	fs.IntVar(&cfg.Window, "window", window, "sender window / receiver buffer capacity (1..128)")
	fs.DurationVar(&cfg.RTO, "rto", cfg.RTO, "fixed retransmission timeout (1ms..1m)")
	fs.IntVar(&cfg.RetryLimit, "retries", cfg.RetryLimit, "retransmissions per packet (0..1000)")
	fs.Float64Var(&cfg.Loss, "loss", 0, "outgoing datagram drop probability (0..0.3)")
	fs.Int64Var(&cfg.Seed, "seed", 1, "outgoing loss PRNG seed")
	jsonOutput := fs.Bool("json", false, "print measured statistics as JSON")
	timeout := fs.Duration("timeout", 0, "whole-command deadline (0 disables)")
	var address, file string
	if receive {
		fs.StringVar(&address, "listen", "127.0.0.1:9000", "UDP listen address")
		fs.StringVar(&cfg.OutputDir, "out", "downloads", "output directory")
		fs.BoolVar(&cfg.Overwrite, "overwrite", false, "allow atomic replacement of an existing destination")
		fs.DurationVar(&cfg.IdleTimeout, "idle", cfg.IdleTimeout, "inactivity timeout, including waiting for START")
		fs.DurationVar(&cfg.Linger, "linger", cfg.Linger, "completed-session FINACK retention (at least 5s)")
	} else {
		fs.StringVar(&address, "to", "127.0.0.1:9000", "receiver UDP address")
		fs.StringVar(&file, "file", "", "regular file to send (required)")
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if cfg.Window <= 0 || cfg.RTO <= 0 || cfg.IdleTimeout <= 0 || cfg.Linger <= 0 {
		return errors.New("window, rto, idle, and linger must be positive")
	}
	if *timeout < 0 {
		return errors.New("timeout must not be negative")
	}
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return err
	}
	var conn *net.UDPConn
	if receive {
		conn, err = net.ListenUDP("udp", addr)
	} else {
		if file == "" {
			return errors.New("--file is required")
		}
		conn, err = net.DialUDP("udp", nil, addr)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	var stats metrics.Stats
	if receive {
		fmt.Fprintf(errOut, "Listening on %s; output %s\n", conn.LocalAddr(), cfg.OutputDir)
		stats, err = transport.Receive(ctx, conn, cfg)
	} else {
		stats, err = transport.Send(ctx, conn, file, cfg)
	}
	if err != nil {
		return err
	}
	if *jsonOutput {
		return json.NewEncoder(out).Encode(stats)
	}
	printStats(out, stats)
	return nil
}

func printStats(out io.Writer, s metrics.Stats) {
	fmt.Fprintf(out, "Transfer completed\nFile: %s (%d bytes)\nWindow: %d  Outgoing loss: %.0f%%\nElapsed: %.6fs  Goodput: %.6f MiB/s\nDatagrams attempted: %d  Simulated drops: %d\nOriginal DATA: %d  Retransmissions: %d (DATA: %d)\nSHA-256: %s  MATCH: %t\n", s.File, s.Bytes, s.Window, s.Loss*100, s.DurationSeconds, s.GoodputBPS/(1024*1024), s.DatagramsSent, s.Dropped, s.OriginalData, s.Retransmissions, s.DataRetransmissions, s.SHA256, s.Verified)
}

// A local receive deadline must cover all retries of an inactive control packet.
func idleBudget(rto time.Duration, retries int) time.Duration {
	return max(15*time.Second, time.Duration(retries+2)*rto)
}
