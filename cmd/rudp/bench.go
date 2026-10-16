package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"rudp-go/internal/metrics"
	"rudp-go/internal/transport"
)

type trial struct {
	Repeat       int           `json:"repeat"`
	SenderSeed   int64         `json:"sender_seed"`
	ReceiverSeed int64         `json:"receiver_seed"`
	Sender       metrics.Stats `json:"sender"`
	Receiver     metrics.Stats `json:"receiver"`
}
type comparison struct {
	Window                int     `json:"window"`
	Loss                  float64 `json:"loss"`
	MedianSeconds         float64 `json:"median_seconds"`
	MedianGoodputBPS      float64 `json:"median_goodput_bps"`
	MedianRetransmissions float64 `json:"median_retransmissions"`
}
type benchmarkReport struct {
	Bytes       int64        `json:"bytes"`
	RTO         string       `json:"rto"`
	Repeats     int          `json:"repeats"`
	Trials      []trial      `json:"trials"`
	Comparisons []comparison `json:"comparisons"`
}

func benchmark(ctx context.Context, args []string, out, errOut io.Writer) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(errOut)
	file := fs.String("file", "", "input file; otherwise generate seeded binary fixture")
	size := fs.Int64("size", 10*1024*1024, "generated fixture bytes")
	windows := fs.String("windows", "1,8,32", "comma-separated window sizes")
	losses := fs.String("losses", "0,0.05,0.10,0.20", "comma-separated outgoing loss rates (both endpoints)")
	repeats := fs.Int("repeats", 3, "trials per setting (1..100)")
	seed := fs.Int64("seed", 7, "base sender seed; receiver uses seed+100000")
	rto := fs.Duration("rto", 500*time.Millisecond, "fixed retransmission timeout")
	retries := fs.Int("retries", 15, "retransmissions per packet")
	timeout := fs.Duration("timeout", 0, "whole benchmark deadline (0 disables)")
	jsonOutput := fs.Bool("json", false, "emit raw trials and medians as JSON")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 || *repeats < 1 || *repeats > 100 || *size < 0 || *timeout < 0 {
		return errors.New("invalid positional arguments, repeats, size, or timeout")
	}
	ws, err := parseNumbers(*windows, func(v float64) bool { return v >= 1 && v <= 128 && math.Trunc(v) == v })
	if err != nil {
		return fmt.Errorf("windows: %w", err)
	}
	ls, err := parseNumbers(*losses, func(v float64) bool { return v >= 0 && v <= .3 })
	if err != nil {
		return fmt.Errorf("losses: %w", err)
	}
	if *rto < time.Millisecond || *rto > time.Minute || *retries < 0 || *retries > 1000 {
		return errors.New("invalid rto or retries")
	}
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	root, err := os.MkdirTemp("", "rudp-bench-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	if *file == "" {
		*file = filepath.Join(root, "fixture.bin")
		f, e := os.Create(*file)
		if e != nil {
			return e
		}
		rng := rand.NewChaCha8([32]byte{1, 3, 3, 7})
		_, e = io.CopyN(f, rng, *size)
		closeErr := f.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
	}
	info, err := os.Stat(*file)
	if err != nil {
		return err
	}
	report := benchmarkReport{Bytes: info.Size(), RTO: rto.String(), Repeats: *repeats}
	for _, loss := range ls {
		for _, window := range ws {
			var durations, goodputs, retrans []float64
			for repeat := range *repeats {
				if err := ctx.Err(); err != nil {
					return err
				}
				cfg := transport.DefaultConfig()
				cfg.Window = int(window)
				cfg.Loss = loss
				cfg.RTO = *rto
				cfg.RetryLimit = *retries
				cfg.Seed = *seed + int64(repeat)
				cfg.IdleTimeout = idleBudget(*rto, *retries)
				rxCfg := cfg
				rxCfg.Window = 128
				rxCfg.Seed = cfg.Seed + 100000
				rxCfg.OutputDir = filepath.Join(root, fmt.Sprintf("trial-%d", len(report.Trials)))
				senderStats, receiverStats, e := runTrial(ctx, *file, cfg, rxCfg)
				if e != nil {
					return fmt.Errorf("window %d loss %.2f repeat %d: %w", int(window), loss, repeat+1, e)
				}
				report.Trials = append(report.Trials, trial{Repeat: repeat + 1, SenderSeed: cfg.Seed, ReceiverSeed: rxCfg.Seed, Sender: senderStats, Receiver: receiverStats})
				durations = append(durations, senderStats.DurationSeconds)
				goodputs = append(goodputs, senderStats.GoodputBPS)
				retrans = append(retrans, float64(senderStats.Retransmissions))
				fmt.Fprintf(errOut, "window=%d loss=%.2f repeat=%d verified=%t elapsed=%.6fs retransmissions=%d\n", int(window), loss, repeat+1, senderStats.Verified, senderStats.DurationSeconds, senderStats.Retransmissions)
			}
			report.Comparisons = append(report.Comparisons, comparison{Window: int(window), Loss: loss, MedianSeconds: median(durations), MedianGoodputBPS: median(goodputs), MedianRetransmissions: median(retrans)})
		}
	}
	if *jsonOutput {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	fmt.Fprintf(out, "%d bytes, RTO %s, %d repeats; both endpoints inject loss\n", report.Bytes, report.RTO, report.Repeats)
	fmt.Fprintln(out, "Window\tLoss\tMedian seconds\tMedian MiB/s\tMedian retransmissions")
	for _, row := range report.Comparisons {
		fmt.Fprintf(out, "%d\t%.0f%%\t%.6f\t%.6f\t%.1f\n", row.Window, row.Loss*100, row.MedianSeconds, row.MedianGoodputBPS/(1024*1024), row.MedianRetransmissions)
	}
	return nil
}

func runTrial(ctx context.Context, file string, cfg, rxCfg transport.Config) (metrics.Stats, metrics.Stats, error) {
	var empty metrics.Stats
	receiver, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return empty, empty, err
	}
	defer receiver.Close()
	sender, err := net.DialUDP("udp", nil, receiver.LocalAddr().(*net.UDPAddr))
	if err != nil {
		return empty, empty, err
	}
	defer sender.Close()
	trialCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		stats metrics.Stats
		err   error
	}
	done := make(chan result, 1)
	go func() { stats, e := transport.Receive(trialCtx, receiver, rxCfg); done <- result{stats, e} }()
	sent, sendErr := transport.Send(trialCtx, sender, file, cfg)
	// FINACK was received before success; retaining linger state is unnecessary now.
	cancel()
	received := <-done
	if sendErr != nil {
		return sent, received.stats, sendErr
	}
	if received.err != nil {
		return sent, received.stats, received.err
	}
	if !sent.Verified || !received.stats.Verified || sent.SHA256 != received.stats.SHA256 {
		return sent, received.stats, errors.New("benchmark checksum verification failed")
	}
	return sent, received.stats, nil
}

func parseNumbers(s string, valid func(float64) bool) ([]float64, error) {
	parts := strings.Split(s, ",")
	if len(parts) > 128 {
		return nil, errors.New("at most 128 settings allowed")
	}
	values := make([]float64, 0, len(parts))
	for _, part := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || math.IsNaN(v) || !valid(v) {
			return nil, fmt.Errorf("invalid setting %q", part)
		}
		if slices.Contains(values, v) {
			return nil, fmt.Errorf("duplicate setting %q", part)
		}
		values = append(values, v)
	}
	return values, nil
}
func median(values []float64) float64 {
	slices.Sort(values)
	n := len(values)
	if n%2 == 1 {
		return values[n/2]
	}
	return (values[n/2-1] + values[n/2]) / 2
}
