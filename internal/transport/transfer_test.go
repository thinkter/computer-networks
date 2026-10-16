package transport

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"rudp-go/internal/metrics"
	"rudp-go/internal/protocol"
)

func TestTransferMatrix(t *testing.T) {
	for _, size := range []int{0, 1024, 10 * 1024 * 1024} {
		for _, loss := range []float64{0, .05, .1, .2} {
			t.Run(fmt.Sprintf("bytes=%d/loss=%.2f", size, loss), func(t *testing.T) {
				exerciseTransfer(t, size, loss, 32)
			})
		}
	}
	for _, window := range []int{1, 8, 128} {
		t.Run(fmt.Sprintf("window=%d", window), func(t *testing.T) { exerciseTransfer(t, 64*1024, .2, window) })
	}
}

func exerciseTransfer(t *testing.T, size int, loss float64, window int) {
	t.Helper()
	source := t.TempDir()
	destination := filepath.Join(t.TempDir(), "new", "downloads")
	data := make([]byte, size)
	rng := rand.NewChaCha8([32]byte{4, 2})
	if _, err := rng.Read(data); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(source, "binary.dat")
	if err := os.WriteFile(input, data, 0600); err != nil {
		t.Fatal(err)
	}
	receiver := testListen(t)
	sender, err := net.DialUDP("udp", nil, receiver.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg := DefaultConfig()
	cfg.Window = window
	cfg.Loss = loss
	cfg.Seed = 7
	cfg.RTO = 5 * time.Millisecond
	cfg.RetryLimit = 100
	rxCfg := cfg
	rxCfg.Window = 128
	rxCfg.Seed = 42
	rxCfg.OutputDir = destination
	type result struct {
		stats metrics.Stats
		err   error
	}
	done := make(chan result, 1)
	go func() { s, e := Receive(ctx, receiver, rxCfg); done <- result{s, e} }()
	stats, err := Send(ctx, sender, input, cfg)
	cancel()
	got := <-done
	if err != nil {
		t.Fatalf("sender: %v; receiver: %v", err, got.err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	output, err := os.ReadFile(filepath.Join(destination, "binary.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output, data) {
		t.Fatal("received bytes differ")
	}
	if !stats.Verified || !got.stats.Verified || stats.SHA256 != got.stats.SHA256 {
		t.Fatal("checksum confirmation missing")
	}
	if want := uint64((size + protocol.MaxPayload - 1) / protocol.MaxPayload); stats.OriginalData != want || got.stats.OriginalData != want {
		t.Fatalf("incorrect unique DATA counts: sender=%d receiver=%d want=%d", stats.OriginalData, got.stats.OriginalData, want)
	}
	entries, err := os.ReadDir(destination)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".part") {
			t.Fatalf("partial retained: %s", entry.Name())
		}
	}
}

func testListen(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestSenderSelectiveRetransmissionAndFinRetry(t *testing.T) {
	server := testListen(t)
	input := filepath.Join(t.TempDir(), "four.bin")
	if err := os.WriteFile(input, make([]byte, 4*protocol.MaxPayload), 0600); err != nil {
		t.Fatal(err)
	}
	sender, err := net.DialUDP("udp", nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	done := make(chan error, 1)
	go func() {
		counts := make(map[uint32]int)
		var buffer [protocol.MaxDatagram + 1]byte
		fins := 0
		send := func(p protocol.Packet, peer *net.UDPAddr) error {
			b, e := protocol.Encode(nil, p)
			if e != nil {
				return e
			}
			_, e = server.WriteToUDP(b, peer)
			return e
		}
		for {
			_ = server.SetReadDeadline(time.Now().Add(3 * time.Second))
			n, peer, e := server.ReadFromUDP(buffer[:])
			if e != nil {
				done <- e
				return
			}
			p, e := protocol.Decode(buffer[:n])
			if e != nil {
				done <- e
				return
			}
			ack := protocol.Packet{Type: protocol.Ack, Session: p.Session}
			switch p.Type {
			case protocol.Start:
				e = send(ack, peer)
			case protocol.Data:
				counts[p.Seq]++
				if len(counts) == 4 && counts[2] == 1 {
					for _, seq := range []uint32{999, 3, 4, 1, 1} {
						ack.Ack = seq
						if e = send(ack, peer); e != nil {
							break
						}
					}
				}
				if p.Seq == 2 && counts[2] == 2 {
					ack.Ack = 2
					e = send(ack, peer)
				}
			case protocol.Fin:
				fins++
				if fins == 1 {
					continue
				}
				if fins != 2 || counts[1] != 1 || counts[2] != 2 || counts[3] != 1 || counts[4] != 1 {
					done <- fmt.Errorf("unexpected retry counts DATA=%v FIN=%d", counts, fins)
					return
				}
				e = send(protocol.Packet{Type: protocol.FinAck, Session: p.Session, Ack: p.Seq}, peer)
				done <- e
				return
			default:
				done <- fmt.Errorf("unexpected packet %v", p.Type)
				return
			}
			if e != nil {
				done <- e
				return
			}
		}
	}()
	cfg := DefaultConfig()
	cfg.Window = 4
	cfg.RTO = 80 * time.Millisecond
	cfg.RetryLimit = 3
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	stats, err := Send(ctx, sender, input, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if stats.DataRetransmissions != 1 || stats.Retransmissions != 2 {
		t.Fatalf("retry stats %+v", stats)
	}
}

func TestSenderRetryBudget(t *testing.T) {
	server := testListen(t)
	input := filepath.Join(t.TempDir(), "empty.bin")
	if err := os.WriteFile(input, nil, 0600); err != nil {
		t.Fatal(err)
	}
	sender, err := net.DialUDP("udp", nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	cfg := DefaultConfig()
	cfg.RTO = 10 * time.Millisecond
	cfg.RetryLimit = 2
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stats, err := Send(ctx, sender, input, cfg)
	if err == nil || !strings.Contains(err.Error(), "retry limit") {
		t.Fatalf("expected retry failure, got %v", err)
	}
	if stats.Retransmissions != 2 || stats.Verified {
		t.Fatalf("budget not enforced: %+v", stats)
	}
	starts := 0
	for {
		_ = server.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		var buffer [protocol.MaxDatagram + 1]byte
		n, _, err := server.ReadFromUDP(buffer[:])
		if err != nil {
			break
		}
		p, err := protocol.Decode(buffer[:n])
		if err != nil {
			t.Fatal(err)
		}
		if p.Type == protocol.Start {
			starts++
		}
	}
	if starts != 3 {
		t.Fatalf("sent %d STARTs, want initial plus 2 retries", starts)
	}
}
