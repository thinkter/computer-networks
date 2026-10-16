package transport

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"rudp-go/internal/metrics"
	"rudp-go/internal/network"
	"rudp-go/internal/protocol"
)

type reply struct {
	kind    protocol.Type
	ack     uint32
	message string
	err     error
}
type pending struct {
	seq     uint32
	data    [protocol.MaxPayload]byte
	size    int
	sent    time.Time
	retries int
	acked   bool
}

// Send transfers a regular file through a connected UDP socket. The caller owns conn.
func Send(ctx context.Context, conn *net.UDPConn, filename string, cfg Config) (stats metrics.Stats, err error) {
	cfg, err = cfg.normalized()
	if err != nil {
		return stats, err
	}
	if conn.RemoteAddr() == nil {
		return stats, errors.New("sender requires a connected UDP socket")
	}
	started := time.Now()
	defer func() { stats.Finish(started) }()
	file, err := os.Open(filename)
	if err != nil {
		return stats, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return stats, err
	}
	if !info.Mode().IsRegular() {
		return stats, errors.New("input must be a regular file")
	}
	hasher := sha256.New()
	if _, err = io.Copy(hasher, file); err != nil {
		return stats, err
	}
	meta := Metadata{Filename: filepath.Base(filename), Size: info.Size(), SHA256: hex.EncodeToString(hasher.Sum(nil)), ChunkSize: protocol.MaxPayload}
	if err = meta.validate(); err != nil {
		return stats, err
	}
	payload, err := json.Marshal(meta)
	if err != nil {
		return stats, err
	}
	stats.File, stats.Bytes, stats.Window, stats.Loss, stats.SHA256 = meta.Filename, meta.Size, cfg.Window, cfg.Loss, meta.SHA256
	var sessionBytes [8]byte
	for binary.BigEndian.Uint64(sessionBytes[:]) == 0 {
		if _, err = rand.Read(sessionBytes[:]); err != nil {
			return stats, err
		}
	}
	session := binary.BigEndian.Uint64(sessionBytes[:])
	link := network.New(conn, cfg.Loss, cfg.Seed, &stats)
	defer func() {
		if err != nil {
			_ = link.Send(protocol.Packet{Type: protocol.Error, Session: session, Payload: []byte("sender aborted")}, nil)
		}
	}()
	readerCtx, cancel := context.WithCancel(ctx)
	replies := make(chan reply, 256)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); readReplies(readerCtx, conn, session, replies) }()
	defer func() {
		cancel()
		_ = conn.SetReadDeadline(time.Now())
		wg.Wait()
		_ = conn.SetReadDeadline(time.Time{})
	}()
	interval := min(25*time.Millisecond, cfg.RTO)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	exchange := func(packet protocol.Packet, expected protocol.Type, ack uint32) error {
		if e := link.Send(packet, nil); e != nil {
			return e
		}
		sent, retries := time.Now(), 0
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case r := <-replies:
				if r.err != nil {
					return r.err
				}
				if r.kind == protocol.Error {
					return fmt.Errorf("receiver: %s", r.message)
				}
				if r.kind == expected && r.ack == ack {
					return nil
				}
			case now := <-ticker.C:
				if now.Sub(sent) < cfg.RTO {
					continue
				}
				if retries >= cfg.RetryLimit {
					return fmt.Errorf("retry limit reached for %v sequence %d", packet.Type, packet.Seq)
				}
				if e := link.Send(packet, nil); e != nil {
					return e
				}
				stats.Retransmissions++
				retries++
				sent = time.Now()
			}
		}
	}
	if err = exchange(protocol.Packet{Type: protocol.Start, Session: session, Payload: payload}, protocol.Ack, 0); err != nil {
		return stats, err
	}
	slots := make([]pending, cfg.Window)
	total := uint32(meta.chunks())
	base, next := uint32(1), uint32(1)
	sendData := func(p *pending) error {
		if e := link.Send(protocol.Packet{Type: protocol.Data, Session: session, Seq: p.seq, Payload: p.data[:p.size]}, nil); e != nil {
			return e
		}
		p.sent = time.Now()
		return nil
	}
	for base <= total {
		for next <= total && uint64(next) < uint64(base)+uint64(cfg.Window) {
			p := &slots[(next-1)%uint32(cfg.Window)]
			p.seq, p.size, p.retries, p.acked = next, meta.dataSize(next), 0, false
			if _, err = file.ReadAt(p.data[:p.size], int64(next-1)*int64(meta.ChunkSize)); err != nil {
				return stats, fmt.Errorf("reading input chunk %d: %w", next, err)
			}
			if err = sendData(p); err != nil {
				return stats, err
			}
			stats.OriginalData++
			next++
		}
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case r := <-replies:
			if r.err != nil {
				return stats, r.err
			}
			if r.kind == protocol.Error {
				return stats, fmt.Errorf("receiver: %s", r.message)
			}
			if r.kind != protocol.Ack || r.ack < base || r.ack >= next {
				continue
			}
			slots[(r.ack-1)%uint32(cfg.Window)].acked = true
			for base < next && slots[(base-1)%uint32(cfg.Window)].acked {
				base++
			}
		case now := <-ticker.C:
			for seq := base; seq < next; seq++ {
				p := &slots[(seq-1)%uint32(cfg.Window)]
				if p.acked || now.Sub(p.sent) < cfg.RTO {
					continue
				}
				if p.retries >= cfg.RetryLimit {
					return stats, fmt.Errorf("retry limit reached for DATA sequence %d", seq)
				}
				if err = sendData(p); err != nil {
					return stats, err
				}
				p.retries++
				stats.Retransmissions++
				stats.DataRetransmissions++
			}
		}
	}
	if err = exchange(protocol.Packet{Type: protocol.Fin, Session: session, Seq: total + 1}, protocol.FinAck, total+1); err != nil {
		return stats, err
	}
	stats.Verified = true
	return stats, nil
}

func readReplies(ctx context.Context, conn *net.UDPConn, session uint64, out chan<- reply) {
	var buffer [protocol.MaxDatagram + 1]byte
	deliver := func(r reply) bool {
		select {
		case out <- r:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		if ctx.Err() != nil {
			return
		}
		if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			deliver(reply{err: err})
			return
		}
		n, err := conn.Read(buffer[:])
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Timeout() {
				continue
			}
			if ctx.Err() == nil {
				deliver(reply{err: err})
			}
			return
		}
		p, err := protocol.Decode(buffer[:n])
		if err != nil || p.Session != session {
			continue
		}
		switch p.Type {
		case protocol.Ack, protocol.FinAck:
			if !deliver(reply{kind: p.Type, ack: p.Ack}) {
				return
			}
		case protocol.Error:
			if !deliver(reply{kind: p.Type, message: string(p.Payload)}) {
				return
			}
		}
	}
}
