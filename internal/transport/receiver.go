package transport

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"rudp-go/internal/metrics"
	"rudp-go/internal/network"
	"rudp-go/internal/protocol"
)

// receivePoll bounds each blocking read so cancellation and timers are observed promptly.
const receivePoll = 20 * time.Millisecond

// Receive accepts exactly one transfer on conn, which must be an unconnected UDP socket.
//
// The first valid START binds the transfer to its source address and session ID; every other
// datagram is ignored. Data is written in order to a temporary file in cfg.OutputDir while a
// fixed selective-repeat buffer (cfg.Window slots of MaxPayload bytes, allocated once) holds
// out-of-order payloads. The verified file is published atomically after FIN. Completed
// transfers keep re-acknowledging FIN for cfg.Linger, after which Receive returns. Cancelling
// ctx after completion returns the successful stats with a nil error; cancelling earlier removes
// the partial file and returns an error.
//
// Stats.Finish runs once at publication, so duration and goodput exclude the completed linger.
func Receive(ctx context.Context, conn *net.UDPConn, cfg Config) (metrics.Stats, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		return metrics.Stats{}, err
	}
	if conn == nil {
		return metrics.Stats{}, errors.New("nil UDP connection")
	}
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return metrics.Stats{}, fmt.Errorf("create output directory: %w", err)
	}
	var stats metrics.Stats
	stats.Window = cfg.Window
	stats.Loss = cfg.Loss
	r := &receiver{
		conn:     conn,
		cfg:      cfg,
		stats:    &stats,
		link:     network.New(conn, cfg.Loss, cfg.Seed, &stats),
		buf:      make([]byte, protocol.MaxDatagram+1),
		lastSeen: time.Now(),
	}
	defer conn.SetReadDeadline(time.Time{})
	if err := r.run(ctx); err != nil {
		r.discard()
		return stats, err
	}
	return stats, nil
}

type receiver struct {
	conn  *net.UDPConn
	cfg   Config
	link  *network.Link
	stats *metrics.Stats
	buf   []byte

	// Session state, set once by open. peerAP is compared against every datagram source;
	// peer is the *net.UDPAddr form required by Link.Send, built once per transfer.
	bound    bool
	peerAP   netip.AddrPort
	peer     *net.UDPAddr
	session  uint64
	meta     Metadata
	dest     string
	start    time.Time
	lastSeen time.Time

	// Output state.
	file    *os.File
	tmpPath string
	out     *bufio.Writer
	digest  hash.Hash
	written int64

	// Selective repeat state: next is the lowest sequence not yet written. Slot i holds the
	// buffered payload for a sequence seq with seq % Window == i, in the fixed region
	// slots[i*MaxPayload : i*MaxPayload+slotLen[i]]. present marks occupied slots.
	next    uint32
	slots   []byte
	slotLen []int
	present []bool

	completed bool
	lingerEnd time.Time
}

func (r *receiver) run(ctx context.Context) error {
	for {
		now := time.Now()
		if err := ctx.Err(); err != nil {
			if r.completed {
				return nil
			}
			return fmt.Errorf("receive canceled: %w", err)
		}
		deadline := now.Add(receivePoll)
		switch {
		case r.completed:
			if !now.Before(r.lingerEnd) {
				return nil
			}
			deadline = earlier(deadline, r.lingerEnd)
		default:
			idleEnd := r.lastSeen.Add(r.cfg.IdleTimeout)
			if !now.Before(idleEnd) {
				if !r.bound {
					return fmt.Errorf("idle timeout waiting for START after %v", r.cfg.IdleTimeout)
				}
				return r.abort("idle timeout", fmt.Errorf("no packets from peer for %v", r.cfg.IdleTimeout))
			}
			deadline = earlier(deadline, idleEnd)
		}
		if err := r.conn.SetReadDeadline(deadline); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}
		n, from, err := r.conn.ReadFromUDPAddrPort(r.buf)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			return fmt.Errorf("read datagram: %w", err)
		}
		if n > protocol.MaxDatagram {
			continue
		}
		p, err := protocol.Decode(r.buf[:n])
		if err != nil {
			continue
		}
		if err := r.handle(p, from); err != nil {
			return err
		}
	}
}

func (r *receiver) handle(p protocol.Packet, from netip.AddrPort) error {
	if !r.bound {
		if p.Type != protocol.Start {
			return nil
		}
		return r.open(p, from)
	}
	if p.Session != r.session || from != r.peerAP {
		return nil
	}
	r.lastSeen = time.Now()
	switch p.Type {
	case protocol.Start:
		return r.onStart(p)
	case protocol.Data:
		return r.onData(p)
	case protocol.Fin:
		return r.onFin(p)
	case protocol.Error:
		if r.completed {
			return nil
		}
		return fmt.Errorf("peer aborted transfer: %s", p.Payload)
	default:
		return nil
	}
}

// open validates the first START, creates the partial file, and binds the session.
func (r *receiver) open(p protocol.Packet, from netip.AddrPort) error {
	meta, err := decodeMetadata(p.Payload)
	if err != nil {
		// A malformed START from an unbound peer is not terminal: keep listening for a valid one.
		r.sendError(from, p.Session, "invalid START metadata")
		return nil
	}
	dest := filepath.Join(r.cfg.OutputDir, meta.Filename)
	if !r.cfg.Overwrite {
		if _, err := os.Lstat(dest); err == nil {
			r.sendError(from, p.Session, "output already exists")
			return fmt.Errorf("refusing to overwrite %s", dest)
		} else if !errors.Is(err, fs.ErrNotExist) {
			r.sendError(from, p.Session, "cannot inspect output")
			return fmt.Errorf("stat %s: %w", dest, err)
		}
	}
	f, err := os.CreateTemp(r.cfg.OutputDir, ".rudp-*.part")
	if err != nil {
		r.sendError(from, p.Session, "cannot create partial file")
		return fmt.Errorf("create partial file: %w", err)
	}
	r.bound = true
	r.peerAP = from
	r.peer = net.UDPAddrFromAddrPort(from)
	r.session = p.Session
	r.meta = meta
	r.dest = dest
	r.file = f
	r.tmpPath = f.Name()
	r.out = bufio.NewWriter(f)
	r.digest = sha256.New()
	r.next = 1
	r.slots = make([]byte, r.cfg.Window*protocol.MaxPayload)
	r.slotLen = make([]int, r.cfg.Window)
	r.present = make([]bool, r.cfg.Window)
	r.start = time.Now()
	r.lastSeen = r.start
	r.stats.File = meta.Filename
	r.ack(0)
	return nil
}

func (r *receiver) onStart(p protocol.Packet) error {
	meta, err := decodeMetadata(p.Payload)
	if err != nil {
		return r.abort("invalid START metadata", err)
	}
	if meta != r.meta {
		return r.abort("START metadata changed", errors.New("repeated START differs from original metadata"))
	}
	r.ack(0)
	return nil
}

func (r *receiver) onData(p protocol.Packet) error {
	seq := p.Seq
	if uint64(seq) > r.meta.chunks() || len(p.Payload) != r.meta.dataSize(seq) {
		return nil
	}
	if seq < r.next {
		// Already written: the original ACK was probably lost.
		r.ack(seq)
		return nil
	}
	if uint64(seq) >= uint64(r.next)+uint64(r.cfg.Window) {
		return nil
	}
	i := r.slot(seq)
	switch {
	case seq == r.next:
		if err := r.advance(p.Payload); err != nil {
			return r.abort("disk write failed", fmt.Errorf("write output: %w", err))
		}
		r.stats.OriginalData++
	case r.present[i]:
		// Duplicate of a buffered packet.
	default:
		base := i * protocol.MaxPayload
		r.slotLen[i] = copy(r.slots[base:base+protocol.MaxPayload], p.Payload)
		r.present[i] = true
		r.stats.OriginalData++
	}
	r.ack(seq)
	return nil
}

func (r *receiver) onFin(p protocol.Packet) error {
	final := r.meta.chunks() + 1
	if uint64(p.Seq) != final {
		return nil
	}
	if r.completed {
		r.finAck(p.Seq)
		return nil
	}
	if uint64(r.next) != final {
		// DATA is still missing; a correct sender never sends FIN before every DATA is ACKed.
		return nil
	}
	if r.written != r.meta.Size {
		return r.abort("size mismatch", fmt.Errorf("wrote %d bytes, expected %d", r.written, r.meta.Size))
	}
	if err := r.out.Flush(); err != nil {
		return r.abort("disk write failed", fmt.Errorf("flush output: %w", err))
	}
	if got := hex.EncodeToString(r.digest.Sum(nil)); got != r.meta.SHA256 {
		return r.abort("checksum mismatch", fmt.Errorf("sha256 %s, expected %s", got, r.meta.SHA256))
	}
	if err := r.publish(); err != nil {
		msg := "publish failed"
		if errors.Is(err, fs.ErrExist) {
			msg = "output already exists"
		}
		return r.abort(msg, err)
	}
	r.completed = true
	r.lingerEnd = time.Now().Add(r.cfg.Linger)
	r.stats.Bytes = r.meta.Size
	r.stats.SHA256 = r.meta.SHA256
	r.stats.Verified = true
	r.stats.Finish(r.start)
	r.finAck(p.Seq)
	return nil
}

// advance writes the in-order payload for next, then drains consecutive buffered slots.
func (r *receiver) advance(payload []byte) error {
	if err := r.write(payload); err != nil {
		return err
	}
	r.next++
	for {
		i := r.slot(r.next)
		if !r.present[i] {
			return nil
		}
		if err := r.write(r.slotBytes(i)); err != nil {
			return err
		}
		r.present[i] = false
		r.next++
	}
}

func (r *receiver) write(b []byte) error {
	if _, err := r.out.Write(b); err != nil {
		return err
	}
	r.digest.Write(b)
	r.written += int64(len(b))
	return nil
}

func (r *receiver) slot(seq uint32) int {
	return int(seq % uint32(r.cfg.Window))
}

func (r *receiver) slotBytes(i int) []byte {
	base := i * protocol.MaxPayload
	return r.slots[base : base+r.slotLen[i]]
}

// publish makes the verified partial file visible at dest. Without Overwrite, os.Link fails
// with EEXIST if dest appeared after the START check, so no existing file is replaced.
func (r *receiver) publish() error {
	if err := r.file.Sync(); err != nil {
		return fmt.Errorf("sync partial file: %w", err)
	}
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("close partial file: %w", err)
	}
	r.file = nil
	if r.cfg.Overwrite {
		if err := os.Rename(r.tmpPath, r.dest); err != nil {
			return fmt.Errorf("publish output: %w", err)
		}
	} else {
		if err := os.Link(r.tmpPath, r.dest); err != nil {
			return fmt.Errorf("publish output: %w", err)
		}
		if err := os.Remove(r.tmpPath); err != nil {
			return fmt.Errorf("remove partial file: %w", err)
		}
	}
	r.tmpPath = ""
	syncDir(r.cfg.OutputDir)
	return nil
}

// discard removes the partial file on any unsuccessful exit.
func (r *receiver) discard() {
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
	}
	if r.tmpPath != "" {
		_ = os.Remove(r.tmpPath)
		r.tmpPath = ""
	}
}

// abort reports a terminal failure to the bound peer and returns err.
func (r *receiver) abort(msg string, err error) error {
	r.sendError(r.peerAP, r.session, msg)
	return err
}

// ack sends ACK(seq). A failed send is indistinguishable from loss; the sender retransmits.
func (r *receiver) ack(seq uint32) {
	_ = r.link.Send(protocol.Packet{Type: protocol.Ack, Session: r.session, Ack: seq}, r.peer)
}

// finAck sends FINACK(seq); send failures are treated like loss for the same reason as ack.
func (r *receiver) finAck(seq uint32) {
	_ = r.link.Send(protocol.Packet{Type: protocol.FinAck, Session: r.session, Ack: seq}, r.peer)
}

// sendError is used on terminal paths only, so its conversion allocation is acceptable.
func (r *receiver) sendError(to netip.AddrPort, session uint64, msg string) {
	payload := []byte(msg)
	if len(payload) > protocol.MaxPayload {
		payload = payload[:protocol.MaxPayload]
	}
	_ = r.link.Send(protocol.Packet{Type: protocol.Error, Session: session, Payload: payload}, net.UDPAddrFromAddrPort(to))
}

func earlier(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// syncDir makes the publication durable on a best-effort basis; the file is already published.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
