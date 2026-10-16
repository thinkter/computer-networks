package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"rudp-go/internal/metrics"
	"rudp-go/internal/protocol"
)

const rcvSession uint64 = 0x7e57

type rcvResult struct {
	stats metrics.Stats
	err   error
}

// rcvHarness runs Receive on a loopback socket and drives it with raw datagrams from client.
type rcvHarness struct {
	t      *testing.T
	dir    string
	server *net.UDPConn
	client *net.UDPConn
	cancel context.CancelFunc
	done   chan rcvResult
}

func rcvLoopback(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func rcvNew(t *testing.T, cfg Config) *rcvHarness {
	t.Helper()
	h := &rcvHarness{
		t:      t,
		dir:    t.TempDir(),
		server: rcvLoopback(t),
		client: rcvLoopback(t),
		done:   make(chan rcvResult, 1),
	}
	cfg.OutputDir = h.dir
	if cfg.Window == 0 {
		cfg.Window = 8
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	t.Cleanup(cancel)
	go func() {
		stats, err := Receive(ctx, h.server, cfg)
		h.done <- rcvResult{stats, err}
	}()
	return h
}

func (h *rcvHarness) serverAddr() *net.UDPAddr { return h.server.LocalAddr().(*net.UDPAddr) }

func (h *rcvHarness) sendFrom(c *net.UDPConn, p protocol.Packet) {
	h.t.Helper()
	b, err := protocol.Encode(nil, p)
	if err != nil {
		h.t.Fatal(err)
	}
	if _, err := c.WriteToUDP(b, h.serverAddr()); err != nil {
		h.t.Fatal(err)
	}
}

func (h *rcvHarness) send(p protocol.Packet) { h.sendFrom(h.client, p) }

// wait returns the Receive result, failing if it does not return promptly.
func (h *rcvHarness) wait() rcvResult {
	h.t.Helper()
	select {
	case r := <-h.done:
		return r
	case <-time.After(5 * time.Second):
		h.t.Fatal("Receive did not return")
		return rcvResult{}
	}
}

// finish cancels after completion, as the bench does, and requires a successful result.
func (h *rcvHarness) finish() metrics.Stats {
	h.t.Helper()
	h.cancel()
	r := h.wait()
	if r.err != nil {
		h.t.Fatalf("Receive after completion: %v", r.err)
	}
	return r.stats
}

// expect waits for an ACK-style packet of type typ with the given ack value, skipping others.
func (h *rcvHarness) expect(typ protocol.Type, ack uint32) {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		p, ok := rcvRead(h.t, h.client, time.Until(deadline))
		if !ok {
			break
		}
		if p.Type == typ && p.Ack == ack {
			return
		}
	}
	h.t.Fatalf("no %v ack=%d from receiver", typ, ack)
}

// expectError waits for an ERROR from the receiver and returns its message.
func (h *rcvHarness) expectError() string {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		p, ok := rcvRead(h.t, h.client, time.Until(deadline))
		if !ok {
			break
		}
		if p.Type == protocol.Error {
			return string(p.Payload)
		}
	}
	h.t.Fatal("no ERROR from receiver")
	return ""
}

// transferAll sends START and every DATA chunk in order, checking each ACK.
func (h *rcvHarness) transferAll(f rcvFixture) {
	h.t.Helper()
	h.send(f.start())
	h.expect(protocol.Ack, 0)
	for seq := uint32(1); seq <= uint32(f.meta.chunks()); seq++ {
		h.send(f.dataPkt(seq))
		h.expect(protocol.Ack, seq)
	}
}

// rcvRead returns the next decodable datagram on c, or false once d elapses or reading fails.
func rcvRead(t *testing.T, c *net.UDPConn, d time.Duration) (protocol.Packet, bool) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(d)); err != nil {
		t.Fatal(err)
	}
	for {
		buf := make([]byte, protocol.MaxDatagram+1)
		n, _, err := c.ReadFromUDP(buf)
		if err != nil {
			return protocol.Packet{}, false
		}
		p, err := protocol.Decode(buf[:n])
		if err != nil {
			continue
		}
		return p, true
	}
}

// expectSilence fails if c receives any decodable datagram within d.
func expectSilence(t *testing.T, c *net.UDPConn, d time.Duration) {
	t.Helper()
	if p, ok := rcvRead(t, c, d); ok {
		t.Fatalf("unexpected %v packet ack=%d seq=%d", p.Type, p.Ack, p.Seq)
	}
}

// rcvFixture is one file to transfer, with metadata that may be deliberately corrupted.
type rcvFixture struct {
	name      string
	data      []byte
	chunkSize int
	meta      Metadata
}

func rcvFile(name string, data []byte, chunkSize int) rcvFixture {
	sum := sha256.Sum256(data)
	return rcvFixture{
		name:      name,
		data:      data,
		chunkSize: chunkSize,
		meta: Metadata{
			Filename:  name,
			Size:      int64(len(data)),
			SHA256:    hex.EncodeToString(sum[:]),
			ChunkSize: chunkSize,
		},
	}
}

func rcvBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 3)
	}
	return b
}

func (f rcvFixture) start() protocol.Packet {
	payload, _ := json.Marshal(f.meta)
	return protocol.Packet{Type: protocol.Start, Session: rcvSession, Payload: payload}
}

func (f rcvFixture) dataPkt(seq uint32) protocol.Packet {
	lo := int(seq-1) * f.chunkSize
	hi := min(lo+f.chunkSize, len(f.data))
	return protocol.Packet{Type: protocol.Data, Session: rcvSession, Seq: seq, Payload: f.data[lo:hi]}
}

func (f rcvFixture) finSeq() uint32 { return uint32(f.meta.chunks()) + 1 }

func (f rcvFixture) finPkt() protocol.Packet {
	return protocol.Packet{Type: protocol.Fin, Session: rcvSession, Seq: f.finSeq()}
}

// rcvNames lists dir entries; ReadDir returns them sorted.
func rcvNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// rcvWantOnly requires dir to contain exactly want, which leaves no partial files behind.
func rcvWantOnly(t *testing.T, dir string, want ...string) {
	t.Helper()
	got := rcvNames(t, dir)
	if len(got) != len(want) {
		t.Fatalf("directory entries %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("directory entries %v, want %v", got, want)
		}
	}
}

// rcvWantFile requires the published output to hold f's bytes and nothing else in dir.
func rcvWantFile(t *testing.T, dir string, f rcvFixture) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(dir, f.name))
	if err != nil {
		t.Fatalf("read published file: %v", err)
	}
	if !bytes.Equal(got, f.data) {
		t.Fatalf("published bytes differ: got %d bytes, want %d", len(got), len(f.data))
	}
	rcvWantOnly(t, dir, f.name)
}

func TestReceiveOutOfOrderDuplicatesAndFinalize(t *testing.T) {
	f := rcvFile("out.bin", rcvBytes(50), 10)
	h := rcvNew(t, Config{Window: 8})
	h.send(f.start())
	h.expect(protocol.Ack, 0)
	for _, seq := range []uint32{3, 2, 3, 5, 4, 1} {
		h.send(f.dataPkt(seq))
		h.expect(protocol.Ack, seq)
	}
	h.send(f.finPkt())
	h.expect(protocol.FinAck, f.finSeq())
	stats := h.finish()
	rcvWantFile(t, h.dir, f)
	if !stats.Verified || stats.Bytes != 50 || stats.File != f.name || stats.SHA256 != f.meta.SHA256 {
		t.Fatalf("stats = %+v", stats)
	}
	if stats.DurationSeconds < 0 {
		t.Fatalf("negative duration: %v", stats.DurationSeconds)
	}
}

func TestReceiveLostFinAckIsReackedAndDuplicateDataReacked(t *testing.T) {
	f := rcvFile("lost.bin", rcvBytes(25), 10)
	h := rcvNew(t, Config{Window: 4})
	h.transferAll(f)
	h.send(f.finPkt())
	h.expect(protocol.FinAck, f.finSeq())
	// The sender missed the FINACK and retransmits FIN; the completed session must answer again.
	h.send(f.finPkt())
	h.expect(protocol.FinAck, f.finSeq())
	// A late duplicate DATA after publication must be re-ACKed without touching the output.
	h.send(f.dataPkt(1))
	h.expect(protocol.Ack, 1)
	h.finish()
	rcvWantFile(t, h.dir, f)
}

func TestReceiveIgnoresForgedPeerAndSession(t *testing.T) {
	f := rcvFile("forged.bin", rcvBytes(20), 10)
	h := rcvNew(t, Config{Window: 4})
	h.send(f.start())
	h.expect(protocol.Ack, 0)

	attacker := rcvLoopback(t)
	// Same session ID from another source address: must not be ACKed or buffered.
	h.sendFrom(attacker, f.dataPkt(2))
	// Bound address with a different session ID: must not be ACKed.
	wrongSession := f.dataPkt(2)
	wrongSession.Session = rcvSession + 1
	h.send(wrongSession)
	expectSilence(t, h.client, 100*time.Millisecond)
	// A forged ERROR from another peer must not terminate the transfer.
	h.sendFrom(attacker, protocol.Packet{Type: protocol.Error, Session: rcvSession, Payload: []byte("forged")})
	expectSilence(t, attacker, 100*time.Millisecond)

	h.send(f.dataPkt(1))
	h.expect(protocol.Ack, 1)
	h.send(f.dataPkt(2))
	h.expect(protocol.Ack, 2)
	h.send(f.finPkt())
	h.expect(protocol.FinAck, f.finSeq())
	h.finish()
	rcvWantFile(t, h.dir, f)
}

func TestReceiveRejectsWrongLengthAndOutOfWindow(t *testing.T) {
	f := rcvFile("bounds.bin", rcvBytes(50), 10)
	h := rcvNew(t, Config{Window: 4})
	h.send(f.start())
	h.expect(protocol.Ack, 0)

	short := f.dataPkt(1)
	short.Payload = short.Payload[:5]
	h.send(short)
	expectSilence(t, h.client, 80*time.Millisecond)

	long := f.dataPkt(1)
	long.Payload = append(append([]byte{}, long.Payload...), 0xff)
	h.send(long)
	expectSilence(t, h.client, 80*time.Millisecond)

	// Window is 4 and nothing is written yet, so seq 5 lies beyond next+Window-1.
	h.send(f.dataPkt(5))
	expectSilence(t, h.client, 80*time.Millisecond)

	// Sequence 6 exceeds the chunk count (5), so its declared length cannot match.
	beyond := f.dataPkt(5)
	beyond.Seq = 6
	h.send(beyond)
	expectSilence(t, h.client, 80*time.Millisecond)

	for seq := uint32(1); seq <= 5; seq++ {
		h.send(f.dataPkt(seq))
		h.expect(protocol.Ack, seq)
	}
	h.send(f.finPkt())
	h.expect(protocol.FinAck, f.finSeq())
	h.finish()
	rcvWantFile(t, h.dir, f)
}

func TestReceiveIgnoresMalformedStartThenAcceptsValid(t *testing.T) {
	f := rcvFile("start.bin", rcvBytes(10), 10)
	h := rcvNew(t, Config{Window: 4})
	h.send(protocol.Packet{Type: protocol.Start, Session: rcvSession, Payload: []byte("{")})
	h.expectError()
	h.transferAll(f)
	h.send(f.finPkt())
	h.expect(protocol.FinAck, f.finSeq())
	h.finish()
	rcvWantFile(t, h.dir, f)
}

func TestReceiveRepeatedStartMustMatch(t *testing.T) {
	f := rcvFile("start-match.bin", rcvBytes(10), 10)
	h := rcvNew(t, Config{Window: 4})
	h.send(f.start())
	h.expect(protocol.Ack, 0)
	h.send(f.start())
	h.expect(protocol.Ack, 0)

	other := rcvFile("other.bin", rcvBytes(10), 10)
	h.send(other.start())
	if msg := h.expectError(); msg == "" {
		t.Fatal("empty ERROR message")
	}
	r := h.wait()
	if r.err == nil {
		t.Fatal("changed START metadata did not fail the transfer")
	}
	rcvWantOnly(t, h.dir)
}

func TestReceiveRefusesExistingOutput(t *testing.T) {
	f := rcvFile("keep.bin", rcvBytes(10), 10)
	h := rcvNew(t, Config{Window: 4})
	dest := filepath.Join(h.dir, f.name)
	if err := os.WriteFile(dest, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	h.send(f.start())
	h.expectError()
	if r := h.wait(); r.err == nil {
		t.Fatal("existing output was not refused")
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != "old" {
		t.Fatalf("existing output changed: %q, %v", got, err)
	}
	rcvWantOnly(t, h.dir, f.name)
}

func TestReceiveOverwriteReplacesOutput(t *testing.T) {
	f := rcvFile("replace.bin", rcvBytes(30), 10)
	h := rcvNew(t, Config{Window: 4, Overwrite: true})
	if err := os.WriteFile(filepath.Join(h.dir, f.name), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	h.transferAll(f)
	h.send(f.finPkt())
	h.expect(protocol.FinAck, f.finSeq())
	h.finish()
	rcvWantFile(t, h.dir, f)
}

func TestReceiveRefusesOutputCreatedDuringTransfer(t *testing.T) {
	f := rcvFile("race.bin", rcvBytes(30), 10)
	h := rcvNew(t, Config{Window: 4})
	h.transferAll(f)
	// Another writer creates the destination after START and before FIN; publication must not replace it.
	dest := filepath.Join(h.dir, f.name)
	if err := os.WriteFile(dest, []byte("racer"), 0o644); err != nil {
		t.Fatal(err)
	}
	h.send(f.finPkt())
	h.expectError()
	if r := h.wait(); r.err == nil {
		t.Fatal("publication overwrote a file created during the transfer")
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != "racer" {
		t.Fatalf("destination changed: %q, %v", got, err)
	}
	rcvWantOnly(t, h.dir, f.name)
}

func TestReceiveCorruptHashCleansUp(t *testing.T) {
	data := rcvBytes(30)
	f := rcvFile("corrupt.bin", data, 10)
	other := rcvFile("other.bin", rcvBytes(31), 10)
	f.meta.SHA256 = other.meta.SHA256
	h := rcvNew(t, Config{Window: 4})
	h.transferAll(f)
	h.send(f.finPkt())
	h.expectError()
	if r := h.wait(); r.err == nil {
		t.Fatal("hash mismatch did not fail the transfer")
	}
	rcvWantOnly(t, h.dir)
}

func TestReceiveCancelBeforeCompletionCleansPartial(t *testing.T) {
	f := rcvFile("cancel.bin", rcvBytes(30), 10)
	h := rcvNew(t, Config{Window: 4})
	h.send(f.start())
	h.expect(protocol.Ack, 0)
	h.send(f.dataPkt(1))
	h.expect(protocol.Ack, 1)
	h.cancel()
	if r := h.wait(); r.err == nil {
		t.Fatal("cancellation before completion returned success")
	}
	rcvWantOnly(t, h.dir)
}

func TestReceiveSelectedSessionErrorCleansUp(t *testing.T) {
	f := rcvFile("peer-error.bin", rcvBytes(30), 10)
	h := rcvNew(t, Config{Window: 4})
	h.send(f.start())
	h.expect(protocol.Ack, 0)
	h.send(f.dataPkt(1))
	h.expect(protocol.Ack, 1)
	h.send(protocol.Packet{Type: protocol.Error, Session: rcvSession, Payload: []byte("sender gave up")})
	if r := h.wait(); r.err == nil {
		t.Fatal("peer ERROR did not end the transfer")
	}
	rcvWantOnly(t, h.dir)
}

func TestReceiveZeroByteFile(t *testing.T) {
	f := rcvFile("empty.bin", nil, 1024)
	h := rcvNew(t, Config{Window: 4})
	h.send(f.start())
	h.expect(protocol.Ack, 0)
	h.send(f.finPkt())
	h.expect(protocol.FinAck, f.finSeq())
	stats := h.finish()
	rcvWantFile(t, h.dir, f)
	if !stats.Verified || stats.Bytes != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestReceiveIdleTimeoutCleansUp(t *testing.T) {
	for _, started := range []bool{false, true} {
		name := "waiting-for-start"
		if started {
			name = "partial-transfer"
		}
		t.Run(name, func(t *testing.T) {
			h := rcvNew(t, Config{IdleTimeout: 100 * time.Millisecond})
			if started {
				f := rcvFile("idle.bin", rcvBytes(30), 10)
				h.send(f.start())
				h.expect(protocol.Ack, 0)
				h.send(f.dataPkt(1))
				h.expect(protocol.Ack, 1)
				h.expectError()
			}
			result := h.wait()
			if result.err == nil || result.stats.Verified {
				t.Fatalf("idle transfer reported success: %+v", result)
			}
			rcvWantOnly(t, h.dir)
		})
	}
}

func TestReceiveDoesNotAckCorruptOrOversizedDatagrams(t *testing.T) {
	f := rcvFile("crc.bin", rcvBytes(30), 10)
	h := rcvNew(t, Config{Window: 4})
	h.send(f.start())
	h.expect(protocol.Ack, 0)
	b, err := protocol.Encode(nil, f.dataPkt(1))
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 1
	if _, err := h.client.WriteToUDP(b, h.serverAddr()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.WriteToUDP(make([]byte, protocol.MaxDatagram+100), h.serverAddr()); err != nil {
		t.Fatal(err)
	}
	expectSilence(t, h.client, 80*time.Millisecond)
	for seq := uint32(1); seq <= uint32(f.meta.chunks()); seq++ {
		h.send(f.dataPkt(seq))
		h.expect(protocol.Ack, seq)
	}
	h.send(f.finPkt())
	h.expect(protocol.FinAck, f.finSeq())
	h.finish()
	rcvWantFile(t, h.dir, f)
}
