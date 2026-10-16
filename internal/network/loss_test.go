package network

import (
	"errors"
	"net"
	"os"
	"slices"
	"testing"
	"time"

	"rudp-go/internal/metrics"
	"rudp-go/internal/protocol"
)

// drawPattern returns the first n drop decisions of a Link with the given seed.
func drawPattern(seed int64, loss float64, n int) []bool {
	l := New(nil, loss, seed, nil)
	out := make([]bool, n)
	for i := range n {
		out[i] = l.drop()
	}
	return out
}

func countTrue(bs []bool) int {
	n := 0
	for _, b := range bs {
		if b {
			n++
		}
	}
	return n
}

func TestDropPatternIsReproducible(t *testing.T) {
	const n = 5000
	a := drawPattern(42, 0.2, n)
	if !slices.Equal(a, drawPattern(42, 0.2, n)) {
		t.Fatal("same seed produced different drop patterns")
	}
	if slices.Equal(a, drawPattern(43, 0.2, n)) {
		t.Fatal("different seeds produced identical drop patterns")
	}
	rate := float64(countTrue(a)) / n
	if rate < 0.17 || rate > 0.23 {
		t.Fatalf("drop rate %.3f, want about 0.20", rate)
	}
}

func TestDropBoundaries(t *testing.T) {
	if countTrue(drawPattern(1, 0, 1000)) != 0 {
		t.Fatal("loss 0 dropped a datagram")
	}
	if countTrue(drawPattern(1, 1, 1000)) != 1000 {
		t.Fatal("loss 1 let a datagram through")
	}
}

func listenLoopback(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// arrival records what a receiver observed about one datagram.
type arrival struct {
	from    *net.UDPAddr
	session uint64
	seq     uint32
}

// drain reads every datagram that arrives before the socket is idle for 100ms.
func drain(t *testing.T, c *net.UDPConn) []arrival {
	t.Helper()
	var got []arrival
	buf := make([]byte, protocol.MaxDatagram+1)
	for {
		if err := c.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		n, from, err := c.ReadFromUDP(buf)
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return got
		}
		if err != nil {
			t.Fatal(err)
		}
		p, err := protocol.Decode(buf[:n])
		if err != nil {
			t.Fatalf("receiver got invalid datagram: %v", err)
		}
		got = append(got, arrival{from: from, session: p.Session, seq: p.Seq})
	}
}

func udpAddr(c *net.UDPConn) *net.UDPAddr { return c.LocalAddr().(*net.UDPAddr) }

// runSeeded sends 64 DATA packets through a seeded lossy link and returns the
// sequence numbers that arrived, sorted, along with the link's counters.
func runSeeded(t *testing.T, seed int64) ([]uint32, metrics.Stats) {
	t.Helper()
	recv := listenLoopback(t)
	sender := listenLoopback(t)
	var st metrics.Stats
	link := New(sender, 0.5, seed, &st)
	for i := range 64 {
		p := protocol.Packet{Type: protocol.Data, Session: 9, Seq: uint32(i + 1), Payload: []byte{byte(i)}}
		if err := link.Send(p, udpAddr(recv)); err != nil {
			t.Fatal(err)
		}
	}
	var seqs []uint32
	for _, a := range drain(t, recv) {
		seqs = append(seqs, a.seq)
	}
	slices.Sort(seqs)
	return seqs, st
}

func TestSeededSendLossIsReproducibleAndCounted(t *testing.T) {
	first, st := runSeeded(t, 7)
	second, st2 := runSeeded(t, 7)
	if !slices.Equal(first, second) {
		t.Fatalf("same seed delivered different sets: %v vs %v", first, second)
	}
	if st.DatagramsSent != 64 || st2.DatagramsSent != 64 {
		t.Fatalf("DatagramsSent = %d/%d, want 64 attempts", st.DatagramsSent, st2.DatagramsSent)
	}
	if st.Dropped != st2.Dropped {
		t.Fatalf("Dropped = %d vs %d for same seed", st.Dropped, st2.Dropped)
	}
	if got := uint64(len(first)) + st.Dropped; got != 64 {
		t.Fatalf("delivered %d + dropped %d != 64 attempts", len(first), st.Dropped)
	}

	// Each attempt consumes one decision, so delivery must match the seeded pattern.
	pattern := drawPattern(7, 0.5, 64)
	var want []uint32
	for i, dropped := range pattern {
		if !dropped {
			want = append(want, uint32(i+1))
		}
	}
	if !slices.Equal(first, want) {
		t.Fatalf("delivered %v, want seeded survivors %v", first, want)
	}

	other, _ := runSeeded(t, 8)
	if slices.Equal(first, other) {
		t.Fatal("different seeds delivered identical sets")
	}
}

func TestFullLossDeliversNothingButCountsAttempts(t *testing.T) {
	recv := listenLoopback(t)
	sender := listenLoopback(t)
	var st metrics.Stats
	link := New(sender, 1, 3, &st)
	for i := range 10 {
		p := protocol.Packet{Type: protocol.Ack, Session: 1, Ack: uint32(i + 1)}
		if err := link.Send(p, udpAddr(recv)); err != nil {
			t.Fatal(err)
		}
	}
	if got := drain(t, recv); len(got) != 0 {
		t.Fatalf("loss 1 delivered %d datagrams", len(got))
	}
	if st.DatagramsSent != 10 || st.Dropped != 10 {
		t.Fatalf("DatagramsSent=%d Dropped=%d, want 10/10", st.DatagramsSent, st.Dropped)
	}
}

func TestStatsAreAttributedToSendingLink(t *testing.T) {
	recv := listenLoopback(t)
	sockA := listenLoopback(t)
	sockB := listenLoopback(t)
	var stA, stB metrics.Stats
	linkA := New(sockA, 0, 1, &stA)
	linkB := New(sockB, 0, 2, &stB)

	for i := range 3 {
		if err := linkA.Send(protocol.Packet{Type: protocol.Data, Session: 1, Seq: uint32(i + 1), Payload: []byte("a")}, udpAddr(recv)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 5 {
		if err := linkB.Send(protocol.Packet{Type: protocol.Data, Session: 2, Seq: uint32(i + 1), Payload: []byte("b")}, udpAddr(recv)); err != nil {
			t.Fatal(err)
		}
	}

	if stA.DatagramsSent != 3 || stB.DatagramsSent != 5 {
		t.Fatalf("DatagramsSent A=%d B=%d, want 3 and 5", stA.DatagramsSent, stB.DatagramsSent)
	}
	if stA.Dropped != 0 || stB.Dropped != 0 {
		t.Fatalf("Dropped A=%d B=%d with zero loss", stA.Dropped, stB.Dropped)
	}

	perSession := map[uint64]int{}
	for _, a := range drain(t, recv) {
		switch a.from.Port {
		case udpAddr(sockA).Port:
			if a.session != 1 {
				t.Fatalf("socket A datagram carried session %d", a.session)
			}
		case udpAddr(sockB).Port:
			if a.session != 2 {
				t.Fatalf("socket B datagram carried session %d", a.session)
			}
		default:
			t.Fatalf("datagram from unexpected source %v", a.from)
		}
		perSession[a.session]++
	}
	if perSession[1] != 3 || perSession[2] != 5 {
		t.Fatalf("arrivals per session = %v, want 3 and 5", perSession)
	}
}

func TestEncodeErrorDoesNotCountAttempt(t *testing.T) {
	recv := listenLoopback(t)
	sender := listenLoopback(t)
	var st metrics.Stats
	link := New(sender, 0, 1, &st)
	err := link.Send(protocol.Packet{Type: protocol.Data, Session: 1, Seq: 0, Payload: []byte("x")}, udpAddr(recv))
	if !errors.Is(err, protocol.ErrShape) {
		t.Fatalf("Send error = %v, want ErrShape", err)
	}
	if st.DatagramsSent != 0 || st.Dropped != 0 {
		t.Fatalf("invalid packet changed counters: %+v", st)
	}
	if got := drain(t, recv); len(got) != 0 {
		t.Fatalf("invalid packet was written: %+v", got)
	}
}
