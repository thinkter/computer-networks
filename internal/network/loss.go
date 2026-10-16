// Package network sends protocol packets over UDP with seeded outgoing loss.
package network

import (
	"fmt"
	"math/rand/v2"
	"net"

	"rudp-go/internal/metrics"
	"rudp-go/internal/protocol"
)

// pcgStream is the fixed second PCG seed word. The first word is the user seed,
// so one seed value always yields the same drop sequence.
const pcgStream = 0x9e3779b97f4a7c15

// Link encodes packets and writes them to a UDP socket, dropping a seeded
// fraction of attempts to simulate loss on this endpoint's outgoing datagrams.
//
// A Link has a single owner and is not safe for concurrent use. Send never
// sleeps.
type Link struct {
	conn  *net.UDPConn
	loss  float64
	rng   *rand.Rand
	stats *metrics.Stats
	buf   [protocol.MaxDatagram]byte
}

// New returns a Link that drops each outgoing datagram with probability loss.
// Drop decisions come from a private generator seeded with seed. Counters are
// recorded in stats, which may be nil.
func New(conn *net.UDPConn, loss float64, seed int64, stats *metrics.Stats) *Link {
	return &Link{
		conn:  conn,
		loss:  loss,
		rng:   rand.New(rand.NewPCG(uint64(seed), pcgStream)),
		stats: stats,
	}
}

// Send encodes p and transmits it to peer. A nil peer writes to the connected
// socket's default destination; otherwise it writes to peer with WriteToUDP.
//
// Every attempt, including attempts dropped by simulated loss, increments
// DatagramsSent. Attempts dropped by simulation also increment Dropped and are
// not written. Encoding errors are returned before any counter changes.
func (l *Link) Send(p protocol.Packet, peer *net.UDPAddr) error {
	dgram, err := protocol.Encode(l.buf[:0], p)
	if err != nil {
		return err
	}
	if l.stats != nil {
		l.stats.DatagramsSent++
	}
	if l.drop() {
		if l.stats != nil {
			l.stats.Dropped++
		}
		return nil
	}
	if peer == nil {
		_, err = l.conn.Write(dgram)
	} else {
		_, err = l.conn.WriteToUDP(dgram, peer)
	}
	if err != nil {
		return fmt.Errorf("network: write: %w", err)
	}
	return nil
}

// drop consumes one generator draw per attempt when loss is positive.
func (l *Link) drop() bool {
	return l.loss > 0 && l.rng.Float64() < l.loss
}
