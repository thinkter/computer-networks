// Package metrics holds transfer counters and the end-of-transfer summary.
package metrics

import "time"

// Stats summarizes one transfer. A Stats value is updated by a single owner.
type Stats struct {
	File                string  `json:"file"`
	Bytes               int64   `json:"bytes"`
	Window              int     `json:"window"`
	Loss                float64 `json:"loss"`
	DurationSeconds     float64 `json:"duration_seconds"`
	GoodputBPS          float64 `json:"goodput_bps"`
	DatagramsSent       uint64  `json:"datagrams_sent"`
	Dropped             uint64  `json:"dropped"`
	OriginalData        uint64  `json:"original_data"`
	Retransmissions     uint64  `json:"retransmissions"`
	DataRetransmissions uint64  `json:"data_retransmissions"`
	SHA256              string  `json:"sha256"`
	Verified            bool    `json:"verified"`
}

// Finish records the time elapsed since start and the goodput over Bytes.
// Goodput is zero when no time elapsed, which keeps the value finite for JSON.
func (s *Stats) Finish(start time.Time) {
	d := time.Since(start).Seconds()
	if d < 0 {
		d = 0
	}
	s.DurationSeconds = d
	s.GoodputBPS = 0
	if d > 0 {
		s.GoodputBPS = float64(s.Bytes) / d
	}
}
