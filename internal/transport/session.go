package transport

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	"rudp-go/internal/protocol"
)

// Config controls one transfer. Zero timing values select the documented defaults.
type Config struct {
	Window      int
	RTO         time.Duration
	RetryLimit  int
	Loss        float64
	Seed        int64
	OutputDir   string
	Overwrite   bool
	IdleTimeout time.Duration
	Linger      time.Duration
}

func (c Config) normalized() (Config, error) {
	if c.Window == 0 {
		c.Window = 32
	}
	if c.RTO == 0 {
		c.RTO = 500 * time.Millisecond
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 15 * time.Second
	}
	if c.Linger == 0 {
		c.Linger = 5 * time.Second
	}
	if c.OutputDir == "" {
		c.OutputDir = "."
	}
	if c.Window < 1 || c.Window > 128 {
		return c, errors.New("window must be in 1..128")
	}
	if c.RTO < time.Millisecond || c.RTO > time.Minute {
		return c, errors.New("rto must be in 1ms..1m")
	}
	if c.RetryLimit < 0 || c.RetryLimit > 1000 {
		return c, errors.New("retries must be in 0..1000")
	}
	if math.IsNaN(c.Loss) || c.Loss < 0 || c.Loss > .3 {
		return c, errors.New("loss must be in 0..0.3")
	}
	if c.IdleTimeout < time.Millisecond || c.Linger < 5*time.Second {
		return c, errors.New("idle timeout must be positive and linger must be at least 5s")
	}
	return c, nil
}

// DefaultConfig is appropriate for the command-line sender and receiver.
func DefaultConfig() Config {
	return Config{RTO: 500 * time.Millisecond, RetryLimit: 15, IdleTimeout: 15 * time.Second, Linger: 5 * time.Second}
}

type Metadata struct {
	Filename  string `json:"filename"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256"`
	ChunkSize int    `json:"chunk_size"`
}

func (m Metadata) validate() error {
	// Both separator styles are forbidden, including on Unix: metadata is portable.
	if m.Filename == "" || m.Filename == "." || m.Filename == ".." || filepath.Base(m.Filename) != m.Filename || strings.ContainsAny(m.Filename, "/\\\x00") || len(m.Filename) > 255 {
		return errors.New("filename must be a safe basename of at most 255 bytes")
	}
	if m.Size < 0 || m.ChunkSize < 1 || m.ChunkSize > protocol.MaxPayload {
		return errors.New("invalid size or chunk size")
	}
	if m.chunks() > math.MaxUint32-1 {
		return errors.New("file exceeds sequence number space")
	}
	sum, err := hex.DecodeString(m.SHA256)
	if err != nil || len(sum) != 32 || strings.ToLower(m.SHA256) != m.SHA256 {
		return errors.New("invalid SHA-256")
	}
	return nil
}

func (m Metadata) chunks() uint64 {
	if m.Size == 0 || m.ChunkSize <= 0 {
		return 0
	}
	return uint64((m.Size-1)/int64(m.ChunkSize)) + 1
}
func (m Metadata) dataSize(seq uint32) int {
	if seq == 0 || uint64(seq) > m.chunks() {
		return 0
	}
	remaining := m.Size - int64(seq-1)*int64(m.ChunkSize)
	if remaining < int64(m.ChunkSize) {
		return int(remaining)
	}
	return m.ChunkSize
}
func decodeMetadata(data []byte) (Metadata, error) {
	var m Metadata
	if err := json.Unmarshal(data, &m); err != nil {
		return m, fmt.Errorf("START metadata: %w", err)
	}
	return m, m.validate()
}
