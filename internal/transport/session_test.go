package transport

import (
	"math"
	"strings"
	"testing"

	"rudp-go/internal/protocol"
)

func TestMetadataRejectsUnsafeNamesAndSizes(t *testing.T) {
	base := Metadata{Filename: "safe.bin", Size: 1, SHA256: strings.Repeat("a", 64), ChunkSize: protocol.MaxPayload}
	for _, name := range []string{"", ".", "..", "../escape", "/tmp/escape", `..\escape`, "bad\x00name", strings.Repeat("x", 256)} {
		t.Run("name="+name, func(t *testing.T) {
			m := base
			m.Filename = name
			if err := m.validate(); err == nil {
				t.Fatalf("accepted unsafe name %q", name)
			}
		})
	}
	for _, size := range []int64{-1, math.MaxInt64, int64(math.MaxUint32) * protocol.MaxPayload} {
		m := base
		m.Size = size
		if err := m.validate(); err == nil {
			t.Fatalf("accepted size %d", size)
		}
	}
	for _, chunk := range []int{0, -1, protocol.MaxPayload + 1} {
		m := base
		m.ChunkSize = chunk
		if err := m.validate(); err == nil {
			t.Fatalf("accepted chunk size %d", chunk)
		}
	}
	for _, hash := range []string{"", strings.Repeat("a", 63), strings.Repeat("A", 64), strings.Repeat("z", 64)} {
		m := base
		m.SHA256 = hash
		if err := m.validate(); err == nil {
			t.Fatalf("accepted hash %q", hash)
		}
	}
}

func TestMetadataChunkBoundaries(t *testing.T) {
	cases := []struct {
		size   int64
		chunks uint64
		last   int
	}{
		{0, 0, 0}, {1, 1, 1}, {protocol.MaxPayload, 1, protocol.MaxPayload}, {protocol.MaxPayload + 1, 2, 1},
		{int64(math.MaxUint32-1) * protocol.MaxPayload, math.MaxUint32 - 1, protocol.MaxPayload},
	}
	for _, tc := range cases {
		m := Metadata{Filename: "f", Size: tc.size, SHA256: strings.Repeat("0", 64), ChunkSize: protocol.MaxPayload}
		if err := m.validate(); err != nil {
			t.Fatal(err)
		}
		if m.chunks() != tc.chunks {
			t.Fatalf("size %d: chunks %d", tc.size, m.chunks())
		}
		if got := m.dataSize(uint32(tc.chunks)); got != tc.last {
			t.Fatalf("size %d: final bytes %d", tc.size, got)
		}
		if m.dataSize(0) != 0 || m.dataSize(uint32(tc.chunks+1)) != 0 {
			t.Fatal("accepted out-of-range sequence")
		}
	}
}
