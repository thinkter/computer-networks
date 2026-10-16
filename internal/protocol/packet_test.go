package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

// rawFrame builds a datagram without field validation. It seals the CRC so
// that only the field rules, not the checksum, can reject the frame.
func rawFrame(t Type, session uint64, seq, ack uint32, payload []byte) []byte {
	b := make([]byte, HeaderSize+len(payload))
	binary.BigEndian.PutUint16(b[offMagic:], Magic)
	b[offVersion] = Version
	b[offType] = byte(t)
	binary.BigEndian.PutUint64(b[offSession:], session)
	binary.BigEndian.PutUint32(b[offSeq:], seq)
	binary.BigEndian.PutUint32(b[offAck:], ack)
	binary.BigEndian.PutUint16(b[offLen:], uint16(len(payload)))
	copy(b[HeaderSize:], payload)
	seal(b)
	return b
}

// seal recomputes the CRC with the standard library streaming API, which is
// independent of the package's checksum helper.
func seal(b []byte) {
	h := crc32.NewIEEE()
	h.Write(b[:offCRC])
	h.Write(b[HeaderSize:])
	binary.BigEndian.PutUint32(b[offCRC:], h.Sum32())
}

func mustEncode(t *testing.T, p Packet) []byte {
	t.Helper()
	b, err := Encode(nil, p)
	if err != nil {
		t.Fatalf("Encode(%v): %v", p.Type, err)
	}
	return b
}

var validPackets = []Packet{
	{Type: Start, Session: 0x0102030405060708, Seq: 0, Ack: 0, Payload: []byte(`{"filename":"a.bin","size":3}`)},
	{Type: Data, Session: 1, Seq: 1, Ack: 0, Payload: bytes.Repeat([]byte{0xAB}, MaxPayload)},
	{Type: Data, Session: 1, Seq: 0xFFFFFFFF, Ack: 0, Payload: []byte{0}},
	{Type: Ack, Session: 42, Seq: 0, Ack: 17},
	{Type: Ack, Session: 42, Seq: 0, Ack: 0}, // ACK(0) acknowledges START
	{Type: Fin, Session: 42, Seq: 9, Ack: 0},
	{Type: FinAck, Session: 42, Seq: 0, Ack: 10},
	{Type: Error, Session: 7, Seq: 0, Ack: 0, Payload: []byte("disk full")},
}

// fieldCase is a packet that violates a field rule. The same table drives the
// Encode rejection test and, via rawFrame, the Decode rejection test.
type fieldCase struct {
	name string
	p    Packet
	want error
}

var invalidFields = []fieldCase{
	{"start seq 1", Packet{Type: Start, Session: 1, Seq: 1, Payload: []byte("x")}, ErrShape},
	{"start ack 1", Packet{Type: Start, Session: 1, Ack: 1, Payload: []byte("x")}, ErrShape},
	{"start empty payload", Packet{Type: Start, Session: 1}, ErrShape},
	{"data seq 0", Packet{Type: Data, Session: 1, Seq: 0, Payload: []byte("x")}, ErrShape},
	{"data ack 1", Packet{Type: Data, Session: 1, Seq: 1, Ack: 1, Payload: []byte("x")}, ErrShape},
	{"data empty payload", Packet{Type: Data, Session: 1, Seq: 1}, ErrShape},
	{"ack seq 1", Packet{Type: Ack, Session: 1, Seq: 1, Ack: 1}, ErrShape},
	{"ack with payload", Packet{Type: Ack, Session: 1, Ack: 1, Payload: []byte("x")}, ErrShape},
	{"fin seq 0", Packet{Type: Fin, Session: 1, Seq: 0}, ErrShape},
	{"fin ack 1", Packet{Type: Fin, Session: 1, Seq: 2, Ack: 1}, ErrShape},
	{"fin with payload", Packet{Type: Fin, Session: 1, Seq: 2, Payload: []byte("x")}, ErrShape},
	{"finack seq 1", Packet{Type: FinAck, Session: 1, Seq: 1, Ack: 1}, ErrShape},
	{"finack ack 0", Packet{Type: FinAck, Session: 1, Ack: 0}, ErrShape},
	{"finack with payload", Packet{Type: FinAck, Session: 1, Ack: 1, Payload: []byte("x")}, ErrShape},
	{"error seq 1", Packet{Type: Error, Session: 1, Seq: 1, Payload: []byte("x")}, ErrShape},
	{"error ack 1", Packet{Type: Error, Session: 1, Ack: 1, Payload: []byte("x")}, ErrShape},
	{"error empty payload", Packet{Type: Error, Session: 1}, ErrShape},
	{"zero session", Packet{Type: Ack, Session: 0}, ErrShape},
	{"type zero", Packet{Type: 0, Session: 1}, ErrType},
	{"type seven", Packet{Type: 7, Session: 1}, ErrType},
	{"payload over max", Packet{Type: Data, Session: 1, Seq: 1, Payload: make([]byte, MaxPayload+1)}, ErrOversized},
}

func TestRoundTrip(t *testing.T) {
	for _, p := range validPackets {
		enc := mustEncode(t, p)
		if want := HeaderSize + len(p.Payload); len(enc) != want {
			t.Fatalf("%v: encoded length %d, want %d", p.Type, len(enc), want)
		}
		got, err := Decode(enc)
		if err != nil {
			t.Fatalf("%v: Decode: %v", p.Type, err)
		}
		if got.Type != p.Type || got.Session != p.Session || got.Seq != p.Seq || got.Ack != p.Ack {
			t.Fatalf("%v: header = %+v, want %+v", p.Type, got, p)
		}
		if !bytes.Equal(got.Payload, p.Payload) {
			t.Fatalf("%v: payload mismatch", p.Type)
		}
	}
}

func TestHeaderLayout(t *testing.T) {
	enc := mustEncode(t, Packet{Type: Data, Session: 0x1122334455667788, Seq: 0x0A0B0C0D, Payload: []byte("hi")})
	want := []byte{
		0x52, 0x55, // magic
		1,                                              // version
		2,                                              // DATA
		0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // session, big-endian
		0x0A, 0x0B, 0x0C, 0x0D, // seq
		0, 0, 0, 0, // ack
		0, 2, // payload length
	}
	if !bytes.Equal(enc[:offCRC], want) {
		t.Fatalf("header prefix = % x, want % x", enc[:offCRC], want)
	}
	if !bytes.Equal(enc[HeaderSize:], []byte("hi")) {
		t.Fatalf("payload = %q", enc[HeaderSize:])
	}
	h := crc32.NewIEEE()
	h.Write(enc[:offCRC])
	h.Write(enc[HeaderSize:])
	if got := binary.BigEndian.Uint32(enc[offCRC:]); got != h.Sum32() {
		t.Fatalf("CRC field = %08x, want %08x", got, h.Sum32())
	}
}

func TestEncodeReusesCallerBuffer(t *testing.T) {
	buf := make([]byte, 0, MaxDatagram)
	p := validPackets[1]
	out, err := Encode(buf, p)
	if err != nil {
		t.Fatal(err)
	}
	if &out[0] != &buf[:1][0] {
		t.Fatal("Encode allocated despite sufficient capacity")
	}
	var encErr error
	allocs := testing.AllocsPerRun(100, func() {
		_, encErr = Encode(buf[:0], p)
	})
	if encErr != nil {
		t.Fatal(encErr)
	}
	if allocs != 0 {
		t.Fatalf("Encode into reused buffer allocated %v times per call", allocs)
	}
}

func TestDecodeDoesNotAllocate(t *testing.T) {
	enc := mustEncode(t, validPackets[1])
	var decErr error
	allocs := testing.AllocsPerRun(100, func() {
		_, decErr = Decode(enc)
	})
	if decErr != nil {
		t.Fatal(decErr)
	}
	if allocs != 0 {
		t.Fatalf("Decode allocated %v times per call", allocs)
	}
}

func TestDecodeBorrowsPayload(t *testing.T) {
	enc := mustEncode(t, Packet{Type: Data, Session: 1, Seq: 1, Payload: []byte("hello")})
	got, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if &got.Payload[0] != &enc[HeaderSize] {
		t.Fatal("Decode copied the payload; it must borrow")
	}
	enc[HeaderSize] = 'J'
	if got.Payload[0] != 'J' {
		t.Fatal("payload does not track the source buffer")
	}
}

func TestEncodeRejectsInvalidFields(t *testing.T) {
	for _, c := range invalidFields {
		t.Run(c.name, func(t *testing.T) {
			out, err := Encode(nil, c.p)
			if !errors.Is(err, c.want) {
				t.Fatalf("Encode error = %v, want %v", err, c.want)
			}
			if out != nil {
				t.Fatalf("Encode returned %d bytes alongside an error", len(out))
			}
		})
	}
}

func TestDecodeRejectsInvalidFields(t *testing.T) {
	for _, c := range invalidFields {
		t.Run(c.name, func(t *testing.T) {
			_, err := Decode(rawFrame(c.p.Type, c.p.Session, c.p.Seq, c.p.Ack, c.p.Payload))
			if !errors.Is(err, c.want) {
				t.Fatalf("Decode error = %v, want %v", err, c.want)
			}
		})
	}
}

func TestDecodeRejectsMalformedDatagrams(t *testing.T) {
	good := mustEncode(t, Packet{Type: Data, Session: 5, Seq: 3, Payload: []byte("payload")})

	withByte := func(i int, v byte) []byte {
		b := bytes.Clone(good)
		b[i] = v
		return b
	}
	flipped := func(i int) []byte {
		b := bytes.Clone(good)
		b[i] ^= 0x01
		return b
	}
	lyingLength := func() []byte {
		b := rawFrame(Data, 5, 3, 0, []byte("payload"))
		binary.BigEndian.PutUint16(b[offLen:], 0)
		seal(b)
		return b
	}

	cases := []struct {
		name string
		b    []byte
		want error
	}{
		{"empty", nil, ErrTruncated},
		{"short header", good[:HeaderSize-1], ErrTruncated},
		{"missing payload byte", good[:len(good)-1], ErrTruncated},
		{"trailing byte", append(bytes.Clone(good), 0), ErrLength},
		{"oversized", append(rawFrame(Data, 1, 1, 0, make([]byte, MaxPayload)), 0), ErrOversized},
		{"bad magic", withByte(offMagic, 0x00), ErrMagic},
		{"bad version", withByte(offVersion, 2), ErrVersion},
		{"type zero", withByte(offType, 0), ErrType},
		{"type unknown", withByte(offType, 7), ErrType},
		{"declared length zero", lyingLength(), ErrLength},
		{"payload bit flip", flipped(len(good) - 1), ErrChecksum},
		{"seq bit flip", flipped(offSeq + 3), ErrChecksum},
		{"session bit flip", flipped(offSession), ErrChecksum},
		{"crc field bit flip", flipped(offCRC), ErrChecksum},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := Decode(c.b)
			if !errors.Is(err, c.want) {
				t.Fatalf("Decode error = %v, want %v", err, c.want)
			}
			if p.Payload != nil || p.Session != 0 {
				t.Fatalf("Decode returned %+v alongside an error", p)
			}
		})
	}
}
