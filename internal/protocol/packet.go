// Package protocol implements the RUDP version 1 datagram format.
//
// Every datagram is a fixed 26-byte big-endian header followed by up to
// MaxPayload payload bytes. The CRC32 (IEEE) covers header bytes 0..21 and the
// payload. It detects accidental corruption only; it is not authentication.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// HeaderSize is the fixed header length in bytes.
	HeaderSize = 26
	// MaxDatagram is the largest datagram accepted or produced, header included.
	MaxDatagram = 1200
	// MaxPayload is the largest payload a single datagram can carry.
	MaxPayload = MaxDatagram - HeaderSize

	// Magic identifies RUDP datagrams.
	Magic uint16 = 0x5255
	// Version is the only wire version this package reads or writes.
	Version uint8 = 1
)

// Header field offsets.
const (
	offMagic   = 0
	offVersion = 2
	offType    = 3
	offSession = 4
	offSeq     = 12
	offAck     = 16
	offLen     = 20
	offCRC     = 22
)

// Type identifies a packet's role in the transfer.
type Type uint8

const (
	Start Type = iota + 1
	Data
	Ack
	Fin
	FinAck
	Error
)

func (t Type) valid() bool { return t >= Start && t <= Error }

func (t Type) String() string {
	switch t {
	case Start:
		return "START"
	case Data:
		return "DATA"
	case Ack:
		return "ACK"
	case Fin:
		return "FIN"
	case FinAck:
		return "FINACK"
	case Error:
		return "ERROR"
	}
	return fmt.Sprintf("Type(%d)", uint8(t))
}

// Packet is one decoded or to-be-encoded datagram.
//
// For Ack and FinAck, the acknowledged sequence number is carried in Ack.
// For Start, Data, Fin, and Error the sequence number is carried in Seq.
type Packet struct {
	Type    Type
	Session uint64
	Seq     uint32
	Ack     uint32
	Payload []byte
}

// Errors returned by Encode and Decode. Test with errors.Is.
var (
	ErrTruncated = errors.New("protocol: truncated datagram")
	ErrOversized = errors.New("protocol: oversized datagram")
	ErrMagic     = errors.New("protocol: bad magic")
	ErrVersion   = errors.New("protocol: unsupported version")
	ErrType      = errors.New("protocol: unknown packet type")
	ErrLength    = errors.New("protocol: declared payload length mismatch")
	ErrChecksum  = errors.New("protocol: CRC mismatch")
	ErrShape     = errors.New("protocol: invalid fields for packet type")
)

// Encode writes p as one datagram and returns the encoded bytes.
//
// The result reuses dst's backing array when cap(dst) holds the datagram, so a
// caller that passes buf[:0] with cap MaxDatagram encodes without allocating.
// Otherwise a new slice is allocated. On error, Encode returns nil and no bytes
// are written.
func Encode(dst []byte, p Packet) ([]byte, error) {
	if err := validate(p.Type, p.Session, p.Seq, p.Ack, len(p.Payload)); err != nil {
		return nil, err
	}
	n := HeaderSize + len(p.Payload)
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}
	// Copy the payload first: it may alias dst, and the header writes below
	// touch dst[:HeaderSize], which would clobber an aliased source.
	copy(dst[HeaderSize:], p.Payload)
	binary.BigEndian.PutUint16(dst[offMagic:], Magic)
	dst[offVersion] = Version
	dst[offType] = byte(p.Type)
	binary.BigEndian.PutUint64(dst[offSession:], p.Session)
	binary.BigEndian.PutUint32(dst[offSeq:], p.Seq)
	binary.BigEndian.PutUint32(dst[offAck:], p.Ack)
	binary.BigEndian.PutUint16(dst[offLen:], uint16(len(p.Payload)))
	binary.BigEndian.PutUint32(dst[offCRC:], checksum(dst[:offCRC], dst[HeaderSize:]))
	return dst, nil
}

// Decode parses and validates one datagram.
//
// Decode does not copy: the returned Payload aliases b and is valid only until
// b is reused. Copy it if it must outlive the buffer. Decode performs no heap
// allocation on success.
func Decode(b []byte) (Packet, error) {
	if len(b) < HeaderSize {
		return Packet{}, ErrTruncated
	}
	if len(b) > MaxDatagram {
		return Packet{}, ErrOversized
	}
	if binary.BigEndian.Uint16(b[offMagic:]) != Magic {
		return Packet{}, ErrMagic
	}
	if b[offVersion] != Version {
		return Packet{}, ErrVersion
	}
	t := Type(b[offType])
	if !t.valid() {
		return Packet{}, fmt.Errorf("%w: %d", ErrType, b[offType])
	}
	declared := int(binary.BigEndian.Uint16(b[offLen:]))
	if have := len(b) - HeaderSize; have < declared {
		return Packet{}, ErrTruncated
	} else if have > declared {
		return Packet{}, ErrLength
	}
	if binary.BigEndian.Uint32(b[offCRC:]) != checksum(b[:offCRC], b[HeaderSize:]) {
		return Packet{}, ErrChecksum
	}
	p := Packet{
		Type:    t,
		Session: binary.BigEndian.Uint64(b[offSession:]),
		Seq:     binary.BigEndian.Uint32(b[offSeq:]),
		Ack:     binary.BigEndian.Uint32(b[offAck:]),
		Payload: b[HeaderSize:],
	}
	if err := validate(p.Type, p.Session, p.Seq, p.Ack, len(p.Payload)); err != nil {
		return Packet{}, err
	}
	return p, nil
}

// checksum is CRC32 IEEE over the header prefix (everything before the CRC
// field) followed by the payload.
func checksum(header, payload []byte) uint32 {
	return crc32.Update(crc32.ChecksumIEEE(header), crc32.IEEETable, payload)
}

// validate enforces the per-type field rules shared by Encode and Decode.
func validate(t Type, session uint64, seq, ack uint32, payloadLen int) error {
	if !t.valid() {
		return fmt.Errorf("%w: %d", ErrType, uint8(t))
	}
	if payloadLen > MaxPayload {
		return fmt.Errorf("%w: payload %d exceeds %d", ErrOversized, payloadLen, MaxPayload)
	}
	if session == 0 {
		return fmt.Errorf("%w: session must be nonzero", ErrShape)
	}
	var ok bool
	switch t {
	case Start, Error:
		ok = seq == 0 && ack == 0 && payloadLen > 0
	case Data:
		ok = seq >= 1 && ack == 0 && payloadLen > 0
	case Ack:
		ok = seq == 0 && payloadLen == 0
	case Fin:
		ok = seq >= 1 && ack == 0 && payloadLen == 0
	case FinAck:
		ok = seq == 0 && ack >= 1 && payloadLen == 0
	}
	if !ok {
		return fmt.Errorf("%w: %s with seq=%d ack=%d payload=%d", ErrShape, t, seq, ack, payloadLen)
	}
	return nil
}
