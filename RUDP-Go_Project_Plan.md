# RUDP-Go: Reliable File Transfer over UDP

## 1. Project overview

**Course:** BCSE308L — Computer Networks  
**Project type:** Go networking application with a reproducible protocol experiment  
**Working title:** RUDP-Go: A Reliable Transport Protocol on Top of UDP

### Problem statement
UDP transports datagrams without guaranteeing their delivery, ordering, or uniqueness. Build a small **reliable file-transfer protocol over UDP** that implements reliability in user space, and demonstrate how sliding windows and retransmissions behave under packet loss.

### Scope statement
This is an educational **QUIC-inspired transport**, **not QUIC, HTTP/3, or WebTransport**. It will not implement TLS, stream multiplexing, sophisticated congestion control, or an internet-safe production protocol.

### Definition of done
Two Go CLI processes can transfer a file over UDP, even with 10–20% simulated packet loss, and verify that the received file is byte-for-byte identical. A comparison run with a one-packet window and a larger sliding window provides measurable results. The entire demo works on localhost.

## 2. MVP features

1. Sender and receiver processes communicating with Go's `net.UDPConn`.
2. A compact binary packet format with encode/decode and validation.
3. File splitting into small datagrams (UDP payload <= 1200 bytes, including the protocol header).
4. Transfer initiation with filename, file size, and SHA-256 metadata.
5. **Selective Repeat ARQ**: per-packet sequence numbers, individual ACKs, sliding sender window, out-of-order receive buffer, and per-packet retransmission timeout.
6. `--window 1` to demonstrate **Stop-and-Wait** using the same implementation.
7. Reliable transfer completion with FIN/FINACK, a short linger period for duplicate FIN packets, checksum verification, and safe file rename.
8. Configurable, reproducible outgoing packet loss at either endpoint.
9. CLI metrics: elapsed time, goodput, datagrams sent, retransmissions, and received-file SHA-256 verification.
10. Automated unit and integration tests.

### Deliberately not in the MVP
WebTransport, HTTP/3, full QUIC, browser-native UDP, TLS, multiplexing, NAT traversal, adaptive congestion control, peer discovery, multi-user hosting, or a required frontend.

## 3. System architecture

```text
  SENDER (Go)                                RECEIVER (Go)
  file -> chunker                            UDP socket -> decode
          |                                         |
          v                                         v
  Selective Repeat sender   <--- UDP --->   window / reorder buffer
     - seq numbers           DATA/ACK          - validate CRC
     - inflight window                         - ACK each valid packet
     - timers/retransmit                       - flush ordered bytes
          |                                         |
      stats collector                           temporary file
                                                    |
                                               SHA-256 verify
                                                    |
                                               atomic rename

  Optional network fault injector on each outgoing UDP send:
  seeded random packet drop, including DATA and ACK datagrams.
```

The sender has one goroutine reading incoming UDP ACKs into a channel and one event loop that owns its send-window state, controls new sends, and scans retransmission deadlines. The receiver can use one bounded event loop. Avoid one goroutine per packet.

## 4. Protocol design (Version 1)

### Datagram constraints
- Set maximum UDP datagram payload to **1200 bytes** to avoid depending on fragmentation across typical IPv4/IPv6 paths.
- Use **26-byte** fixed-size protocol headers and up to **1174 data bytes** in a DATA datagram.
- Encode multibyte integers in big-endian order (`encoding/binary`).

### Header, 26 bytes

| Offset | Field | Type | Purpose |
|---|---|---|---|
| 0 | Magic | uint16 | Fixed identifier, e.g. `0x5255` |
| 2 | Version | uint8 | `1` |
| 3 | Type | uint8 | START, DATA, ACK, FIN, FINACK, ERROR |
| 4 | Session ID | uint64 | Random transfer identifier |
| 12 | Seq | uint32 | Sequence number for START/DATA/FIN |
| 16 | Ack | uint32 | Exact packet sequence acknowledged by ACK/FINACK |
| 20 | Payload length | uint16 | Declared number of payload bytes |
| 22 | CRC32 | uint32 | CRC32 IEEE over the header excluding CRC field, plus payload |

Reject malformed/truncated datagrams, bad magic/version, incorrect declared lengths, oversized datagrams, and CRC mismatch. CRC detects accidental corruption; it is **not cryptographic authentication**.

### Packet types and sequence numbers
- `START (seq=0)`: JSON metadata containing sanitized basename, file size, SHA-256, and chunk size. Receiver creates a `.part` file and responds `ACK(0)`.
- `DATA (seq=1..N)`: binary payload with up to 1174 file bytes. Receiver acknowledges every valid accepted or already-written packet individually: `ACK(seq)`.
- `FIN (seq=N+1)`: sent only after all DATA packets are ACKed. Receiver verifies byte count and SHA-256, atomically publishes the completed file, and responds `FINACK(N+1)`.
- `ERROR`: optional short error description and an unsuccessful transfer state.
- Receiver must handle repeated START, DATA, and FIN idempotently. Keep completed-session state for at least 5 seconds so a lost FINACK can be recovered from a repeated FIN.

The receiver must associate the session with the original peer IP/port; ignore packets for other sessions/peers. Support **one active transfer at a time** for simplicity.

### Sender state machine

```text
INIT -> START_SENT -> TRANSFERRING -> FIN_SENT -> COMPLETE
                          |              |
                          +--------------+-> FAILED (retry limit / timeout / error)
```

1. Compute file SHA-256 and create a cryptographically random session ID.
2. Send START; retry on timeout until ACK(0) or retry budget exceeded.
3. Fill the send window with up to W outstanding DATA packets.
4. Track each outstanding packet's send timestamp, retry count, and ACK status.
5. On ACK(seq), mark that exact packet as acknowledged. Slide `base` forward past consecutive acknowledged sequence numbers. Fill newly freed slots.
6. Every 25 ms, retransmit each unacknowledged packet whose 500 ms timeout has expired. Stop after a bounded retry count (e.g. 15) and report failure.
7. After all DATA packets are acknowledged, send FIN and await FINACK, retrying FIN if needed.
8. Report duration, goodput, original packets, retransmissions, and result.

**Important:** Track sequence numbers in the window correctly; ACKs may arrive out of order, be duplicated, or be lost. Reusing Go maps of in-flight packets is fine for this coursework scope.

### Receiver behavior
1. Validate START. Sanitize the filename with `filepath.Base`; reject path traversal and refuse to overwrite an existing destination unless explicitly allowed.
2. Create a temporary `.part` file for the transfer.
3. For each DATA packet, discard invalid CRC; discard datagrams outside the bounded receive window, re-ACK duplicates that were already received, and buffer valid out-of-order data.
4. Write contiguous chunks to disk starting with the next expected sequence number. Never retain the entire file in memory.
5. ACK each valid DATA packet once accepted into the bounded receive buffer or recognized as a duplicate.
6. On FIN, ensure the expected byte count has been written and SHA-256 matches START metadata; close and rename the `.part` file, then send FINACK. Re-ACK repeated FIN while the completed session is retained.
7. Clean up partial files on terminal errors; handle zero-byte files.

### Fixed configuration for MVP
- `max_datagram_bytes = 1200`
- `max_data_payload = 1174`
- `window = 32` by default; allow 1–128 via CLI
- `rto = 500ms` fixed; make configurable
- `retry_limit = 15`
- `loss = 0.0` by default; allow 0–0.3 in demos
- `session_linger = 5s`

## 5. Go repository structure

```text
rudp-go/
├── go.mod
├── README.md
├── cmd/
│   └── rudp/
│       └── main.go                # CLI send / receive / bench
├── internal/
│   ├── protocol/
│   │   ├── packet.go              # types + encode/decode + CRC32
│   │   └── packet_test.go
│   ├── transport/
│   │   ├── sender.go              # Selective Repeat window + retransmission
│   │   ├── receiver.go            # reordering + disk writing
│   │   ├── session.go             # START / FIN state and metadata
│   │   └── transfer_test.go       # localhost integration tests
│   ├── network/
│   │   └── loss.go                # seeded drop simulator wrapping sends
│   └── metrics/
│       └── stats.go               # counters + transfer summary
├── testdata/
│   └── README.md                  # how to generate fixture files
└── docs/
    ├── protocol.md                # format + state machines
    └── results.md                 # measurements / tables / conclusions
```

Use the Go standard library for the MVP: `net`, `os`, `io`, `flag`, `encoding/binary`, `encoding/json`, `crypto/rand`, `crypto/sha256`, `hash/crc32`, `time`, `context`, and `testing`. No external dependencies are required.

## 6. CLI contract

Compile with:

```bash
go build -o rudp ./cmd/rudp
```

Start the receiver:

```bash
./rudp receive --listen 127.0.0.1:9000 --out ./downloads --loss 0.10 --seed 42
```

Send a file:

```bash
./rudp send --to 127.0.0.1:9000 --file ./sample.bin --window 32 --loss 0.10 --seed 7
```

Benchmark Stop-and-Wait using the same engine:

```bash
./rudp send --to 127.0.0.1:9000 --file ./sample.bin --window 1 --loss 0.10 --seed 7
```

The `--loss` option affects **outgoing datagrams from the endpoint where it is specified**, so receiver loss drops ACK/FINACK packets; sender loss drops START/DATA/FIN packets. Seeds make runs reproducible. Log a summary such as:

```text
Transfer completed
File: sample.bin (10.00 MiB)
Window: 32  Drop rate configured: 10%
Elapsed: ...   Goodput: ... MiB/s
Sent DATA packets: ...   Retransmitted DATA packets: ...
SHA-256: MATCH
```

All numeric output is measured, never pre-filled or simulated in the final result.

## 7. Implementation milestones

### Milestone 1 — Working UDP transfer (Day 1)
- Set up Go module and CLI.
- Implement packet encoder/decoder and CRC tests.
- Add START, DATA, ACK, FIN, FINACK.
- Support **Stop-and-Wait** (`window=1`) with timeouts and retries.
- Transfer a small binary file correctly over localhost.

**Exit criterion:** A file survives transfer at 0% loss, validates SHA-256, and both programs exit cleanly.

### Milestone 2 — Selective Repeat (Day 2)
- Generalize sender from one in-flight packet to a bounded sliding window.
- Buffer and ACK out-of-order DATA at receiver.
- Handle duplicate and lost ACKs.
- Implement retry caps, duplicate FIN/FINACK handling, and receiver `.part` file cleanup.

**Exit criterion:** Reliable transfer at 10% data loss and 10% ACK loss with window=32.

### Milestone 3 — Loss injection, tests, measurements (Day 3)
- Implement seeded, configurable drop simulation.
- Add metrics and machine-readable `--json` output if useful.
- Test empty, tiny, and >=10 MiB files; loss rates 0%, 5%, 10%, 20%.
- Compare `--window 1` against `--window 32` under the same settings.
- Run `go test ./...` and `go test -race ./...`.

**Exit criterion:** No hangs, verified checksums, bounded retries, and repeatable benchmark data.

### Milestone 4 — Polish / course demonstration (Day 4, optional)
- Add a live CLI progress display (bytes acknowledged, active packets, retransmission counter).
- Export CSV/JSON results, plot throughput against loss/window size.
- Add a one-page architecture diagram and protocol explanation.
- Prepare the 3-minute demo and report.

**Optional stretch:** Add a small Go `net/http` dashboard (polling or SSE), network delay/jitter injection, a Go-Back-N baseline, or adaptive RTO. None is required for the MVP.

## 8. Verification matrix

| Test | Expected behavior |
|---|---|
| Encode/decode round trip | Headers and payload preserved |
| Bad magic/version/CRC/length | Packet rejected safely |
| 0-byte file | START + FIN complete; hashes match |
| 1 KiB and 10 MiB files | Exact bytes and hash match |
| Packet lost | Only missing packet retransmitted |
| ACK lost | Sender retries; receiver deduplicates |
| DATA received out of order | Receiver buffers and writes in order |
| FINACK lost | Duplicate FIN triggers duplicate FINACK |
| Window 1 vs 32 | Both correct; compare measured goodput |
| Retry limit reached | Clean failure, no infinite hang |
| Unexpected source/session | Datagram ignored |
| Existing output filename | Refuse overwrite by default |

Use seeded test runs on localhost. Note: successful completion at high loss is expected **when the retry budget is sufficient**, not absolutely guaranteed for every random loss pattern.

## 9. Experiment and presentation

**Research question:** How does the sender's sliding-window size affect throughput and retransmission overhead as packet loss increases?

Independent variables: `window ∈ {1, 8, 32}` and configured outgoing loss `∈ {0%, 5%, 10%, 20%}`. Hold file size (e.g. 10 MiB), timeout, and test setup constant. Repeat each setting several times and report median transfer duration, goodput, and retransmission count. For stronger window comparisons, introduce repeatable RTT using Linux `tc netem` or a correctly asynchronous delay shim; localhost RTT alone may hide the window's benefit.

### Suggested demo (3–4 minutes)
1. Explain that UDP does not guarantee reliable ordered delivery.
2. Show the 26-byte packet format and sender/receiver state flow.
3. Send a file over localhost with `--window 1`, 10% loss; show retransmissions and matching SHA-256.
4. Repeat with `--window 32`; explain selective ACKs and pipelining.
5. Present a small benchmark chart and conclusions; explicitly distinguish your protocol from production QUIC.

### Expected coursework alignment
- **Module 2:** packet switching and performance parameters.
- **Module 3:** error detection (CRC), flow control, sliding windows, and Selective Repeat ARQ.
- **Module 6:** TCP vs UDP characteristics, transport reliability, traffic/performance measurements.
- **Module 7 (only if dashboard is added):** HTTP application-layer service.

Do not claim congestion control, WebTransport compatibility, or TLS security unless you actually implement them.

## 10. Deliverables

- `rudp` Go executable / source repository.
- README with build and two-terminal demo commands.
- Protocol specification and architecture diagram.
- Automated tests with checksums / packet-loss scenarios.
- Benchmark table or graph, plus short methodology.
- Short report and live demo.

## 11. Build rules for an implementation agent

- Prioritize correctness and completion of the MVP over UI polish.
- Keep the protocol small and well documented; do not pull in QUIC libraries.
- Never ACK corrupted or unaccepted data.
- Do not use global shared sender-window maps without synchronization. Prefer single-owner event loops.
- Bound all packet sizes, windows, retries, queues, and file output paths.
- Always verify final file bytes using SHA-256.
- Preserve `.part` output until verification succeeds; never publish a corrupt file.
- Use a fixed seed for reproducible loss experiments.
- Add tests after each milestone and ensure `go test ./...` passes before moving on.
- Mark unimplemented ideas as stretch goals rather than pretending to support them.
