# RUDP-Go: short report and live demonstration

**Course:** BCSE308L — Computer Networks  
**Research question:** How does sliding-window size change file-transfer goodput and retransmission overhead as packet loss increases?

## Design and implementation

UDP supplies datagram boundaries but does not guarantee delivery, uniqueness, or order. RUDP-Go implements those missing properties in Go's user space. A versioned 26-byte binary header carries a random session ID, sequence and ACK numbers, payload length, and CRC32. Files are streamed as chunks no larger than 1174 bytes, keeping the complete UDP payload within 1200 bytes.

Selective Repeat stores a bounded sender window and independently acknowledges each accepted DATA packet. A missing packet is retransmitted after its own fixed timeout; already ACKed packets are not resent just because a lower sequence is missing. A bounded receiver reorder buffer writes contiguous data to a private temporary file. The same algorithm becomes Stop-and-Wait with a one-packet window.

START exchanges basename, size, chunk size, and SHA-256. FIN is accepted only after all chunks have been written and the hash and byte count match. Atomic publication prevents a partial or corrupt file from appearing as the requested destination. The default no-overwrite path also protects against a destination created during transfer. Completed session retention allows recovery when FINACK is lost.

Each endpoint has its own seeded outgoing loss generator. Receiver loss therefore exercises lost ACKs and FINACKs, not just lost file data. Counters distinguish original DATA attempts, timer-driven retries, outgoing attempts, and simulated drops. The [protocol specification](protocol.md) contains the architecture diagram, exact layout, state transitions, timing boundaries, and safety limits.

## Experiment and evidence

The [results and methodology](results.md) provide the measured window/loss comparison and its raw JSON. They hold file size, RTO, fixture, and endpoint seeds constant across window settings, repeat each setting, and report medians rather than guessed or pre-filled performance. Localhost runs do not simulate WAN RTT, delay, bandwidth, or congestion.

Validation uses binary byte comparisons and SHA-256, not just successful process exits. The automated suite covers corruption, truncation, out-of-order delivery, duplicates, exact selective retransmission, dropped FINACK, foreign peers/sessions, output races, cancellation, bounded retries, and partial cleanup. The size/loss matrix includes empty, tiny, and 10 MiB files. Separate live CLI processes also complete normally, including the receiver's full FIN linger; evidence is retained in `smoke.json`.

An initial large-file smoke attempt used a 60-second command deadline with the conservative default 500ms RTO and hit that deadline under loss. The completed large-file live verification used 10ms RTO; the committed comparison experiment uses 5ms RTO for practical localhost runtime. These explicit experimental overrides do not change the 500ms default. Fixed timeout dominates loss-recovery costs, so timings from different RTO settings must not be directly compared.

## Three-minute presentation script

Prepare beforehand: build `rudp`, generate the 64 KiB fixture using the README, keep the measured results open, and start each receiver shortly before its sender. Use separate output directories for the two live runs.

1. **0:00–0:35 — Problem and format.** Explain UDP's missing reliability guarantees. Show the header and architecture diagram in `protocol.md`. CRC detects accidental packet corruption; SHA-256 verifies the finished file.
2. **0:35–1:25 — Stop-and-Wait.** In terminal 1 run `./rudp receive --out demo-w1 --loss 0.10 --seed 42 --idle 2m`. In terminal 2 run `./rudp send --file sample.bin --window 1 --loss 0.10 --seed 7 --rto 20ms`. Show retries and matching SHA-256; run `cmp sample.bin demo-w1/sample.bin`.
3. **1:25–2:15 — Sliding window.** Restart the receiver with `--out demo-w32` and the same loss/seed settings. Repeat the sender with `--window 32` and the same `--rto 20ms`. Show correct output and compare the two actual summaries. Demo timings are measured independently of the committed benchmark.
4. **2:15–3:00 — Results and limits.** Present the results table, explain that pipelining overlaps timeout stalls across independent packets, and note retransmission counts need not strictly fall with a wider window. Distinguish this protocol from QUIC: no TLS, congestion control, multiplexing, or browser compatibility. Leave the receiver running until its normal five-second linger ends.

Generate `sample.bin` as **64 KiB** for this timed demonstration; a 10 MiB Stop-and-Wait transfer at the default timeout can exceed the presentation slot. Existing demo output is deliberately not overwritten: choose fresh directories for another rehearsal or opt in explicitly with `--overwrite`.

## Coursework alignment and limitations

- **Module 2:** packet switching, elapsed time, throughput/goodput, and experimental variables.
- **Module 3:** CRC error detection, sliding windows, Selective Repeat ARQ, sequence numbers, and retransmission timers.
- **Module 6:** TCP versus UDP transport behavior and application-level reliability.

The implementation supports one file and one peer per receiver invocation, bounded windows, and finite retry budgets. It is not an authenticated or congestion-safe internet transport. Seeds reproduce PRNG decisions, not operating-system scheduling. Linger and timeout settings are not negotiated. Larger RTO settings require an appropriately longer receiver idle/linger budget. Dashboard, delay injection, Go-Back-N, and adaptive RTO remain unimplemented stretch goals.
