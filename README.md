# RUDP-Go

A standard-library-only Go file-transfer protocol over UDP for BCSE308L Computer Networks. Selective Repeat ARQ supplies per-packet ACKs, a bounded sliding window, retransmission timers, ordered disk output, and SHA-256 verification. `--window 1` runs the same engine as Stop-and-Wait.

This is an educational, QUIC-inspired protocol—not QUIC, HTTP/3, or WebTransport. There is no authentication, TLS, congestion control, multiplexing, or production internet-safety claim. Use localhost or an isolated lab network.

## Build

Go 1.24 or newer; no external dependencies.

```bash
go build -o rudp ./cmd/rudp
go test ./...
go test -race ./...
go vet ./...
```

## Two-terminal demonstration

Generate a small binary file once:

```bash
dd if=/dev/urandom of=sample.bin bs=1024 count=64 status=none
```

Terminal 1:

```bash
./rudp receive --listen 127.0.0.1:9000 --out ./downloads --loss 0.10 --seed 42 --idle 2m
```

Terminal 2:

```bash
./rudp send --to 127.0.0.1:9000 --file ./sample.bin --window 32 --loss 0.10 --seed 7
sha256sum sample.bin downloads/sample.bin
cmp sample.bin downloads/sample.bin
```

Both processes report the verified SHA-256. The receiver publishes only verified bytes, retains the completed session for five seconds to recover lost FINACKs, then exits. Start a new receiver for each transfer. For the Stop-and-Wait comparison, use a different destination directory and `--window 1` on the sender. Existing destinations are refused unless the receiver explicitly uses `--overwrite`.

## Commands and controls

`./rudp send --help`, `./rudp receive --help`, and `./rudp bench --help` list all options. Flags follow the subcommand; positional file arguments are not accepted.

| Option | Default | Meaning |
|---|---|---|
| `--window` | `32` | Sender sequence-window width, or receiver reorder capacity; range 1–128 |
| `--loss` | `0` | This endpoint's outgoing drop probability, range 0–0.3 |
| `--seed` | `1` | Private PCG loss-generator seed |
| `--rto` | `500ms` | Fixed sender retransmission timeout, range 1ms–1m |
| `--retries` | `15` | Retransmissions per packet after its initial attempt, range 0–1000 |
| `--timeout` | `0` | Whole-command deadline; zero disables it |
| `--json` | off | Emit successful measured statistics as JSON on stdout; diagnostics use stderr |
| receive `--idle` | `15s` | Timeout waiting for START or further packets from the selected peer |
| receive `--linger` | `5s` | Completed-session retention; minimum five seconds |
| receive `--overwrite` | off | Atomically replace an existing destination after verification |

Sender defaults: `--to 127.0.0.1:9000`; `--file` is required. Receiver defaults: `--listen 127.0.0.1:9000 --out downloads`. Output directories are created automatically. Partial files are private, randomly named `.rudp-*.part` files in that directory; failures, cancellation, and idle expiry remove them. SIGINT/SIGTERM cancel cleanly. Force-kill or machine failure can leave a partial file; it is never treated as a completed destination.

Set receiver capacity at least as large as the sender window when comparing performance. For example, use `receive --window 128` for a sender using `--window 128`. There is no flow-control negotiation; packets outside the receiver window are discarded without ACK and can later be retried.

Loss applies to **all outgoing datagrams**, including START/FIN at the sender and ACK/FINACK at the receiver. Seeds reproduce each endpoint's random decision stream, not OS scheduling or exact transfer duration. If both endpoints drop with probability `p`, a DATA/ACK exchange survives with probability `(1-p)^2` before real socket loss. A finite retry budget can still fail at high loss.

Timers are scanned every `min(25ms, rto)`. For very large RTOs, set receiver `--idle` beyond the sender's retry horizon and `--linger` beyond the expected FIN retry horizon. The receiver does not know the sender's timer settings. A short whole-command deadline can expire on a correct but slow lossy transfer.

## Reproducible experiment

The benchmark starts actual UDP sender/receiver engines on separate ephemeral IPv4 loopback sockets, generates a deterministic binary fixture unless `--file` is provided, verifies every transfer, and reports per-setting medians. Receiver capacity is 128. Successful trials explicitly cancel completed receiver linger after the sender has received FINACK; linger is exercised separately in live-process verification.

Quick comparison:

```bash
./rudp bench --size 65536 --windows 1,32 --losses 0,0.10,0.20 --repeats 3 --rto 10ms
```

Reproduce the committed measurement matrix:

```bash
./rudp bench --size 10485760 --windows 1,8,32 --losses 0,0.05,0.10,0.20 --repeats 3 --rto 5ms --seed 7 --json --timeout 10m > results.json
```

The experiment deliberately shortens RTO for localhost runtime; **the protocol/CLI default remains 500ms**. Default benchmark settings are a 10 MiB fixture, windows 1/8/32, four loss levels, three repeats, and 500ms RTO; that full default experiment can take a long time. Goodput includes sender-side source hashing and completion, not fixture generation or receiver linger. There is no injected RTT/delay; these are not WAN throughput predictions.

See [protocol and architecture](docs/protocol.md), [measurements and methodology](docs/results.md), [short report and live-demo script](docs/report.md), and [fixture instructions](testdata/README.md). Raw evidence is in `docs/benchmark.json` and `docs/smoke.json`.

See [OUTPUT.md](OUTPUT.md) for fresh Ghostty terminal screenshots and complete build, transfer, checksum, and benchmark transcripts.

## Verification coverage

Unit tests validate the binary layout, CRC, bounds, packet field rules, borrowed decoding buffers, and reproducible outgoing loss. Localhost integration tests cover 0-byte, 1 KiB, and 10 MiB files at 0/5/10/20% loss at both endpoints, plus windows 1/8/32/128. Scripted peers exercise selective retransmission, duplicate/out-of-order ACKs and DATA, lost FINACK, forged peer/session IDs, malformed/corrupt/oversized packets, wrong chunk sizes, receive-window bounds, safe filenames, overwrite races, checksum failure, cancellation, and retry/idle limits.

Unimplemented stretch goals: dashboard, delay/jitter injection, Go-Back-N baseline, adaptive RTO, and congestion control.
