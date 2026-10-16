# Measured results

## Methodology

Command actually run:

```bash
./rudp bench --size 10485760 --windows 1,8,32 --losses 0,0.05,0.10,0.20 --repeats 3 --rto 5ms --seed 7 --json --timeout 10m
```

- Environment: Linux/amd64, Go `go1.27.1-X:nodwarf5`; standard library only. Ordinary workstation scheduling, no CPU isolation or synthetic RTT/delay/bandwidth limit.
- Fixed fixture: 10 MiB (10,485,760 bytes), generated from the documented ChaCha8 seed. SHA-256: `303ef582a140a14bce7346a17072dd27b7977903a6a6a37e0ad015e9186792ec`.
- Chunk size: 1174 bytes; 8932 original DATA packets per trial. Sender windows are 1, 8, and 32; receiver capacity remains 128.
- Loss is independently injected at **both** endpoints. Each repeat uses sender seeds 7/8/9 and receiver seeds 100007/100008/100009, reused across window settings. Loss drops control datagrams as well as DATA/ACK.
- RTO is held at **5ms**, retry limit at 15. This is an explicit localhost experiment override; normal CLI and benchmark default RTO is still **500ms**. Timers are scanned at the configured 5ms interval, so scheduling and scan quantization affect actual retry timing.
- One sender event loop and ACK-reader goroutine communicate with an actual UDP receiver event loop on ephemeral loopback sockets. No mocked packet delivery or pre-filled metrics.
- Duration is sender wall time from opening/hashing the source through successful completion and reader shutdown. Goodput is file bytes/duration, converted to MiB/s below. Fixture creation and completed receiver linger are excluded.
- After FINACK reaches the sender, the benchmark explicitly cancels the completed receiver rather than waiting five seconds per trial. Separate CLI runs verify normal linger and process exit.
- Three repeats per setting, reporting independent medians of duration, goodput, and total sender retransmissions (START + DATA + FIN). All 36 transfers succeeded with matching sender/receiver SHA-256. [Raw trials and summaries](benchmark.json) retain endpoint counters, seeds, timing, hash, and verification state.

## Comparison

Configured loss below is per endpoint, not the combined DATA/ACK exchange loss. `MiB = 2^20 bytes`.

| Outgoing loss at each endpoint | Sender window | Median duration (s) | Median goodput (MiB/s) | Median retransmissions |
|---|---|---|---|---|
| 0% | 1 | 0.718241 | 13.922899 | 0 |
| 0% | 8 | 0.243804 | 41.016571 | 0 |
| 0% | 32 | 0.235420 | 42.477363 | 0 |
| 5% | 1 | 9.534266 | 1.048848 | 946 |
| 5% | 8 | 5.830872 | 1.715009 | 946 |
| 5% | 32 | 2.750167 | 3.636143 | 946 |
| 10% | 1 | 20.785548 | 0.481104 | 2076 |
| 10% | 8 | 10.069498 | 0.993098 | 2076 |
| 10% | 32 | 4.179773 | 2.392474 | 2076 |
| 20% | 1 | 50.260235 | 0.198964 | 5024 |
| 20% | 8 | 19.000379 | 0.526305 | 5024 |
| 20% | 32 | 7.420505 | 1.347617 | 5024 |

## Interpretation

A wider window improved goodput in every measured loss setting by overlapping independent outstanding packets and their timeout stalls. The no-loss benefit diminishes between the larger windows on this loopback setup. With Stop-and-Wait, every lost exchange stalls the only outstanding packet.

Retransmission totals happened to match across window sizes for each repeat in this run. Reusing each endpoint's seeded decision stream produced the same aggregate drops/retries; it does **not** imply the same logical packet was dropped or delivered at the same instant across windows. Wider windows improved scheduling overlap, not the configured loss probability. Other seeds, timing, real socket drops, or receive-capacity constraints can change retransmission totals.

Increasing outgoing loss decreases goodput and increases repair traffic. When both endpoints independently drop with probability `p`, the initial DATA/ACK exchange succeeds with probability `(1-p)^2`, not `1-p`. These are application-level goodput results, not wire bandwidth measurements or evidence of congestion control.

## Verification evidence and limits

The final integration suite transfers 0-byte, 1 KiB, and 10 MiB inputs across 0/5/10/20% loss at both endpoints and additionally exercises windows 1/8/128. It independently compares bytes, checks hashes, counts distinct accepted data, and checks partial cleanup. Raw scripted-peer tests cover reordering, corruption, malformed frames, window bounds, exact selective retry, lost FINACK, peer/session isolation, overwrite races, cancellation, and bounded retry/idle failure.

Observed checks:

```text
go build -o rudp ./cmd/rudp    PASS
go test ./...                 PASS
go test -race ./...           PASS
go vet ./...                  PASS
```

[Live-process evidence](smoke.json) records separate CLI processes transferring empty and tiny files with the default timer and a 10 MiB file at 20% outgoing loss on each endpoint with 10ms RTO. Each successful run independently compared bytes and SHA-256, left no partial file, and let the receiver finish its normal five-second linger. The normal text summary and invalid CLI numeric settings were also exercised.

An earlier 10 MiB lossy smoke attempt at the default 500ms RTO exceeded an explicit 60-second harness deadline. It is recorded as a timeout, not a successful measurement. The successful large-file smoke and comparison matrix use the timer settings stated above. Do not apply their timings to the conservative default or shorten that deadline and assume a protocol failure.

Limitations: only one workstation and loopback path, three repeats, no injected RTT or congestion, no confidence intervals, and fixed seeds. PRNG decisions are reproducible; timing is not deterministic. A stronger transport experiment should add repeatable asynchronous RTT (for example, isolated Linux `tc netem`) and more repeats. No such delay is claimed here.
