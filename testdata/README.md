# Binary fixtures

Large fixtures are generated locally, not committed.

```bash
# Empty file.
touch empty.bin

# Binary files for the two-process CLI demonstration.
dd if=/dev/urandom of=tiny.bin bs=1024 count=1 status=none
dd if=/dev/urandom of=sample.bin bs=1048576 count=10 status=none
```

`rudp bench --size 10485760` creates its own temporary deterministic 10 MiB `fixture.bin`: `math/rand/v2.NewChaCha8` with seed bytes `{1, 3, 3, 7, 0, ...}`. Fixture generation is outside the measured transfer duration. Benchmark destinations and fixtures are removed on return. `--file PATH` benchmarks an existing regular file instead; `--size` only applies to generated fixtures.

Integration tests generate isolated 0-byte, 1 KiB, and 10 MiB binary inputs, plus window-comparison fixtures, in `testing.T.TempDir`. Tests independently compare received bytes and transfer SHA-256 values. No shared port, output directory, or checked-in large binary is required.

For an independent manual check after a CLI transfer:

```bash
cmp sample.bin downloads/sample.bin
sha256sum sample.bin downloads/sample.bin
```
