# lmio-archive-grep

A grep-like tool for searching the _TeskaLabs LogMan.io_ `lmio-receiver` disk archive.

It reads partition data directly from the on-disk archive — no ZooKeeper, no receiver API.
It understands the receiver's column files in both uncompressed (hot, warm) and xz-compressed (cold) form, and supports time-range filtering,
literal or regular-expression search on the raw log line, stream filters, source/timestamp display, JSON output, and a match limit.

## Build & test

Needs a Go toolchain (the module declares `go 1.27.1`) and the single
dependency `github.com/ulikunitz/xz`.

```sh
# Build the binary into the current directory
go build -o lmio-archive-grep .

# Run the unit test suite
go test ./...
```


## Usage

```
lmio-archive-grep \
  -root /data/ssd/lmio-receiver-1 -root /data/hdd/lmio-receiver-1 \
  -stream received.plus.palo-alto \
  -from 2026-09-15T12:00:00Z -to 2026-09-15T13:00:00Z \
  -literal 'PAN-OS' \
  -show
```

Options:

| Flag | Meaning |
|------|---------|
| `-root <dir>` | Archive root that contains the `hot`/`warm`/`cold` phase dirs. Repeatable. Default: auto-detect `/data`, `/data/ssd/lmio-receiver-1`, `/data/hdd/lmio-receiver-1` |
| `-stream <pattern>` | Stream filter: substring or glob (e.g. `received.plus.*`) |
| `-from`, `-to` | Time range, inclusive on `collected_at`. Accepts RFC3339 (`2026-09-15T12:00:00Z`), `YYYY-MM-DD`, unix seconds, or raw microseconds |
| `-literal <s>` | Literal substring search in the raw line |
| `-pattern <re>` | Go regular expression search in the raw line |
| `-i` | Case-insensitive literal search |
| `-show` | Also print `collected_at` and `source` columns |
| `-json` | Emit JSON lines |
| `-max <n>` | Stop after n matches |
| `-list-streams` | List streams present on disk (by phase) and exit |

Time range is applied in two stages: a coarse partition pre-filter via `summary.yaml`
(or the `collected_at` column when the summary is absent, i.e. an open hot partition),
then a per-row filter on `collected_at`.
A partition whose `collected_at` is outside the window is skipped without decompressing its raw column.


## Deployment notes

- Hot files are mode `0640 root:root`; run the tool with `sudo` on the receiver host, or as the user that owns the receiver's data directories.
- The two data roots on a typical dual-disk deployment are `/data/ssd/lmio-receiver-1` (hot) and `/data/hdd/lmio-receiver-1` (warm, cold). Pass both `-root` flags.

## Verification

- Sample partition `received.plus.fortinet-fortigate-1/adijdi.part` (cold, xz): tool reports 57469 rows and `collected_at` min `2026-10-12T08:44:49.615476Z`, both matching `summary.yaml`.
- Live hot partition `received.system.nginx/adjecf.part` (uncompressed, no summary): scanned via the `collected_at` column, source decodes to `127.0.0.1 43171 D`.
- Multi-token corrupted partition `received.supertenant.asab/adiokh.part` (cold, xz): both sources `127.0.0.1 34902 S` (3 rows) and `127.0.0.1 49344 S` (93 rows) decode correctly, 96 total = `summary.yaml`.


## On-disk format (from receiver source, verified against fusion data)

Each partition is a directory `<stream>/<partno>.part` containing column files:

| File | Column type | Layout |
|------|-------------|--------|
| `col-raw.data` / `.pos` | `string` (raw log line) | `.pos`: one 4-byte LE cumulative end-offset per row; `.data`: concatenated records. Record `i` spans `pos[i-1]..pos[i]` |
| `col-collected_at.data` | `timestamp` | 8-byte LE microseconds since epoch per row |
| `col-received_at.data` | `timestamp` | same |
| `col-source.data` | `token:rle` | pairs of (tokenID u16, count u16) |
| `col-source-token.data` / `.pos` | `string` sub-column | token ID `n` → record `n-1` of the sub-column; ID 0 is the empty string |
| `summary.yaml` | — | informative min/max/count (receiver keeps the authoritative copy in ZooKeeper) |

Cold partitions store each file with a `.xz` suffix.
The reader auto-detects the suffix per column.

## License

Apache License 2.0. See [LICENSE](LICENSE).
