// lmio-archive-grep — grep-like search tool for the lmio-receiver disk archive.
//
// It reads partition data directly from the on-disk archive (hot/warm/cold),
// without ZooKeeper. It supports compressed (xz) and uncompressed partitions,
// a time range, a byte/string pattern, and a stream filter.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ulikunitz/xz"
)

// ===== column decoding =====

// stringColumn reads a string column (raw, source-token) from a partition dir.
// It returns (offsets, data). Offsets[i] is the end offset of row i (exclusive).
// The pos file holds one 4-byte LE offset per row; the data file holds the
// concatenated record bytes. Empty records appear as equal consecutive offsets.
func stringColumn(dir, name string) ([]uint32, []byte, error) {
	posPath, dataPath, comp, err := resolveColumnFiles(dir, name)
	if err != nil {
		return nil, nil, err
	}
	posBytes, err := readAll(posPath, comp)
	if err != nil {
		return nil, nil, err
	}
	if len(posBytes)%4 != 0 {
		return nil, nil, fmt.Errorf("%s: pos file size %d is not a multiple of 4", posPath, len(posBytes))
	}
	offsets := make([]uint32, len(posBytes)/4)
	for i := range offsets {
		offsets[i] = uint32(posBytes[4*i]) | uint32(posBytes[4*i+1])<<8 | uint32(posBytes[4*i+2])<<16 | uint32(posBytes[4*i+3])<<24
	}
	data, err := readAll(dataPath, comp)
	if err != nil {
		return nil, nil, err
	}
	return offsets, data, nil
}

// timestampColumn reads a timestamp column: one 8-byte LE microseconds-since-epoch value per row.
func timestampColumn(dir, name string) ([]int64, error) {
	b, err := readColumnData(dir, name)
	if err != nil {
		return nil, err
	}
	if len(b)%8 != 0 {
		return nil, fmt.Errorf("column %s: data size %d is not a multiple of 8", name, len(b))
	}
	out := make([]int64, len(b)/8)
	for i := range out {
		out[i] = int64(uint64(b[8*i]) | uint64(b[8*i+1])<<8 | uint64(b[8*i+2])<<16 | uint64(b[8*i+3])<<24 |
			uint64(b[8*i+4])<<32 | uint64(b[8*i+5])<<40 | uint64(b[8*i+6])<<48 | uint64(b[8*i+7])<<56)
	}
	return out, nil
}

// tokenRLEColumn decodes a token:rle column.
// The RLE file holds pairs of (tokenID uint16, count uint16).
//
// Correctly-written files hold one pair per run: (token, length).
//
// Files written by the buggy receiver flush() hold, for every true run of
// token T with length L, a sequence of consecutive T-pairs with
// non-decreasing counts ending in L — the cumulative snapshots that
// flush() appended with os.pwrite() (which appends on an O_APPEND fd),
// plus the genuine run pair. Counts never decrease within one true run.
//
// Recovery: consecutive pairs of the same token with a non-decreasing
// count belong to the same run; the run length is the maximum count in
// the group. A token change or a count decrease starts a new run. This
// reproduces the exact original run stream for both formats.
// Token IDs resolve via the "<name>-token" string sub-column; ID 0 is the
// empty string.
func tokenRLEColumn(dir, name string) (tokens []string, err error) {
	rleBytes, err := readColumnData(dir, name)
	if err != nil {
		return nil, err
	}
	if len(rleBytes)%4 != 0 {
		return nil, fmt.Errorf("%s: rle file size %d is not a multiple of 4", name, len(rleBytes))
	}
	tokenOffsets, tokenData, err := stringColumn(dir, name+"-token")
	if err != nil {
		return nil, err
	}
	resolve := func(id uint16) string {
		if id == 0 {
			return ""
		}
		idx := int(id)
		if idx < 1 || idx >= len(tokenOffsets) {
			return fmt.Sprintf("<token:%d>", id)
		}
		lo := tokenOffsets[idx-1]
		hi := tokenOffsets[idx]
		if hi < lo {
			hi = lo
		}
		return string(tokenData[lo:hi])
	}

	// Decode runs from the pair stream (see the format note above).
	var runs []struct {
		id  uint16
		len int
	}
	i := 0
	n := len(rleBytes) / 4
	for i < n {
		id := uint16(rleBytes[4*i]) | uint16(rleBytes[4*i+1])<<8
		cnt := int(uint16(rleBytes[4*i+2]) | uint16(rleBytes[4*i+3])<<8)
		runLen := cnt
		prev := cnt
		i++
		for i < n {
			nid := uint16(rleBytes[4*i]) | uint16(rleBytes[4*i+1])<<8
			ncnt := int(uint16(rleBytes[4*i+2]) | uint16(rleBytes[4*i+3])<<8)
			if nid != id || ncnt < prev {
				break
			}
			if ncnt > runLen {
				runLen = ncnt
			}
			prev = ncnt
			i++
		}
		runs = append(runs, struct {
			id  uint16
			len int
		}{id, runLen})
	}
	for _, r := range runs {
		if r.len <= 0 {
			continue
		}
		tok := resolve(r.id)
		for i := 0; i < r.len; i++ {
			tokens = append(tokens, tok)
		}
	}
	return tokens, nil
}

// resolveColumnFiles finds the files for a column, tolerating xz-compressed
// variants (cold) and plain variants (hot/warm). It returns the pos path, data
// path, and whether xz compression is in use.
func resolveColumnFiles(dir, name string) (posPath, dataPath string, compressed bool, err error) {
	base := filepath.Join(dir, "col-"+name)
	posPlain := base + ".pos"
	dataPlain := base + ".data"
	posXz := base + ".pos.xz"
	dataXz := base + ".data.xz"

	_, errPos := os.Stat(posPlain)
	_, errData := os.Stat(dataPlain)
	_, errPosXz := os.Stat(posXz)
	_, errDataXz := os.Stat(dataXz)

	// xz variant (cold)
	if errDataXz == nil {
		return posXz, dataXz, true, nil
	}
	// plain variant (hot/warm)
	if errData == nil {
		return posPlain, dataPlain, false, nil
	}
	// Mixed: a token sub-column may exist as plain .pos + .data only.
	if errPosXz == nil && errDataXz == nil {
		return posXz, dataXz, true, nil
	}
	if errPos == nil && errData == nil {
		return posPlain, dataPlain, false, nil
	}
	return "", "", false, fmt.Errorf("column %q not found in %s", name, dir)
}

func readAll(path string, compressed bool) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if compressed {
		r, err := xz.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return io.ReadAll(r)
	}
	return io.ReadAll(f)
}

// readColumnData reads just the .data file of a column (used by token:rle,
// which has no separate .pos file).
func readColumnData(dir, name string) ([]byte, error) {
	dataPlain := filepath.Join(dir, "col-"+name+".data")
	dataXz := filepath.Join(dir, "col-"+name+".data.xz")
	if _, err := os.Stat(dataXz); err == nil {
		return readAll(dataXz, true)
	}
	if _, err := os.Stat(dataPlain); err == nil {
		return readAll(dataPlain, false)
	}
	return nil, fmt.Errorf("column %q data not found in %s", name, dir)
}

// ===== archive scanning =====

// scanPartitions walks the given roots' hot/warm/cold phase dirs and collects
// .part directories matching the stream pattern.
func scanPartitions(roots []string, streamPattern string) ([]partitionEntry, error) {
	var out []partitionEntry
	for _, root := range roots {
		for _, phase := range []string{"hot", "warm", "cold"} {
			phaseRoot := filepath.Join(root, phase)
			streams, err := findStreams(phaseRoot, streamPattern)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, err
			}
			for _, s := range streams {
				parts, err := os.ReadDir(s.path)
				if err != nil {
					continue
				}
				for _, p := range parts {
					if !p.IsDir() {
						continue
					}
					if !strings.HasSuffix(p.Name(), ".part") {
						continue
					}
					out = append(out, partitionEntry{stream: s.name, path: filepath.Join(s.path, p.Name()), phase: phase})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].stream != out[j].stream {
			return out[i].stream < out[j].stream
		}
		return out[i].path < out[j].path
	})
	return out, nil
}

type streamDir struct {
	name string
	path string
}

func findStreams(root, streamPattern string) ([]streamDir, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var streams []streamDir
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if streamPattern != "" {
			ok, _ := filepath.Match(streamPattern, name)
			if !ok && !strings.Contains(name, streamPattern) {
				continue
			}
		}
		streams = append(streams, streamDir{name: name, path: filepath.Join(root, name)})
	}
	sort.Slice(streams, func(i, j int) bool { return streams[i].name < streams[j].name })
	return streams, nil
}

// ===== partition metadata =====

type partitionEntry struct {
	stream string
	path   string
	phase  string // hot, warm, cold
}

type summaryMeta struct {
	CollectedAtMin int64
	CollectedAtMax int64
}

// readSummary parses the collected_at min/max from summary.yaml, if present.
// The receiver writes a fixed YAML shape; we parse the fields we need without a
// YAML dependency.
func readSummary(dir string) *summaryMeta {
	data, err := os.ReadFile(filepath.Join(dir, "summary.yaml"))
	if err != nil {
		return nil
	}
	lines := strings.Split(string(data), "\n")
	m := &summaryMeta{}
	inCollected := false
	section := ""
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "collected_at:") {
			inCollected = true
			continue
		}
		if inCollected && strings.HasPrefix(trimmed, "received_at:") {
			inCollected = false
			continue
		}
		if !inCollected {
			continue
		}
		switch {
		case strings.HasPrefix(trimmed, "summary:"):
			section = "summary"
		case strings.HasPrefix(trimmed, "min:"):
			section = "min"
		case strings.HasPrefix(trimmed, "max:"):
			section = "max"
		case strings.HasPrefix(trimmed, "unix_ms:"):
			v, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(trimmed, "unix_ms:")), 10, 64)
			if err != nil {
				continue
			}
			if section == "min" {
				m.CollectedAtMin = v
			} else if section == "max" {
				m.CollectedAtMax = v
			}
		}
	}
	if m.CollectedAtMin == 0 && m.CollectedAtMax == 0 {
		return nil
	}
	return m
}

// partitionTimeRange returns the partition's collected_at coverage in
// microseconds since epoch. ok=false means the partition has no usable bounds.
func partitionTimeRange(p partitionEntry, summary *summaryMeta) (minUS, maxUS int64, ok bool) {
	if summary != nil {
		return summary.CollectedAtMin, summary.CollectedAtMax, true
	}
	// No summary (live hot partition): read the timestamp column.
	ts, err := timestampColumn(p.path, "collected_at")
	if err != nil || len(ts) == 0 {
		return 0, 0, false
	}
	minV, maxV := ts[0], ts[0]
	for _, v := range ts {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	return minV, maxV, true
}

// ===== search =====

type matchResult struct {
	Stream      string
	Partition   string
	Phase       string
	Row         int
	RowID       uint64
	CollectedAt int64 // microseconds
	ReceivedAt  int64
	Source      string
	Line        string
}

func searchPartition(p partitionEntry, opts *options, summary *summaryMeta) ([]matchResult, error) {
	// Time-range prefilter via collected_at coverage.
	if opts.From != nil || opts.To != nil {
		minUS, maxUS, ok := partitionTimeRange(p, summary)
		if ok {
			if opts.From != nil && maxUS < *opts.From {
				return nil, nil
			}
			if opts.To != nil && minUS > *opts.To {
				return nil, nil
			}
		}
	}

	rawOffsets, rawData, err := stringColumn(p.path, "raw")
	if err != nil {
		return nil, err
	}
	if len(rawOffsets) == 0 {
		return nil, nil
	}
	rows := len(rawOffsets)

	var collectedAt, receivedAt []int64
	if opts.Show || opts.From != nil || opts.To != nil {
		collectedAt, _ = timestampColumn(p.path, "collected_at")
		receivedAt, _ = timestampColumn(p.path, "received_at")
	}
	var sources []string
	if opts.Show {
		sources, _ = tokenRLEColumn(p.path, "source")
	}

	partNo := decodePartNo(p.path)
	timeFiltered := opts.From != nil || opts.To != nil

	var results []matchResult
	for i := 0; i < rows; i++ {
		lo := uint32(0)
		if i > 0 {
			lo = rawOffsets[i-1]
		}
		hi := rawOffsets[i]
		if hi < lo {
			continue
		}
		// Guard against a concurrently-written hot partition whose pos file is
		// ahead of the data file.
		if hi > uint32(len(rawData)) {
			hi = uint32(len(rawData))
		}
		if hi <= lo {
			continue
		}
		line := rawData[lo:hi]

		if timeFiltered {
			var ca int64
			if collectedAt != nil && i < len(collectedAt) {
				ca = collectedAt[i]
			}
			if opts.From != nil && ca < *opts.From {
				continue
			}
			if opts.To != nil && ca > *opts.To {
				continue
			}
		}

		if !matchLine(line, opts) {
			continue
		}

		var ca, ra int64
		if collectedAt != nil && i < len(collectedAt) {
			ca = collectedAt[i]
		}
		if receivedAt != nil && i < len(receivedAt) {
			ra = receivedAt[i]
		}
		var src string
		if sources != nil && i < len(sources) {
			src = sources[i]
		}
		results = append(results, matchResult{
			Stream: p.stream, Partition: partNo, Phase: p.phase, Row: i,
			RowID: rowID(partNo, i), CollectedAt: ca, ReceivedAt: ra, Source: src,
			Line: string(line),
		})
	}
	return results, nil
}

func matchLine(line []byte, opts *options) bool {
	if opts.Pattern != nil {
		return opts.Pattern.Match(line)
	}
	if opts.Literal != "" {
		if opts.CaseInsensitive {
			return strings.Contains(strings.ToLower(string(line)), strings.ToLower(opts.Literal))
		}
		return strings.Contains(string(line), opts.Literal)
	}
	return true // no pattern: match everything (listing mode)
}

// decodePartNo extracts the base-16 partition number from the .part directory name.
func decodePartNo(path string) string {
	return strings.TrimSuffix(filepath.Base(path), ".part")
}

// rowID reconstructs the receiver's row_id = (partno << 24) | (row_no+1).
func rowID(partNo string, row int) uint64 {
	return (a16Decode(partNo) << 24) | uint64(row+1)
}

// a16Decode decodes the receiver's base-16 (a-p) partition number.
func a16Decode(s string) uint64 {
	const alpha = "abcdefghijklmnop"
	var v uint64
	for i := 0; i < len(s); i++ {
		idx := strings.IndexByte(alpha, s[i])
		if idx < 0 {
			return 0
		}
		v = v*16 + uint64(idx)
	}
	return v
}

// ===== options =====

type options struct {
	Root            []string
	Stream          string
	From            *int64 // microseconds
	To              *int64 // microseconds
	Literal         string
	Pattern         *regexp.Regexp
	CaseInsensitive bool
	Show            bool
	JSON            bool
	MaxCount        int
	Collected       bool
	Received        bool
}

func parseTime(s string) (*int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	// raw microseconds
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &v, nil
	}
	// RFC3339 / ISO8601 with optional fractional seconds
	layouts := []string{
		"2006-01-02T15:04:05.999999Z07:00",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			us := t.UnixMicro()
			return &us, nil
		}
	}
	// unix seconds
	if v, err := strconv.ParseInt(s, 10, 64); err == nil && v < 10_000_000_000 {
		us := v * 1_000_000
		return &us, nil
	}
	return nil, fmt.Errorf("cannot parse time %q (use RFC3339, YYYY-MM-DD, unix seconds, or microseconds)", s)
}

// ===== main =====

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func main() {
	var (
		roots       stringList
		stream      = flag.String("stream", "", "stream name pattern (substring or glob), e.g. received.plus.palo-alto")
		fromStr     = flag.String("from", "", "start of time range (RFC3339, YYYY-MM-DD, unix seconds, or microseconds)")
		toStr       = flag.String("to", "", "end of time range")
		pattern     = flag.String("pattern", "", "regular expression to search in raw log lines")
		literal     = flag.String("literal", "", "literal string to search in raw log lines")
		ignoreCase  = flag.Bool("i", false, "case-insensitive search")
		show        = flag.Bool("show", false, "show source, collected_at, received_at columns")
		jsonOut     = flag.Bool("json", false, "output JSON lines")
		maxCount    = flag.Int("max", 0, "stop after this many matches (0 = unlimited)")
		listStreams = flag.Bool("list-streams", false, "list streams present on disk and exit")
	)
	flag.Var(&roots, "root", "archive root containing hot/warm/cold phase dirs; repeatable")
	flag.Parse()

	opts := &options{
		Root:            roots,
		Stream:          *stream,
		Literal:         *literal,
		CaseInsensitive: *ignoreCase,
		Show:            *show,
		JSON:            *jsonOut,
		MaxCount:        *maxCount,
	}

	if *pattern != "" {
		re, err := regexp.Compile(*pattern)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid pattern: %v\n", err)
			os.Exit(2)
		}
		opts.Pattern = re
	}

	var err error
	if *fromStr != "" {
		opts.From, err = parseTime(*fromStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid -from: %v\n", err)
			os.Exit(2)
		}
	}
	if *toStr != "" {
		opts.To, err = parseTime(*toStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid -to: %v\n", err)
			os.Exit(2)
		}
	}

	if len(opts.Root) == 0 {
		opts.Root = autoDetectRoots()
		if len(opts.Root) == 0 {
			fmt.Fprintln(os.Stderr, "archive root not found; pass -root")
			os.Exit(2)
		}
	}

	if *listStreams {
		seen := map[string]bool{}
		for _, root := range opts.Root {
			for _, phase := range []string{"hot", "warm", "cold"} {
				streams, err := findStreams(filepath.Join(root, phase), "")
				if err != nil {
					continue
				}
				for _, s := range streams {
					key := phase + "\t" + s.name
					if seen[key] {
						continue
					}
					seen[key] = true
					fmt.Printf("%s\t%s\n", phase, s.name)
				}
			}
		}
		return
	}

	parts, err := scanPartitions(opts.Root, opts.Stream)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scan failed: %v\n", err)
		os.Exit(1)
	}
	if len(parts) == 0 {
		fmt.Fprintf(os.Stderr, "no partitions found (stream pattern %q)\n", opts.Stream)
		os.Exit(1)
	}

	total := 0
	for _, p := range parts {
		summary := readSummary(p.path)
		res, err := searchPartition(p, opts, summary)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s/%s: %v\n", p.phase, p.path, err)
			continue
		}
		for _, m := range res {
			total++
			emit(m, opts)
			if opts.MaxCount > 0 && total >= opts.MaxCount {
				fmt.Fprintf(os.Stderr, "reached match limit %d\n", opts.MaxCount)
				os.Exit(0)
			}
		}
	}
	fmt.Fprintf(os.Stderr, "%d partition(s) scanned, %d match(es)\n", len(parts), total)
}

func emit(m matchResult, opts *options) {
	if opts.JSON {
		ts := ""
		if m.CollectedAt > 0 {
			ts = time.UnixMicro(m.CollectedAt).UTC().Format("2006-01-02T15:04:05.999999Z")
		}
		recv := ""
		if m.ReceivedAt > 0 {
			recv = time.UnixMicro(m.ReceivedAt).UTC().Format("2006-01-02T15:04:05.999999Z")
		}
		fmt.Printf("{\"stream\":%q,\"phase\":%q,\"partition\":%q,\"row\":%d,\"row_id\":%d,\"collected_at\":%q,\"received_at\":%q,\"source\":%q,\"line\":%q}\n",
			m.Stream, m.Phase, m.Partition, m.Row, m.RowID, ts, recv, m.Source, m.Line)
		return
	}
	prefix := fmt.Sprintf("%s\t%s/%s\t", m.Stream, m.Phase, m.Partition)
	if opts.Show {
		ca := ""
		if m.CollectedAt > 0 {
			ca = time.UnixMicro(m.CollectedAt).UTC().Format("2006-01-02T15:04:05.000000Z")
		}
		prefix += fmt.Sprintf("%s\t%s\t", ca, m.Source)
	}
	fmt.Printf("%s%s\n", prefix, m.Line)
}

// autoDetectRoots tries the standard receiver data paths.
func autoDetectRoots() []string {
	var out []string
	candidates := []string{
		"/data",
		"/data/ssd/lmio-receiver-1",
		"/data/hdd/lmio-receiver-1",
	}
	for _, c := range candidates {
		for _, phase := range []string{"hot", "warm", "cold"} {
			if fi, err := os.Stat(filepath.Join(c, phase)); err == nil && fi.IsDir() {
				out = append(out, c)
				break
			}
		}
	}
	return out
}
