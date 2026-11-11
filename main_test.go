package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ulikunitz/xz"
)

// ===== test helpers =====

// posFile encodes cumulative end-offsets as 4-byte LE, the layout of a .pos file.
func posFile(offsets []uint32) []byte {
	b := make([]byte, 4*len(offsets))
	for i, o := range offsets {
		b[4*i] = byte(o)
		b[4*i+1] = byte(o >> 8)
		b[4*i+2] = byte(o >> 16)
		b[4*i+3] = byte(o >> 24)
	}
	return b
}

// tsFile encodes microseconds-since-epoch values as 8-byte LE, the layout of a
// timestamp .data file.
func tsFile(vals ...int64) []byte {
	b := make([]byte, 8*len(vals))
	for i, v := range vals {
		u := uint64(v)
		for j := 0; j < 8; j++ {
			b[8*i+j] = byte(u >> (8 * j))
		}
	}
	return b
}

// rlePairs encodes (tokenID, count) pairs as 4-byte LE (u16, u16).
func rlePairs(pairs ...[2]int) []byte {
	var b []byte
	for _, p := range pairs {
		b = append(b, byte(p[0]), byte(p[0]>>8), byte(p[1]), byte(p[1]>>8))
	}
	return b
}

// writeTokenSubcolumn writes the string sub-column backing the token IDs in a
// partition dir. The receiver stores token strings with a leading sentinel end
// offset of 0, so token ID n resolves to record n (offsets[n-1]..offsets[n]).
func writeTokenSubcolumn(t *testing.T, dir string, records ...string) {
	t.Helper()
	var data []byte
	offs := []uint32{0} // sentinel; ID 0 is the empty string, ID n -> record n
	for _, r := range records {
		data = append(data, r...)
		offs = append(offs, uint32(len(data)))
	}
	if err := os.WriteFile(filepath.Join(dir, "col-source-token.pos"), posFile(offs), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "col-source-token.data"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// xzCompress compresses src the way cold partitions store their column files.
func xzCompress(t *testing.T, src []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := xz.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(src); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// ===== token:rle decoder =====

// A correctly-written RLE file holds one (tokenID, runLength) pair per run.
func TestTokenRLECleanFormat(t *testing.T) {
	dir := t.TempDir()
	writeTokenSubcolumn(t, dir, "tokA", "tokB")
	// Runs: tokA x3, tokB x2, tokA x1, "" (ID 0) x2.
	if err := os.WriteFile(filepath.Join(dir, "col-source.data"), rlePairs(
		[2]int{1, 3},
		[2]int{2, 2},
		[2]int{1, 1},
		[2]int{0, 2},
	), 0o644); err != nil {
		t.Fatal(err)
	}
	tokens, err := tokenRLEColumn(dir, "source")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tokA", "tokA", "tokA", "tokB", "tokB", "tokA", "", ""}
	if !reflect.DeepEqual(tokens, want) {
		t.Fatalf("got %v, want %v", tokens, want)
	}
}

// Files written by the buggy receiver flush() hold, for every true run of
// token T with length L, consecutive T-pairs with non-decreasing counts ending
// in (and repeating) L. Recovery groups them into one run whose length is the
// maximum count in the group. Verify the recovered run stream matches the true
// runs tokA x3, tokB x2, tokA x1.
func TestTokenRLECorruptedFlushRecovery(t *testing.T) {
	dir := t.TempDir()
	writeTokenSubcolumn(t, dir, "tokA", "tokB")
	if err := os.WriteFile(filepath.Join(dir, "col-source.data"), rlePairs(
		[2]int{1, 1}, [2]int{1, 2}, [2]int{1, 3}, [2]int{1, 3}, // true run tokA x3
		[2]int{2, 1}, [2]int{2, 2}, [2]int{2, 2}, // true run tokB x2
		[2]int{1, 1}, [2]int{1, 1}, // true run tokA x1
	), 0o644); err != nil {
		t.Fatal(err)
	}
	tokens, err := tokenRLEColumn(dir, "source")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tokA", "tokA", "tokA", "tokB", "tokB", "tokA"}
	if !reflect.DeepEqual(tokens, want) {
		t.Fatalf("got %v, want %v", tokens, want)
	}
}

// A truncated corrupt snapshot (no final genuine pair) still recovers the run
// via max-of-segment.
func TestTokenRLECorruptedTruncatedSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeTokenSubcolumn(t, dir, "tokA")
	if err := os.WriteFile(filepath.Join(dir, "col-source.data"), rlePairs(
		[2]int{1, 1}, [2]int{1, 2}, [2]int{1, 3}, // snapshots, genuine pair missing
	), 0o644); err != nil {
		t.Fatal(err)
	}
	tokens, err := tokenRLEColumn(dir, "source")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tokA", "tokA", "tokA"}
	if !reflect.DeepEqual(tokens, want) {
		t.Fatalf("got %v, want %v", tokens, want)
	}
}

// A count decrease between same-token pairs starts a new run.
func TestTokenRLECountDecreaseStartsNewRun(t *testing.T) {
	dir := t.TempDir()
	writeTokenSubcolumn(t, dir, "tokA")
	// (1,2),(1,3) -> one run of length 3; then count 1 < 3 starts a new run.
	if err := os.WriteFile(filepath.Join(dir, "col-source.data"), rlePairs(
		[2]int{1, 2}, [2]int{1, 3}, [2]int{1, 1},
	), 0o644); err != nil {
		t.Fatal(err)
	}
	tokens, err := tokenRLEColumn(dir, "source")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tokA", "tokA", "tokA", "tokA"}
	if !reflect.DeepEqual(tokens, want) {
		t.Fatalf("got %v, want %v", tokens, want)
	}
}

// ===== string-column offsets =====

func TestStringColumnOffsets(t *testing.T) {
	dir := t.TempDir()
	// Records: "hello" [0:5], "world" [5:10], "" [10:10] (empty), "!" [10:11].
	offs := []uint32{5, 10, 10, 11}
	if err := os.WriteFile(filepath.Join(dir, "col-raw.pos"), posFile(offs), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "col-raw.data"), []byte("helloworld!"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, data, err := stringColumn(dir, "raw")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, offs) {
		t.Fatalf("offsets: got %v, want %v", got, offs)
	}
	if string(data) != "helloworld!" {
		t.Fatalf("data: got %q, want %q", data, "helloworld!")
	}
	// Manual record extraction mirroring searchPartition: row i spans [prev, offs[i]].
	rows := []string{string(data[0:offs[0]]), string(data[offs[0]:offs[1]]), string(data[offs[1]:offs[2]]), string(data[offs[2]:offs[3]])}
	wantRows := []string{"hello", "world", "", "!"}
	if !reflect.DeepEqual(rows, wantRows) {
		t.Fatalf("rows: got %v, want %v", rows, wantRows)
	}
}

// Cold partitions store each column file xz-compressed; the reader must
// auto-detect and decompress.
func TestStringColumnXZ(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "col-raw.pos.xz"), xzCompress(t, posFile([]uint32{5, 11})), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "col-raw.data.xz"), xzCompress(t, []byte("helloworld!")), 0o644); err != nil {
		t.Fatal(err)
	}
	got, data, err := stringColumn(dir, "raw")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []uint32{5, 11}) {
		t.Fatalf("offsets: got %v", got)
	}
	if string(data) != "helloworld!" {
		t.Fatalf("data: got %q", data)
	}
}

// ===== timestamp decode =====

func TestTimestampColumn(t *testing.T) {
	dir := t.TempDir()
	vals := []int64{0x0102030405060708, 1234567890123456, -1}
	if err := os.WriteFile(filepath.Join(dir, "col-collected_at.data"), tsFile(vals...), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := timestampColumn(dir, "collected_at")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, vals) {
		t.Fatalf("got %v, want %v", got, vals)
	}
}

func TestTimestampColumnNotMultipleOf8(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "col-collected_at.data"), []byte{1, 2, 3, 4, 5, 6, 7}, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := timestampColumn(dir, "collected_at"); err == nil {
		t.Fatal("expected error for non-multiple-of-8 data")
	}
}

// ===== time-range prefilter =====

func TestPartitionTimeRangeFromSummary(t *testing.T) {
	p := partitionEntry{stream: "s", path: "/nonexistent", phase: "cold"}
	s := &summaryMeta{CollectedAtMin: 100, CollectedAtMax: 200}
	minUS, maxUS, ok := partitionTimeRange(p, s)
	if !ok || minUS != 100 || maxUS != 200 {
		t.Fatalf("got (%d,%d,%v), want (100,200,true)", minUS, maxUS, ok)
	}
}

// Without a summary (live hot partition), bounds come from the collected_at
// column.
func TestPartitionTimeRangeFromColumn(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "col-collected_at.data"), tsFile(5, 3, 9, 1), 0o644); err != nil {
		t.Fatal(err)
	}
	p := partitionEntry{path: dir}
	minUS, maxUS, ok := partitionTimeRange(p, nil)
	if !ok || minUS != 1 || maxUS != 9 {
		t.Fatalf("got (%d,%d,%v), want (1,9,true)", minUS, maxUS, ok)
	}
}

func TestPartitionTimeRangeNoData(t *testing.T) {
	dir := t.TempDir()
	p := partitionEntry{path: dir}
	if _, _, ok := partitionTimeRange(p, nil); ok {
		t.Fatal("expected ok=false for a partition with no bounds")
	}
}

func TestReadSummary(t *testing.T) {
	dir := t.TempDir()
	summary := `part_id: 000001a
collected_at:
  min:
    unix_ms: 1111111111111
  max:
    unix_ms: 2222222222222
received_at:
  min:
    unix_ms: 3333333333333
  max:
    unix_ms: 4444444444444
count: 10
`
	if err := os.WriteFile(filepath.Join(dir, "summary.yaml"), []byte(summary), 0o644); err != nil {
		t.Fatal(err)
	}
	m := readSummary(dir)
	if m == nil {
		t.Fatal("expected a parsed summary")
	}
	if m.CollectedAtMin != 1111111111111 || m.CollectedAtMax != 2222222222222 {
		t.Fatalf("got min=%d max=%d", m.CollectedAtMin, m.CollectedAtMax)
	}
}

// A partition whose collected_at coverage ends before -from is skipped without
// touching the raw column (only the collected_at column needs to exist).
func TestSearchPrefilterSkipsBeforeFrom(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "col-collected_at.data"), tsFile(100, 200), 0o644); err != nil {
		t.Fatal(err)
	}
	p := partitionEntry{stream: "s", path: dir, phase: "hot"}
	from := int64(300)
	opts := &options{From: &from}
	res, err := searchPartition(p, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 0 {
		t.Fatalf("expected prefiltered-out partition to yield no matches, got %d", len(res))
	}
}

// A partition whose collected_at coverage starts after -to is skipped.
func TestSearchPrefilterSkipsAfterTo(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "col-collected_at.data"), tsFile(100, 200), 0o644); err != nil {
		t.Fatal(err)
	}
	p := partitionEntry{stream: "s", path: dir, phase: "hot"}
	to := int64(50)
	opts := &options{To: &to}
	res, err := searchPartition(p, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 0 {
		t.Fatalf("expected prefiltered-out partition to yield no matches, got %d", len(res))
	}
}

// The same prefilter applies when bounds come from summary.yaml.
func TestSearchPrefilterSkipsFromSummary(t *testing.T) {
	dir := t.TempDir()
	p := partitionEntry{stream: "s", path: dir, phase: "cold"}
	s := &summaryMeta{CollectedAtMin: 100, CollectedAtMax: 200}
	from := int64(500)
	opts := &options{From: &from}
	res, err := searchPartition(p, opts, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 0 {
		t.Fatalf("expected prefiltered-out partition to yield no matches, got %d", len(res))
	}
}

// ===== end-to-end search =====

// Full partition search: raw string column + timestamps + token:rle source +
// literal match, exercising stringColumn, timestampColumn, tokenRLEColumn and
// matchLine together.
func TestSearchPartitionEndToEnd(t *testing.T) {
	dir := t.TempDir()
	// Raw rows: "panic in module A" (17), "all good" (9), "panic in module B" (17).
	if err := os.WriteFile(filepath.Join(dir, "col-raw.pos"), posFile([]uint32{17, 26, 43}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "col-raw.data"), []byte("panic in module Aall good panic in module B"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "col-collected_at.data"), tsFile(100, 200, 300), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "col-received_at.data"), tsFile(100, 200, 300), 0o644); err != nil {
		t.Fatal(err)
	}
	writeTokenSubcolumn(t, dir, "tokA", "tokB")
	// Runs: tokA x2, tokB x1.
	if err := os.WriteFile(filepath.Join(dir, "col-source.data"), rlePairs(
		[2]int{1, 2}, [2]int{2, 1},
	), 0o644); err != nil {
		t.Fatal(err)
	}

	p := partitionEntry{stream: "received.test", path: dir, phase: "hot"}
	opts := &options{Literal: "panic", Show: true}
	res, err := searchPartition(p, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("want 2 matches, got %d", len(res))
	}
	if res[0].Line != "panic in module A" || res[1].Line != "panic in module B" {
		t.Fatalf("unexpected lines: %q, %q", res[0].Line, res[1].Line)
	}
	if res[0].Source != "tokA" || res[1].Source != "tokB" {
		t.Fatalf("unexpected sources: %q, %q", res[0].Source, res[1].Source)
	}
	if res[0].CollectedAt != 100 || res[1].CollectedAt != 300 {
		t.Fatalf("unexpected collected_at: %d, %d", res[0].CollectedAt, res[1].CollectedAt)
	}
	if res[0].RowID != rowID("1", 0) {
		t.Fatalf("unexpected row_id: %d", res[0].RowID)
	}
}

// ===== time parsing =====

func TestParseTime(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"2026-09-15T12:00:00Z", 1789473600000000}, // RFC3339, whole second
		{"2026-09-15", 1789430400000000},           // YYYY-MM-DD midnight UTC
		{"1500000000", 1500000000},                 // bare integer = raw microseconds
		{"1789473600000000", 1789473600000000},     // raw microseconds
	}
	for _, c := range cases {
		got, err := parseTime(c.in)
		if err != nil {
			t.Fatalf("parseTime(%q): %v", c.in, err)
		}
		if *got != c.want {
			t.Fatalf("parseTime(%q) = %d, want %d", c.in, *got, c.want)
		}
	}
	if _, err := parseTime("not-a-time"); err == nil {
		t.Fatal("expected error for unparseable time")
	}
}
