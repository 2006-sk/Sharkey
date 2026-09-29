package protocol

import (
	"encoding/binary"
	"fmt"
)

// scan.go: the payload format used for anti-entropy / resynchronisation.
//
// When a storage node comes back after a failure it has missed writes. The
// coordinator repairs it by paging through a healthy replica's data with
// OpScan and pushing the entries to the recovered node with OpApplyBatch.
// Both operations need to move MANY entries inside ONE protocol message, so
// this file defines a nested encoding that is carried inside
// Response.Value (for OpScan) or Request.Value (for OpApplyBatch).
//
// It reuses exactly the same ideas as the outer frame format: fixed-width
// big-endian integers and u32 length prefixes in front of every variable-length
// field. Length prefixes (rather than separators such as ',' or '\n') mean
// keys and values may contain any bytes at all, and the decoder always knows
// exactly how far to read without scanning for a terminator.

// ScanEntry is one stored entry returned by OpScan. Tombstones (deleted keys)
// are included so that deletes propagate during resynchronisation.
//
// Why tombstones matter: if a node missed a DELETE while it was down, and the
// scan only returned live keys, the recovered node would still hold the old
// value and nothing would tell it to remove it. By shipping a "this key is
// deleted as of version V" marker, the receiver can apply the delete with
// normal last-writer-wins rules: the higher version wins, whether it is a
// value or a tombstone.
//
// Fields:
//   - Key: the stored key (arbitrary bytes, held in a Go string).
//   - Value: the stored bytes; empty for a tombstone.
//   - Version: the write version assigned by the coordinator. Receivers
//     compare it with what they hold, so re-applying an entry is harmless
//     (idempotent) and an older entry never overwrites a newer one.
//   - Tombstone: true if the key was deleted at Version.
type ScanEntry struct {
	Key       string
	Value     []byte
	Version   uint64
	Tombstone bool
}

// ScanPage is one page of a scan.
//
// Encoding (carried in Response.Value):
//
//	| nextCursorLen u32 | nextCursor | count u32 | entries... |
//	entry = | tombstone u8 | version u64 | keyLen u32 | key | valLen u32 | value |
//
// An empty NextCursor means the scan is complete.
//
// Why pages and a cursor: a node may hold far more data than fits in one
// frame (frames are capped at DefaultMaxFrameSize, 4 MiB), and building one
// giant response would also stall the connection for everyone else sharing
// it. So the scan is split into bounded pages. The server returns an opaque
// cursor with each page; the client sends it back (in the Key field of the
// next OpScan request) to continue where the previous page stopped. The
// cursor is "opaque" because only the server interprets its bytes; the client
// just echoes them.
//
// Byte layout of a page with one live entry (key "k", value "v", version 7):
//
//	00 00 00 03  'a' 'b' 'c'          nextCursor = "abc" (length 3)
//	00 00 00 01                       count = 1
//	00                                tombstone = false
//	00 00 00 00 00 00 00 07           version = 7
//	00 00 00 01  'k'                  key
//	00 00 00 01  'v'                  value
//
// Fields:
//   - NextCursor: where the next page starts; empty when there is nothing left.
//   - Entries: the entries in this page (possibly zero on the last page).
type ScanPage struct {
	NextCursor []byte
	Entries    []ScanEntry
}

// ScanEntrySize is the encoded size of an entry, used to cap page sizes.
//
// The arithmetic mirrors the entry layout exactly:
//
//	1 (tombstone u8) + 8 (version u64) + 4 (keyLen u32) + len(key)
//	                                   + 4 (valLen u32) + len(value)
//
// The node building a scan page adds entries until the running total would
// exceed its budget, which keeps every page comfortably under the frame
// limit. If this formula drifted from EncodeScanPage, pages could exceed the
// frame limit and be rejected by the reader as ErrFrameTooLarge.
func ScanEntrySize(key string, value []byte) int {
	return 1 + 8 + 4 + len(key) + 4 + len(value)
}

// EncodeScanPage serialises a scan page.
//
// It appends into a nil slice and lets append grow it; this runs once per
// resync page, not per client request, so the occasional reallocation is not
// worth optimising. Every integer is written big-endian ("network byte
// order", most significant byte first) so both ends agree regardless of the
// CPU's native byte order.
func EncodeScanPage(p *ScanPage) []byte {
	var dst []byte
	// Header: the cursor, length-prefixed so it may contain any bytes.
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(p.NextCursor)))
	dst = append(dst, p.NextCursor...)
	// Entry count. The decoder uses it to know how many entries to expect, so
	// it can detect a truncated page instead of silently returning fewer entries.
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(p.Entries)))
	for _, e := range p.Entries {
		// The tombstone flag is one byte: 1 = deleted, 0 = live.
		var t byte
		if e.Tombstone {
			t = 1
		}
		dst = append(dst, t)
		// Fixed-width 8-byte version, then key and value each with a u32 length
		// prefix, in exactly the order ScanEntrySize counts them.
		dst = binary.BigEndian.AppendUint64(dst, e.Version)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(e.Key)))
		dst = append(dst, e.Key...)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(e.Value)))
		dst = append(dst, e.Value...)
	}
	return dst
}

// DecodeScanPage parses a scan page.
//
// Defensive decoding: the input comes from the network, so every length is
// checked against the bytes that actually remain BEFORE slicing. A malformed
// or truncated page returns ErrMalformed instead of panicking with an
// out-of-range slice. As with DecodeRequest, the returned keys are copied
// into strings, but the Values alias (point into) b: no copy is made, so the
// caller must not reuse b while still holding the entries.
func DecodeScanPage(b []byte) (*ScanPage, error) {
	// readBytes (protocol.go) reads a u32 length and then that many bytes, and
	// returns the rest of the buffer; we thread `b` through each call like a
	// cursor moving forward.
	cursor, b, err := readBytes(b, "cursor")
	if err != nil {
		return nil, err
	}
	// Need 4 bytes for the entry count.
	if len(b) < 4 {
		return nil, fmt.Errorf("%w: truncated scan entry count", ErrMalformed)
	}
	n := binary.BigEndian.Uint32(b)
	b = b[4:]
	// Entries are appended one by one. We deliberately do NOT pre-allocate
	// make([]ScanEntry, 0, n): n comes straight off the wire, and a hostile or
	// corrupt count such as 0xFFFFFFFF would make us allocate a huge slice before
	// discovering that the bytes are not there. Growing as entries are actually
	// decoded bounds memory by the real input size.
	p := &ScanPage{NextCursor: cursor}
	for i := uint32(0); i < n; i++ {
		// 9 = 1 (tombstone byte) + 8 (version) fixed-width bytes per entry.
		if len(b) < 9 {
			return nil, fmt.Errorf("%w: truncated scan entry %d", ErrMalformed, i)
		}
		// Only the exact value 1 means tombstone; anything else is treated as live.
		e := ScanEntry{Tombstone: b[0] == 1, Version: binary.BigEndian.Uint64(b[1:9])}
		var key []byte
		// b[9:] skips the fixed part just parsed; readBytes then consumes the
		// length-prefixed key and returns what follows it.
		if key, b, err = readBytes(b[9:], "scan key"); err != nil {
			return nil, err
		}
		if e.Value, b, err = readBytes(b, "scan value"); err != nil {
			return nil, err
		}
		// string(key) copies the bytes, so the key does not alias the input buffer.
		e.Key = string(key)
		p.Entries = append(p.Entries, e)
	}
	// Leftover bytes after `count` entries mean the page is corrupt or the two
	// sides disagree about the format; reject rather than silently ignore.
	if len(b) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes in scan page", ErrMalformed, len(b))
	}
	return p, nil
}
