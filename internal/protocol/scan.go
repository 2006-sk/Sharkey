package protocol

import (
	"encoding/binary"
	"fmt"
)

// ScanEntry is one stored entry returned by OpScan. Tombstones (deleted keys)
// are included so that deletes propagate during resynchronisation.
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
type ScanPage struct {
	NextCursor []byte
	Entries    []ScanEntry
}

// ScanEntrySize is the encoded size of an entry, used to cap page sizes.
func ScanEntrySize(key string, value []byte) int {
	return 1 + 8 + 4 + len(key) + 4 + len(value)
}

// EncodeScanPage serialises a scan page.
func EncodeScanPage(p *ScanPage) []byte {
	var dst []byte
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(p.NextCursor)))
	dst = append(dst, p.NextCursor...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(p.Entries)))
	for _, e := range p.Entries {
		var t byte
		if e.Tombstone {
			t = 1
		}
		dst = append(dst, t)
		dst = binary.BigEndian.AppendUint64(dst, e.Version)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(e.Key)))
		dst = append(dst, e.Key...)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(e.Value)))
		dst = append(dst, e.Value...)
	}
	return dst
}

// DecodeScanPage parses a scan page.
func DecodeScanPage(b []byte) (*ScanPage, error) {
	cursor, b, err := readBytes(b, "cursor")
	if err != nil {
		return nil, err
	}
	if len(b) < 4 {
		return nil, fmt.Errorf("%w: truncated scan entry count", ErrMalformed)
	}
	n := binary.BigEndian.Uint32(b)
	b = b[4:]
	p := &ScanPage{NextCursor: cursor}
	for i := uint32(0); i < n; i++ {
		if len(b) < 9 {
			return nil, fmt.Errorf("%w: truncated scan entry %d", ErrMalformed, i)
		}
		e := ScanEntry{Tombstone: b[0] == 1, Version: binary.BigEndian.Uint64(b[1:9])}
		var key []byte
		if key, b, err = readBytes(b[9:], "scan key"); err != nil {
			return nil, err
		}
		if e.Value, b, err = readBytes(b, "scan value"); err != nil {
			return nil, err
		}
		e.Key = string(key)
		p.Entries = append(p.Entries, e)
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes in scan page", ErrMalformed, len(b))
	}
	return p, nil
}
