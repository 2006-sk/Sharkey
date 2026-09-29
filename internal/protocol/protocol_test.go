package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// Tests for the wire format. They run without any network: encoding and
// decoding are pure functions over byte slices, and framing works over any
// io.Reader/io.Writer (a bytes.Buffer stands in for the socket).

// TestRequestRoundTrip proves encode -> decode is lossless for requests:
// every field survives exactly. That is the basic contract of any codec; if
// it broke, nodes would store different keys or versions than clients sent.
//
// The cases are chosen to hit edges: all-zero defaults, every field set,
// a version using the high bits of the u64 (catches accidental truncation to
// 32 bits), no key at all, and an empty-but-present value. The second case
// puts '\n', '\r' and NUL bytes in the key and value: with length-prefix
// framing those are just bytes, whereas a newline-delimited protocol would
// split the message there. This is the test behind the README's "keys and
// values may contain any bytes" claim.
func TestRequestRoundTrip(t *testing.T) {
	cases := []*Request{
		{Op: OpGet, Key: "k"},
		{ID: 7, Op: OpPut, Flags: FlagReplica, Version: 42, Key: "key\nwith\nnewlines", Value: []byte("v\x00\nbinary\r\n")},
		{Op: OpDelete, Version: 1 << 60, Key: "x"},
		{Op: OpPing},
		{Op: OpPut, Key: "empty-value", Value: []byte{}},
	}
	for _, want := range cases {
		// AppendRequest(nil, ...) produces a payload (no frame header), which is
		// exactly what DecodeRequest expects.
		got, err := DecodeRequest(AppendRequest(nil, want))
		if err != nil {
			t.Fatalf("decode %v: %v", want.Op, err)
		}
		if got.ID != want.ID || got.Op != want.Op || got.Flags != want.Flags || got.Version != want.Version ||
			got.Key != want.Key || !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("round trip mismatch: got %+v want %+v", got, want)
		}
	}
}

// TestResponseRoundTrip is the same lossless round-trip property for
// responses, with a non-OK status and a value containing a newline.
func TestResponseRoundTrip(t *testing.T) {
	want := &Response{ID: 99, Status: StatusNotFound, Version: 7, Value: []byte("hello\n")}
	got, err := DecodeResponse(AppendResponse(nil, want))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != want.ID || got.Status != want.Status || got.Version != want.Version || !bytes.Equal(got.Value, want.Value) {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

// TestDecodeMalformed proves the decoder is robust against hostile or
// corrupted input: every broken payload yields ErrMalformed and nothing
// panics or reads out of bounds. The server feeds DecodeRequest raw bytes
// from the network, so a panic here would be a remote crash bug.
func TestDecodeMalformed(t *testing.T) {
	valid := AppendRequest(nil, &Request{Op: OpPut, Key: "abc", Value: []byte("def")})

	// Every strict prefix of a valid payload must be rejected, never panic.
	// This simulates truncation at every possible byte position: inside the
	// fixed header, inside a length field, inside the key, inside the value.
	for i := 0; i < len(valid); i++ {
		if _, err := DecodeRequest(valid[:i]); !errors.Is(err, ErrMalformed) {
			t.Fatalf("prefix len %d: want ErrMalformed, got %v", i, err)
		}
	}
	// (Payload lengths come from the frame header; if the inner lengths do not
	// add up to exactly the payload size, something is wrong.)
	// Trailing garbage.
	if _, err := DecodeRequest(append(append([]byte{}, valid...), 0xFF)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("trailing bytes: want ErrMalformed, got %v", err)
	}
	// Key length pointing past the end of the payload.
	// Bytes 14..17 hold keyLen (see the offset table in protocol.go). Setting
	// it to 0xFFFFFFFF claims a 4 GiB key inside a tiny payload; the bounds
	// check in readBytes must catch this instead of slicing past the end.
	bad := append([]byte{}, valid...)
	binary.BigEndian.PutUint32(bad[14:18], 0xFFFFFFFF)
	if _, err := DecodeRequest(bad); !errors.Is(err, ErrMalformed) {
		t.Fatalf("huge key length: want ErrMalformed, got %v", err)
	}
	// RequestID must pull the ID out of the first 4 big-endian bytes even when
	// the rest is garbage (00 00 01 02 = 0x0102 = 258), and fall back to 0 when
	// there are not even 4 bytes. `valid` was built with ID 0, so it yields 0.
	if RequestID(valid) != 0 || RequestID([]byte{0, 0, 1, 2, 9}) != 258 || RequestID([]byte{1}) != 0 {
		t.Fatal("RequestID extraction")
	}
	// Responses too: 2 bytes cannot hold the 17-byte response header.
	if _, err := DecodeResponse([]byte{0, 1}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("short response: want ErrMalformed, got %v", err)
	}
}

// TestValidate checks the semantic rules applied after decoding: size limits
// are enforced (tiny limits of 4 bytes make that easy to hit), data
// operations require a key, unknown ops are rejected, and PING legitimately
// has no key. Validate is the server's gatekeeper before a handler runs.
func TestValidate(t *testing.T) {
	l := Limits{MaxKeySize: 4, MaxValueSize: 4, MaxFrameSize: 64}
	// Table-driven test: each row is a named case plus the expected outcome,
	// so adding a case is one line and failures name the case.
	tests := []struct {
		name string
		req  Request
		ok   bool
	}{
		{"ok", Request{Op: OpPut, Key: "k", Value: []byte("v")}, true},
		{"key too big", Request{Op: OpGet, Key: "kkkkk"}, false},
		{"value too big", Request{Op: OpPut, Key: "k", Value: []byte("vvvvv")}, false},
		{"empty key", Request{Op: OpGet}, false},
		{"unknown op", Request{Op: 99, Key: "k"}, false},
		{"ping no key", Request{Op: OpPing}, true},
	}
	for _, tt := range tests {
		err := tt.req.Validate(l)
		if (err == nil) != tt.ok {
			t.Errorf("%s: err=%v, want ok=%v", tt.name, err, tt.ok)
		}
	}
}

// TestFrames exercises the framing layer on its own: several frames written
// back to back into one stream come out as the same separate messages (the
// whole reason length prefixes exist, since a stream has no boundaries), and
// the error cases behave exactly as the transport relies on.
func TestFrames(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&buf, []byte("two\nlines")); err != nil {
		t.Fatal(err)
	}
	// Two frames are now concatenated in one buffer; ReadFrame must split them
	// at exactly the right place.
	for _, want := range []string{"one", "two\nlines"} {
		got, err := ReadFrame(&buf, 1024)
		if err != nil || string(got) != want {
			t.Fatalf("got %q, %v want %q", got, err, want)
		}
	}
	// Clean end of stream between frames must be plain io.EOF (compared with
	// == on purpose, not errors.Is): the server treats it as "client hung up",
	// not as an error worth logging.
	if _, err := ReadFrame(&buf, 1024); err != io.EOF {
		t.Fatalf("want io.EOF at end, got %v", err)
	}

	// Oversized frame is rejected from the header alone.
	// Limit 10 < 100: ReadFrame must fail after reading only the 4-byte header,
	// without allocating or reading the 100-byte body. That is the defence
	// against a peer claiming a gigantic frame.
	buf.Reset()
	_ = WriteFrame(&buf, make([]byte, 100))
	if _, err := ReadFrame(&buf, 10); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}

	// Truncated frame body.
	// The header promises 10 bytes but only "ab" follows, then EOF. That must be
	// io.ErrUnexpectedEOF (the peer vanished mid-message), distinguishable from
	// the clean io.EOF above.
	buf.Reset()
	buf.Write([]byte{0, 0, 0, 10, 'a', 'b'})
	if _, err := ReadFrame(&buf, 1024); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
}

// TestScanPageRoundTrip proves the nested scan-page encoding used by resync
// round-trips, including the cursor and a tombstone entry (a deleted key
// with no value). If tombstones were lost here, deletes would not propagate
// to a recovering node. The last check shows a truncated page is an error,
// not a silently empty page.
func TestScanPageRoundTrip(t *testing.T) {
	want := &ScanPage{
		NextCursor: []byte("cursor"),
		Entries: []ScanEntry{
			{Key: "a", Value: []byte("1"), Version: 1},
			{Key: "b", Version: 2, Tombstone: true},
		},
	}
	got, err := DecodeScanPage(EncodeScanPage(want))
	if err != nil {
		t.Fatal(err)
	}
	if string(got.NextCursor) != "cursor" || len(got.Entries) != 2 ||
		got.Entries[0].Key != "a" || string(got.Entries[0].Value) != "1" ||
		!got.Entries[1].Tombstone || got.Entries[1].Version != 2 {
		t.Fatalf("mismatch: %+v", got)
	}
	if _, err := DecodeScanPage([]byte{0, 0}); err == nil {
		t.Fatal("expected error for truncated page")
	}
}
