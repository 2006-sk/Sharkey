package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestRequestRoundTrip(t *testing.T) {
	cases := []*Request{
		{Op: OpGet, Key: "k"},
		{ID: 7, Op: OpPut, Flags: FlagReplica, Version: 42, Key: "key\nwith\nnewlines", Value: []byte("v\x00\nbinary\r\n")},
		{Op: OpDelete, Version: 1 << 60, Key: "x"},
		{Op: OpPing},
		{Op: OpPut, Key: "empty-value", Value: []byte{}},
	}
	for _, want := range cases {
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

func TestDecodeMalformed(t *testing.T) {
	valid := AppendRequest(nil, &Request{Op: OpPut, Key: "abc", Value: []byte("def")})

	// Every strict prefix of a valid payload must be rejected, never panic.
	for i := 0; i < len(valid); i++ {
		if _, err := DecodeRequest(valid[:i]); !errors.Is(err, ErrMalformed) {
			t.Fatalf("prefix len %d: want ErrMalformed, got %v", i, err)
		}
	}
	// Trailing garbage.
	if _, err := DecodeRequest(append(append([]byte{}, valid...), 0xFF)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("trailing bytes: want ErrMalformed, got %v", err)
	}
	// Key length pointing past the end of the payload.
	bad := append([]byte{}, valid...)
	binary.BigEndian.PutUint32(bad[14:18], 0xFFFFFFFF)
	if _, err := DecodeRequest(bad); !errors.Is(err, ErrMalformed) {
		t.Fatalf("huge key length: want ErrMalformed, got %v", err)
	}
	if RequestID(valid) != 0 || RequestID([]byte{0, 0, 1, 2, 9}) != 258 || RequestID([]byte{1}) != 0 {
		t.Fatal("RequestID extraction")
	}
	if _, err := DecodeResponse([]byte{0, 1}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("short response: want ErrMalformed, got %v", err)
	}
}

func TestValidate(t *testing.T) {
	l := Limits{MaxKeySize: 4, MaxValueSize: 4, MaxFrameSize: 64}
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

func TestFrames(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&buf, []byte("two\nlines")); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"one", "two\nlines"} {
		got, err := ReadFrame(&buf, 1024)
		if err != nil || string(got) != want {
			t.Fatalf("got %q, %v want %q", got, err, want)
		}
	}
	if _, err := ReadFrame(&buf, 1024); err != io.EOF {
		t.Fatalf("want io.EOF at end, got %v", err)
	}

	// Oversized frame is rejected from the header alone.
	buf.Reset()
	_ = WriteFrame(&buf, make([]byte, 100))
	if _, err := ReadFrame(&buf, 10); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}

	// Truncated frame body.
	buf.Reset()
	buf.Write([]byte{0, 0, 0, 10, 'a', 'b'})
	if _, err := ReadFrame(&buf, 1024); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
}

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
