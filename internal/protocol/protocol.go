// Package protocol defines the binary wire format shared by clients,
// the coordinator and storage nodes.
//
// # The core problem: TCP is a byte stream, not a message stream
//
// TCP guarantees that bytes arrive in order and without gaps, but it has NO
// notion of message boundaries. If a sender does two writes of 10 bytes, the
// receiver may get them as one read of 20 bytes, or as reads of 3, 15 and 2
// bytes. The kernel is free to split and merge the stream however it likes
// (segmentation, Nagle, receive-buffer timing). So any protocol on top of TCP
// must answer: "where does one message end and the next begin?"
//
// Common answers:
//
//   - Delimiters (e.g. a newline, as in HTTP/1 headers or Redis inline
//     commands): simple, but the delimiter cannot appear inside the data
//     unless you add escaping, and the reader must scan every byte.
//   - Length prefix (what we use): each message starts with a fixed-size
//     integer saying how many bytes follow. The reader reads exactly 4 bytes,
//     learns n, then reads exactly n bytes. No scanning, no escaping, and the
//     data may contain ANY byte value.
//
// Every message on the wire is a frame:
//
//	+----------------+---------------------------+
//	| length (u32 BE)| payload (length bytes)    |
//	+----------------+---------------------------+
//
// The length prefix makes message boundaries explicit, so keys and values
// may contain arbitrary bytes (including newlines and NULs).
//
// For example, the 3-byte payload "abc" is sent as 7 bytes:
//
//	00 00 00 03 61 62 63
//	\---------/ \------/
//	 length=3   payload
//
// Request payload:
//
//	| id u32 | op u8 | flags u8 | version u64 | keyLen u32 | key | valLen u32 | value |
//
// Response payload:
//
//	| id u32 | status u8 | version u64 | valLen u32 | value |
//
// Note the nesting: the frame length covers the whole payload, and inside the
// payload the key and value have their OWN u32 length prefixes. The outer
// length tells the reader how much to pull off the socket; the inner lengths
// tell the decoder how to split the payload into fields.
//
// Byte offsets inside a request payload (used by DecodeRequest):
//
//	offset  0..3    id        (4 bytes)
//	offset  4       op        (1 byte)
//	offset  5       flags     (1 byte)
//	offset  6..13   version   (8 bytes)
//	offset 14..17   keyLen    (4 bytes), then keyLen bytes of key
//	then            valLen    (4 bytes), then valLen bytes of value
//
// All integers are big-endian. For error statuses the value carries a
// human-readable error message.
//
// Big-endian ("network byte order") means the most significant byte is sent
// first: the u32 value 258 (0x00000102) goes on the wire as 00 00 01 02. The
// choice itself is arbitrary, but both ends MUST agree, and big-endian is the
// long-standing convention for network protocols (IP, TCP and most RFCs use
// it). Using encoding/binary.BigEndian makes the encoding independent of the
// CPU's native byte order (x86 and ARM are both little-endian in practice).
//
// The request ID is echoed in the response. It lets one connection carry many
// concurrent requests (multiplexing): the server may answer them in any order
// and the client matches responses to callers by ID.
//
// Without IDs a connection could only be used strictly one-request-then-wait
// (or pipelined with responses forced into request order). Then one slow
// request delays every request queued behind it on that connection: this is
// head-of-line blocking. With IDs, request 7 can finish before request 3, and
// the client routes each response to whoever is waiting on that ID (see
// transport.MuxConn). ID 0 is reserved: the server uses it for
// connection-level errors that do not answer any particular request.
//
// Division of labour: this package only turns structs into bytes and back
// and enforces size limits. It knows nothing about sockets, goroutines or
// timeouts; package transport owns those. Keeping encoding pure (plain byte
// slices in, byte slices out) makes it trivial to unit-test and fuzz.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Op identifies the operation requested.
//
// It is one byte on the wire (u8), which leaves room for 255 operations. A
// named type rather than a bare uint8 means the compiler stops us from
// passing, say, a Status where an Op is expected.
type Op uint8

const (
	// Op codes start at 1, not 0, so an all-zero (uninitialised) request is not
	// silently a valid GET. The numeric values are part of the wire format: once
	// deployed they must never be renumbered, or old and new binaries would
	// disagree about what a byte means.
	OpGet    Op = 1
	OpPut    Op = 2
	OpDelete Op = 3
	OpPing   Op = 4 // health check; response value is a JSON health document
	OpStats  Op = 5 // response value is a JSON stats document
	OpScan   Op = 6 // node-only: page through stored entries (anti-entropy)
	OpLocate Op = 7 // coordinator-only: return the replica set for a key
	// (Summary: GET/PUT/DELETE are the client data operations; PING and STATS
	// are operational; SCAN and APPLY_BATCH exist only between coordinator and
	// nodes; LOCATE is answered only by the coordinator.)
	//
	// OpApplyBatch is node-only: apply many versioned entries (encoded as a
	// ScanPage in Value) in one round trip. Used by resynchronisation.
	OpApplyBatch Op = 8
)

// String makes Ops print readably in logs and error messages ("GET" instead
// of "1"); fmt uses it automatically because Op implements fmt.Stringer.
// Unknown values still print something useful rather than an empty string.
func (o Op) String() string {
	switch o {
	case OpGet:
		return "GET"
	case OpPut:
		return "PUT"
	case OpDelete:
		return "DELETE"
	case OpPing:
		return "PING"
	case OpStats:
		return "STATS"
	case OpScan:
		return "SCAN"
	case OpLocate:
		return "LOCATE"
	case OpApplyBatch:
		return "APPLY_BATCH"
	default:
		return fmt.Sprintf("Op(%d)", uint8(o))
	}
}

// Request flags.
//
// Flags is a bit set packed into one byte: each flag is a distinct power of
// two (1<<0 = 0b01, 1<<1 = 0b10), so several can be combined with | and
// tested independently with &, e.g. `req.Flags&FlagReplica != 0`. Up to 8
// flags fit in the byte. Flags do not change what a write does to the data;
// they tell the node WHY the write arrived, which it uses for statistics.
const (
	// FlagReplica marks a write forwarded by the coordinator to a non-primary
	// replica. Nodes count these as replication operations.
	FlagReplica uint8 = 1 << 0
	// FlagSync marks a write pushed during anti-entropy resynchronisation.
	FlagSync uint8 = 1 << 1
)

// Status is the outcome of a request.
//
// Like Op it is a single byte on the wire. Note that NotFound is an ordinary
// outcome, not a failure (see Response.Err): "the key does not exist" is a
// valid answer to a GET.
type Status uint8

const (
	// Numeric values are wire format: never renumber them.
	StatusOK          Status = 0
	StatusNotFound    Status = 1
	StatusError       Status = 2 // internal server error
	StatusBadRequest  Status = 3 // malformed or oversized request
	StatusUnavailable Status = 4 // not enough healthy replicas / quorum not met
)

// String gives Status a readable form for logs and for Response.Err.
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusNotFound:
		return "NOT_FOUND"
	case StatusError:
		return "ERROR"
	case StatusBadRequest:
		return "BAD_REQUEST"
	case StatusUnavailable:
		return "UNAVAILABLE"
	default:
		return fmt.Sprintf("Status(%d)", uint8(s))
	}
}

// Default size limits. They are enforced both when encoding and decoding.
//
// Why have limits at all? Every length on the wire is chosen by the sender.
// A server that trusted them could be made to allocate gigabytes by a single
// 4-byte header (a trivial memory-exhaustion denial of service), and one
// enormous value would also monopolise a connection shared by many requests.
// Bounding keys, values and whole frames caps the memory any one request
// can cost. All three are configurable through Limits.
//
// requestHeaderSize and responseHeaderSize are the fixed-size parts of each
// payload, i.e. everything except the key and value bytes themselves:
//
//	request:  id 4 + op 1 + flags 1 + version 8 + keyLen 4 + valLen 4 = 22 bytes
//	response: id 4 + status 1 + version 8 + valLen 4                  = 17 bytes
//
// A payload shorter than its header size cannot possibly be valid, which
// lets the decoders reject it with one comparison before touching any field.
const (
	DefaultMaxKeySize   = 1 << 10 // 1 KiB
	DefaultMaxValueSize = 1 << 20 // 1 MiB
	// (The frame limit must exceed MaxValueSize plus the header and the largest
	// allowed key, or a maximal but legal PUT could never be sent.)
	// DefaultMaxFrameSize bounds a single frame. It must fit the largest
	// request plus headers; scan pages and stats documents also fit in it.
	DefaultMaxFrameSize = 4 << 20 // 4 MiB

	// Unexported: these are details of the encoding, not knobs for callers.
	requestHeaderSize  = 4 + 1 + 1 + 8 + 4 + 4
	responseHeaderSize = 4 + 1 + 8 + 4
)

// Limits bounds key, value and frame sizes.
//
// Fields (all in bytes):
//   - MaxKeySize: longest key accepted by Validate.
//   - MaxValueSize: longest value accepted by Validate (except
//     OpApplyBatch, whose value is a whole batch of entries).
//   - MaxFrameSize: longest frame ReadFrame will accept. Checked from the
//     4-byte header, BEFORE the payload is read or allocated.
//
// Limits is a plain value type (no pointers), so copying it into each
// ServerConfig is cheap and each server can be configured independently.
type Limits struct {
	MaxKeySize   int
	MaxValueSize int
	MaxFrameSize int
}

// DefaultLimits returns the default protocol limits.
//
// A function (rather than an exported var) returns a fresh copy each time,
// so no caller can accidentally modify the defaults seen by everyone else.
func DefaultLimits() Limits {
	return Limits{
		MaxKeySize:   DefaultMaxKeySize,
		MaxValueSize: DefaultMaxValueSize,
		MaxFrameSize: DefaultMaxFrameSize,
	}
}

// Sentinel errors. Callers test for them with errors.Is, which also sees
// through wrapping: the functions below return
// fmt.Errorf("%w: details", ErrMalformed), which keeps the precise message
// for logs while remaining matchable as ErrMalformed.
var (
	// ErrFrameTooLarge is returned when a frame exceeds MaxFrameSize.
	// The connection cannot be resynchronised afterwards and must be closed.
	//
	// Why it cannot be resynchronised: ReadFrame refuses to read the oversized
	// body, so those bytes are still sitting in the stream. The next 4 bytes we
	// read would be somewhere in the middle of that body and would be
	// misinterpreted as a length prefix. There is no marker to scan for to find
	// the next frame, so the only safe option is to close the connection.
	ErrFrameTooLarge = errors.New("protocol: frame too large")
	// ErrMalformed is returned for payloads that cannot be decoded.
	//
	// Unlike ErrFrameTooLarge this is recoverable: the frame was read completely,
	// so the stream is still positioned exactly at the start of the next frame.
	// The server answers StatusBadRequest and keeps the connection open.
	ErrMalformed = errors.New("protocol: malformed message")
)

// Request is a decoded request.
//
// Fields, in wire order:
//   - ID: echoed in the response; assigned by the transport. Callers leave it
//     zero; MuxConn/Conn stamp a fresh per-connection ID just before sending,
//     so IDs are only unique within one connection.
//   - Op: which operation (GET, PUT, ...).
//   - Flags: bit set of Flag* values (why this write was sent).
//   - Version: write version assigned by the coordinator (0 = unset). Nodes
//     use it for last-writer-wins: a write only replaces a stored value with
//     a lower version. This is what makes retrying a write safe.
//   - Key: the key. A Go string can hold arbitrary bytes, not only UTF-8.
//   - Value: the value bytes (for PUT, and for SCAN/APPLY_BATCH parameters).
type Request struct {
	ID      uint32 // echoed in the response; assigned by the transport
	Op      Op
	Flags   uint8
	Version uint64 // write version assigned by the coordinator (0 = unset)
	Key     string
	Value   []byte
}

// Response is a decoded response.
//
// Fields, in wire order:
//   - ID: ID of the request this answers (0 = connection-level error that
//     answers no particular request, e.g. "server at connection limit").
//   - Status: outcome; see Status.
//   - Version: version of the value returned (GET) or applied (PUT/DELETE).
//   - Value: the returned value, a JSON document (PING/STATS), an encoded
//     ScanPage (SCAN), or a human-readable message for error statuses.
type Response struct {
	ID      uint32 // ID of the request this answers
	Status  Status
	Version uint64 // version of the value returned (GET) or applied (PUT/DELETE)
	Value   []byte
}

// Err converts an error status into a Go error. OK and NotFound return nil.
//
// NotFound is deliberately not an error: callers check resp.Status for it.
// Treating it as an error would, for example, make a missing key look like a
// failing node to code that counts errors.
func (r *Response) Err() error {
	switch r.Status {
	case StatusOK, StatusNotFound:
		return nil
	default:
		// The remote side's message travels in Value, so include it verbatim.
		return fmt.Errorf("remote %s: %s", r.Status, r.Value)
	}
}

// ErrorResponse builds a response carrying an error message.
//
// The ID is left zero; the server fills in the ID of the request being
// answered just before writing it.
func ErrorResponse(s Status, format string, args ...any) *Response {
	return &Response{Status: s, Value: []byte(fmt.Sprintf(format, args...))}
}

// Validate checks a request against the limits.
//
// Decoding (DecodeRequest) and validation are separate steps on purpose:
// decoding checks SYNTAX (are the bytes well formed?), validation checks
// SEMANTICS (is this a request we are willing to execute: known op, key
// present, sizes within limits?). Both failures are answered with
// StatusBadRequest by the server, and in both cases the frame was fully
// consumed, so the connection remains usable.
func (r *Request) Validate(l Limits) error {
	// len on a string counts bytes, not characters, which is what we want for
	// a size limit.
	if len(r.Key) > l.MaxKeySize {
		return fmt.Errorf("%w: key is %d bytes, max %d", ErrMalformed, len(r.Key), l.MaxKeySize)
	}
	// A batch carries many values; only the frame limit bounds it.
	if len(r.Value) > l.MaxValueSize && r.Op != OpApplyBatch {
		return fmt.Errorf("%w: value is %d bytes, max %d", ErrMalformed, len(r.Value), l.MaxValueSize)
	}
	// Per-op rules. Data operations are meaningless without a key.
	switch r.Op {
	case OpGet, OpPut, OpDelete, OpLocate:
		if len(r.Key) == 0 {
			return fmt.Errorf("%w: %s requires a non-empty key", ErrMalformed, r.Op)
		}
	// These carry no key (or, for SCAN, a cursor that is empty on the first
	// page), so an empty key is fine.
	case OpPing, OpStats, OpScan, OpApplyBatch:
	// An unknown op is rejected here rather than reaching a handler: it could
	// come from a newer client or from corrupted bytes, and either way we must
	// not guess what it means.
	default:
		return fmt.Errorf("%w: unknown op %d", ErrMalformed, uint8(r.Op))
	}
	return nil
}

// AppendRequest appends the encoded payload of r (without the length prefix).
//
// The "Append" style (take a dst slice, return the extended slice, like the
// standard library's strconv.AppendInt) lets callers reuse one buffer across
// many messages, so steady-state encoding allocates nothing. The transport
// relies on this: frameWriter appends many frames into one shared buffer.
//
// Fields are written in exactly the order of the layout in the package
// comment; DecodeRequest reads them back in the same order.
func AppendRequest(dst []byte, r *Request) []byte {
	dst = binary.BigEndian.AppendUint32(dst, r.ID)
	dst = append(dst, byte(r.Op), r.Flags)
	dst = binary.BigEndian.AppendUint64(dst, r.Version)
	// Lengths are written as u32. Validate keeps real keys and values far below
	// 4 GiB, so the uint32 conversion cannot truncate for any request we accept.
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Key)))
	dst = append(dst, r.Key...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Value)))
	dst = append(dst, r.Value...)
	return dst
}

// DecodeRequest decodes a request payload. The returned Value aliases p.
//
// "Aliases" means r.Value is a sub-slice of p that shares its memory: no
// copy is made. That is cheap, but it means the caller must not overwrite p
// while r is in use. The transport allocates a fresh buffer per frame
// (ReadFrame's make), so aliasing is safe there. Key is converted with
// string(...), which copies, because Go strings are immutable.
//
// Every read is bounds-checked first; malformed input of any shape returns
// an error wrapping ErrMalformed and never panics. This matters because p
// comes from the network: in Go an unrecovered panic in ANY goroutine
// terminates the whole program, and the server has no recover, so a panic
// here would let any client crash the entire process.
func DecodeRequest(p []byte) (*Request, error) {
	// Too short to hold even the fixed-size fields: reject before indexing.
	if len(p) < requestHeaderSize {
		return nil, fmt.Errorf("%w: request payload is %d bytes, header needs %d", ErrMalformed, len(p), requestHeaderSize)
	}
	// The fixed-size prefix is at known offsets (see the package comment):
	// id at 0..3, op at 4, flags at 5, version at 6..13. Because we checked
	// len(p) >= requestHeaderSize above, these indexes are in bounds.
	r := &Request{ID: binary.BigEndian.Uint32(p), Op: Op(p[4]), Flags: p[5], Version: binary.BigEndian.Uint64(p[6:14])}
	// Advance past the 14 fixed bytes; p now starts at keyLen.
	p = p[14:]
	// readBytes consumes "u32 length + that many bytes" and returns the rest,
	// so p advances like a cursor through the payload.
	key, p, err := readBytes(p, "key")
	if err != nil {
		return nil, err
	}
	val, p, err := readBytes(p, "value")
	if err != nil {
		return nil, err
	}
	// The frame length and the inner lengths must agree exactly. Extra bytes
	// indicate a corrupt or mismatched encoder; accepting them would hide bugs.
	if len(p) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(p))
	}
	r.Key = string(key)
	r.Value = val
	return r, nil
}

// AppendResponse appends the encoded payload of r (without the length prefix).
// Same Append style and field order as AppendRequest; see the package comment
// for the layout.
func AppendResponse(dst []byte, r *Response) []byte {
	dst = binary.BigEndian.AppendUint32(dst, r.ID)
	dst = append(dst, byte(r.Status))
	dst = binary.BigEndian.AppendUint64(dst, r.Version)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Value)))
	dst = append(dst, r.Value...)
	return dst
}

// DecodeResponse decodes a response payload. The returned Value aliases p.
// It is the mirror image of AppendResponse and, like DecodeRequest, checks
// every length before slicing so hostile input cannot cause a panic.
func DecodeResponse(p []byte) (*Response, error) {
	if len(p) < responseHeaderSize {
		return nil, fmt.Errorf("%w: response payload is %d bytes, header needs %d", ErrMalformed, len(p), responseHeaderSize)
	}
	// Fixed prefix: id at 0..3, status at 4, version at 5..12; valLen starts at 13.
	r := &Response{ID: binary.BigEndian.Uint32(p), Status: Status(p[4]), Version: binary.BigEndian.Uint64(p[5:13])}
	val, rest, err := readBytes(p[13:], "value")
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(rest))
	}
	r.Value = val
	return r, nil
}

// RequestID extracts the request ID from a payload that may be otherwise
// malformed, so an error response can still be matched to its caller.
//
// Why this matters with multiplexing: a MuxConn client has many requests in
// flight and routes each response by ID. If the server answered a garbled
// request with ID 0, the client would treat it as a connection-level error
// and fail every pending request on the connection. Echoing the real ID
// (the first 4 bytes, which are usually intact) confines the error to the
// one caller that sent the bad request.
func RequestID(p []byte) uint32 {
	// Not even 4 bytes: there is no ID to recover, fall back to 0.
	if len(p) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(p)
}

// readBytes reads a u32 length-prefixed byte string.
//
// It returns the bytes (aliasing p) and the remainder of p after them. The
// `what` argument only improves error messages ("truncated key length").
func readBytes(p []byte, what string) (val, rest []byte, err error) {
	// Need 4 bytes just for the length field.
	if len(p) < 4 {
		return nil, nil, fmt.Errorf("%w: truncated %s length", ErrMalformed, what)
	}
	n := binary.BigEndian.Uint32(p)
	p = p[4:]
	// Compare as uint64: n is a uint32 taken from the wire and len(p) is an int.
	// Converting both to uint64 avoids any overflow or sign surprises (for
	// example on 32-bit platforms, where int cannot represent every uint32).
	// Without this check, p[:n] would panic on a length that lies.
	if uint64(n) > uint64(len(p)) {
		return nil, nil, fmt.Errorf("%w: %s length %d exceeds remaining %d bytes", ErrMalformed, what, n, len(p))
	}
	// p[:n] is the field, p[n:] is everything after it.
	return p[:n], p[n:], nil
}

// AppendRequestFrame appends a complete frame (length prefix + payload) for r.
//
// Trick: we don't know the payload length until it has been encoded, so we
// reserve 4 placeholder bytes, encode the payload after them, then go back
// and overwrite the placeholder with the real length ("backpatching"):
//
//	dst before:  [ ...earlier frames... ]
//	reserve:     [ ...earlier frames... | 00 00 00 00 ]
//	encode:      [ ...earlier frames... | 00 00 00 00 | payload... ]
//	backpatch:   [ ...earlier frames... |   length    | payload... ]
//	                                    ^ start
//
// This encodes straight into the destination buffer in one pass, with no
// temporary payload buffer and no copy. `start` makes it correct even when
// dst already holds other frames (as in frameWriter's shared buffer).
func AppendRequestFrame(dst []byte, r *Request) []byte {
	// Remember where this frame begins inside dst.
	start := len(dst)
	// Placeholder for the length prefix.
	dst = append(dst, 0, 0, 0, 0)
	dst = AppendRequest(dst, r)
	// Payload length = everything appended after the 4-byte placeholder. Note
	// that dst[start:] is re-sliced AFTER the appends, because append may have
	// moved the data to a new, larger array.
	binary.BigEndian.PutUint32(dst[start:], uint32(len(dst)-start-4))
	return dst
}

// AppendResponseFrame appends a complete frame (length prefix + payload) for r.
// It uses the same reserve-then-backpatch technique as AppendRequestFrame.
// The server calls it inside frameWriter.write, so many responses are encoded
// directly into one shared send buffer.
func AppendResponseFrame(dst []byte, r *Response) []byte {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	dst = AppendResponse(dst, r)
	binary.BigEndian.PutUint32(dst[start:], uint32(len(dst)-start-4))
	return dst
}

// WriteFrame writes a length-prefixed frame. The caller should set a write
// deadline on the underlying connection.
//
// (Without a deadline, a Write to a peer that has stopped reading can block
// forever once the socket's send buffer fills up.)
//
// The production paths use AppendRequestFrame/AppendResponseFrame into
// reusable buffers instead; WriteFrame is the simple, allocating form, used
// by the tests to hand-craft frames.
func WriteFrame(w io.Writer, payload []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	// A single Write keeps small frames in one TCP segment.
	//
	// Writing the 4-byte header and the payload separately would cost two
	// syscalls and, with TCP_NODELAY set, could send the header as its own tiny
	// packet. With Nagle's algorithm enabled instead, the classic
	// "write-write-read" pattern can stall waiting for a delayed ACK. Copying
	// header and payload into one buffer avoids both problems.
	buf := make([]byte, 0, 4+len(payload))
	buf = append(buf, hdr[:]...)
	buf = append(buf, payload...)
	_, err := w.Write(buf)
	return err
}

// ReadFrame reads one length-prefixed frame, rejecting frames larger than max
// before allocating memory for them.
//
// Two reads: first exactly 4 bytes of header, then exactly n bytes of body.
// Both use io.ReadFull, because a single Read on a TCP connection may return
// FEWER bytes than requested (whatever has arrived so far). A plain Read
// would work in tests on loopback and then break under real network
// conditions when a frame arrives in pieces. ReadFull loops until the buffer
// is full or an error occurs.
//
// In the transport, r is a bufio.Reader wrapped around the socket, so these
// small reads are usually served from memory: one read syscall can pull in
// many frames at once.
//
// Error semantics (TestFrames relies on both):
//   - EOF before any header byte: returns io.EOF. The peer closed the
//     connection cleanly BETWEEN frames; this is a normal end of stream.
//   - EOF in the middle of a header or body: io.ErrUnexpectedEOF. The peer
//     vanished mid-message; the frame is incomplete. (io.ReadFull itself
//     already reports a partial header this way.)
//   - Length > max: ErrFrameTooLarge, and the body is NOT read (see
//     ErrFrameTooLarge for why the connection must then be closed).
//   - A read deadline expiring shows up as a net.Error with Timeout() true.
func ReadFrame(r io.Reader, max int) ([]byte, error) {
	// Fixed-size array on the stack: no heap allocation for the header.
	var hdr [4]byte
	// ReadFull returns io.EOF only if it read zero bytes, io.ErrUnexpectedEOF if
	// it read some but not all 4.
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	// Decode the big-endian length.
	n := binary.BigEndian.Uint32(hdr[:])
	// The critical defence: check the claimed size BEFORE make([]byte, n). A
	// hostile or buggy peer could otherwise send 4 bytes claiming ~4 GiB and make
	// us allocate it. uint64 on both sides avoids int-conversion surprises.
	if uint64(n) > uint64(max) {
		return nil, fmt.Errorf("%w: %d bytes, max %d", ErrFrameTooLarge, n, max)
	}
	// A fresh buffer per frame. Decoded Values alias it (see DecodeRequest), so
	// it must not be reused for the next frame.
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		// We already consumed the header, so hitting EOF now means the frame was
		// cut short. Report it as ErrUnexpectedEOF so callers can distinguish it
		// from a clean close between frames.
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return buf, nil
}
