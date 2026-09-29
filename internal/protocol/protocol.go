// Package protocol defines the binary wire format shared by clients,
// the coordinator and storage nodes.
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
// Request payload:
//
//	| id u32 | op u8 | flags u8 | version u64 | keyLen u32 | key | valLen u32 | value |
//
// Response payload:
//
//	| id u32 | status u8 | version u64 | valLen u32 | value |
//
// All integers are big-endian. For error statuses the value carries a
// human-readable error message.
//
// The request ID is echoed in the response. It lets one connection carry many
// concurrent requests (multiplexing): the server may answer them in any order
// and the client matches responses to callers by ID.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Op identifies the operation requested.
type Op uint8

const (
	OpGet    Op = 1
	OpPut    Op = 2
	OpDelete Op = 3
	OpPing   Op = 4 // health check; response value is a JSON health document
	OpStats  Op = 5 // response value is a JSON stats document
	OpScan   Op = 6 // node-only: page through stored entries (anti-entropy)
	OpLocate Op = 7 // coordinator-only: return the replica set for a key
	// OpApplyBatch is node-only: apply many versioned entries (encoded as a
	// ScanPage in Value) in one round trip. Used by resynchronisation.
	OpApplyBatch Op = 8
)

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
const (
	// FlagReplica marks a write forwarded by the coordinator to a non-primary
	// replica. Nodes count these as replication operations.
	FlagReplica uint8 = 1 << 0
	// FlagSync marks a write pushed during anti-entropy resynchronisation.
	FlagSync uint8 = 1 << 1
)

// Status is the outcome of a request.
type Status uint8

const (
	StatusOK          Status = 0
	StatusNotFound    Status = 1
	StatusError       Status = 2 // internal server error
	StatusBadRequest  Status = 3 // malformed or oversized request
	StatusUnavailable Status = 4 // not enough healthy replicas / quorum not met
)

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
const (
	DefaultMaxKeySize   = 1 << 10 // 1 KiB
	DefaultMaxValueSize = 1 << 20 // 1 MiB
	// DefaultMaxFrameSize bounds a single frame. It must fit the largest
	// request plus headers; scan pages and stats documents also fit in it.
	DefaultMaxFrameSize = 4 << 20 // 4 MiB

	requestHeaderSize  = 4 + 1 + 1 + 8 + 4 + 4
	responseHeaderSize = 4 + 1 + 8 + 4
)

// Limits bounds key, value and frame sizes.
type Limits struct {
	MaxKeySize   int
	MaxValueSize int
	MaxFrameSize int
}

// DefaultLimits returns the default protocol limits.
func DefaultLimits() Limits {
	return Limits{
		MaxKeySize:   DefaultMaxKeySize,
		MaxValueSize: DefaultMaxValueSize,
		MaxFrameSize: DefaultMaxFrameSize,
	}
}

var (
	// ErrFrameTooLarge is returned when a frame exceeds MaxFrameSize.
	// The connection cannot be resynchronised afterwards and must be closed.
	ErrFrameTooLarge = errors.New("protocol: frame too large")
	// ErrMalformed is returned for payloads that cannot be decoded.
	ErrMalformed = errors.New("protocol: malformed message")
)

// Request is a decoded request.
type Request struct {
	ID      uint32 // echoed in the response; assigned by the transport
	Op      Op
	Flags   uint8
	Version uint64 // write version assigned by the coordinator (0 = unset)
	Key     string
	Value   []byte
}

// Response is a decoded response.
type Response struct {
	ID      uint32 // ID of the request this answers
	Status  Status
	Version uint64 // version of the value returned (GET) or applied (PUT/DELETE)
	Value   []byte
}

// Err converts an error status into a Go error. OK and NotFound return nil.
func (r *Response) Err() error {
	switch r.Status {
	case StatusOK, StatusNotFound:
		return nil
	default:
		return fmt.Errorf("remote %s: %s", r.Status, r.Value)
	}
}

// ErrorResponse builds a response carrying an error message.
func ErrorResponse(s Status, format string, args ...any) *Response {
	return &Response{Status: s, Value: []byte(fmt.Sprintf(format, args...))}
}

// Validate checks a request against the limits.
func (r *Request) Validate(l Limits) error {
	if len(r.Key) > l.MaxKeySize {
		return fmt.Errorf("%w: key is %d bytes, max %d", ErrMalformed, len(r.Key), l.MaxKeySize)
	}
	// A batch carries many values; only the frame limit bounds it.
	if len(r.Value) > l.MaxValueSize && r.Op != OpApplyBatch {
		return fmt.Errorf("%w: value is %d bytes, max %d", ErrMalformed, len(r.Value), l.MaxValueSize)
	}
	switch r.Op {
	case OpGet, OpPut, OpDelete, OpLocate:
		if len(r.Key) == 0 {
			return fmt.Errorf("%w: %s requires a non-empty key", ErrMalformed, r.Op)
		}
	case OpPing, OpStats, OpScan, OpApplyBatch:
	default:
		return fmt.Errorf("%w: unknown op %d", ErrMalformed, uint8(r.Op))
	}
	return nil
}

// AppendRequest appends the encoded payload of r (without the length prefix).
func AppendRequest(dst []byte, r *Request) []byte {
	dst = binary.BigEndian.AppendUint32(dst, r.ID)
	dst = append(dst, byte(r.Op), r.Flags)
	dst = binary.BigEndian.AppendUint64(dst, r.Version)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Key)))
	dst = append(dst, r.Key...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Value)))
	dst = append(dst, r.Value...)
	return dst
}

// DecodeRequest decodes a request payload. The returned Value aliases p.
func DecodeRequest(p []byte) (*Request, error) {
	if len(p) < requestHeaderSize {
		return nil, fmt.Errorf("%w: request payload is %d bytes, header needs %d", ErrMalformed, len(p), requestHeaderSize)
	}
	r := &Request{ID: binary.BigEndian.Uint32(p), Op: Op(p[4]), Flags: p[5], Version: binary.BigEndian.Uint64(p[6:14])}
	p = p[14:]
	key, p, err := readBytes(p, "key")
	if err != nil {
		return nil, err
	}
	val, p, err := readBytes(p, "value")
	if err != nil {
		return nil, err
	}
	if len(p) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(p))
	}
	r.Key = string(key)
	r.Value = val
	return r, nil
}

// AppendResponse appends the encoded payload of r (without the length prefix).
func AppendResponse(dst []byte, r *Response) []byte {
	dst = binary.BigEndian.AppendUint32(dst, r.ID)
	dst = append(dst, byte(r.Status))
	dst = binary.BigEndian.AppendUint64(dst, r.Version)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Value)))
	dst = append(dst, r.Value...)
	return dst
}

// DecodeResponse decodes a response payload. The returned Value aliases p.
func DecodeResponse(p []byte) (*Response, error) {
	if len(p) < responseHeaderSize {
		return nil, fmt.Errorf("%w: response payload is %d bytes, header needs %d", ErrMalformed, len(p), responseHeaderSize)
	}
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
func RequestID(p []byte) uint32 {
	if len(p) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(p)
}

// readBytes reads a u32 length-prefixed byte string.
func readBytes(p []byte, what string) (val, rest []byte, err error) {
	if len(p) < 4 {
		return nil, nil, fmt.Errorf("%w: truncated %s length", ErrMalformed, what)
	}
	n := binary.BigEndian.Uint32(p)
	p = p[4:]
	if uint64(n) > uint64(len(p)) {
		return nil, nil, fmt.Errorf("%w: %s length %d exceeds remaining %d bytes", ErrMalformed, what, n, len(p))
	}
	return p[:n], p[n:], nil
}

// AppendRequestFrame appends a complete frame (length prefix + payload) for r.
func AppendRequestFrame(dst []byte, r *Request) []byte {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	dst = AppendRequest(dst, r)
	binary.BigEndian.PutUint32(dst[start:], uint32(len(dst)-start-4))
	return dst
}

// AppendResponseFrame appends a complete frame (length prefix + payload) for r.
func AppendResponseFrame(dst []byte, r *Response) []byte {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	dst = AppendResponse(dst, r)
	binary.BigEndian.PutUint32(dst[start:], uint32(len(dst)-start-4))
	return dst
}

// WriteFrame writes a length-prefixed frame. The caller should set a write
// deadline on the underlying connection.
func WriteFrame(w io.Writer, payload []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	// A single Write keeps small frames in one TCP segment.
	buf := make([]byte, 0, 4+len(payload))
	buf = append(buf, hdr[:]...)
	buf = append(buf, payload...)
	_, err := w.Write(buf)
	return err
}

// ReadFrame reads one length-prefixed frame, rejecting frames larger than max
// before allocating memory for them.
func ReadFrame(r io.Reader, max int) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if uint64(n) > uint64(max) {
		return nil, fmt.Errorf("%w: %d bytes, max %d", ErrFrameTooLarge, n, max)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return buf, nil
}
