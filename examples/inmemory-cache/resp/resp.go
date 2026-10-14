// Package resp implements the small part of the Redis serialization protocol
// (RESP2) that the cache example needs: commands as arrays of bulk strings or
// inline text, and simple, error, integer, bulk, nil and array replies.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Limits keep a misbehaving peer from allocating unbounded memory.
const (
	MaxArgs      = 1024
	MaxBulkBytes = 64 << 10
	maxLineBytes = 64 << 10
)

const (
	integerBase = 10
	integerBits = 64
)

// ErrProtocol reports malformed or oversized input.
var ErrProtocol = errors.New("resp: protocol error")

// ReadCommand reads one command: a RESP array of bulk strings, or an inline
// command line such as "PING" typed into telnet.
func ReadCommand(r *bufio.Reader) ([]string, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return strings.Fields(line), nil
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 || n > MaxArgs {
		return nil, ErrProtocol
	}
	args := make([]string, 0, n)
	for range n {
		head, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if len(head) == 0 || head[0] != '$' {
			return nil, ErrProtocol
		}
		arg, err := readBulk(r, head[1:])
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

func readLine(r *bufio.Reader) (string, error) {
	var line []byte
	for {
		chunk, isPrefix, err := r.ReadLine()
		if err != nil {
			return "", err
		}
		line = append(line, chunk...)
		if len(line) > maxLineBytes {
			return "", ErrProtocol
		}
		if !isPrefix {
			return string(line), nil
		}
	}
}

func readBulk(r *bufio.Reader, size string) (string, error) {
	n, err := strconv.Atoi(size)
	if err != nil || n < 0 || n > MaxBulkBytes {
		return "", ErrProtocol
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return "", ErrProtocol
	}
	return string(buf[:n]), nil
}

// Writer encodes replies. Call Flush after each complete reply.
type Writer struct{ *bufio.Writer }

// NewWriter buffers RESP output to the supplied writer.
func NewWriter(w io.Writer) Writer { return Writer{bufio.NewWriter(w)} }

// Simple writes a RESP simple string.
func (w Writer) Simple(s string) { _, _ = fmt.Fprintf(w, "+%s\r\n", s) }

func (w Writer) Error(s string) {
	_, _ = fmt.Fprintf(w, "-%s\r\n", strings.ReplaceAll(s, "\r\n", " "))
}

// Int writes a RESP integer.
func (w Writer) Int(n int64) { _, _ = fmt.Fprintf(w, ":%d\r\n", n) }

// Nil writes a RESP null bulk string.
func (w Writer) Nil() { _, _ = w.WriteString("$-1\r\n") }

// Array writes the header for a RESP array of the supplied length.
func (w Writer) Array(n int) { _, _ = fmt.Fprintf(w, "*%d\r\n", n) }

// Bulk writes a RESP bulk string with its byte length.
func (w Writer) Bulk(s string) { _, _ = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s) }

// Command encodes a request as an array of bulk strings.
func (w Writer) Command(args ...string) {
	w.Array(len(args))
	for _, arg := range args {
		w.Bulk(arg)
	}
}

// Reply is one decoded server reply.
type Reply struct {
	Kind  byte // '+', '-', ':', '$', '*'
	Str   string
	Int   int64
	Nil   bool
	Array []Reply
}

// Err returns the server error carried by an error reply.
func (r Reply) Err() error {
	if r.Kind == '-' {
		return errors.New(r.Str)
	}
	return nil
}

// ReadReply decodes one reply.
func ReadReply(r *bufio.Reader) (Reply, error) {
	line, err := readLine(r)
	if err != nil {
		return Reply{}, err
	}
	if len(line) == 0 {
		return Reply{}, ErrProtocol
	}
	reply := Reply{Kind: line[0]}
	body := line[1:]
	switch reply.Kind {
	case '+', '-':
		reply.Str = body
	case ':':
		reply.Int, err = strconv.ParseInt(body, integerBase, integerBits)
	case '$':
		if body == "-1" {
			reply.Nil = true
			return reply, nil
		}
		reply.Str, err = readBulk(r, body)
	case '*':
		n, convErr := strconv.Atoi(body)
		if convErr != nil || n < -1 || n > MaxArgs {
			return Reply{}, ErrProtocol
		}
		reply.Nil = n == -1
		for range max(n, 0) {
			item, err := ReadReply(r)
			if err != nil {
				return Reply{}, err
			}
			reply.Array = append(reply.Array, item)
		}
	default:
		return Reply{}, ErrProtocol
	}
	if err != nil {
		return Reply{}, ErrProtocol
	}
	return reply, nil
}
