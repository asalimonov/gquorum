//go:build !functional

package resp

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestReadCommandArrayAndInline(t *testing.T) {
	t.Parallel()
	r := bufio.NewReader(strings.NewReader("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nv a l\r\nPING  hello\r\n"))
	args, err := ReadCommand(r)
	if err != nil || len(args) != 3 || args[0] != "SET" || args[2] != "v a l" {
		t.Fatal(args, err)
	}
	args, err = ReadCommand(r)
	if err != nil || len(args) != 2 || args[0] != "PING" || args[1] != "hello" {
		t.Fatal(args, err)
	}
	for _, bad := range []string{"*2\r\n$1\r\na\r\n:1\r\n", "*1\r\n$3\r\nab\r\n\r\n", "*99999\r\n", "*1\r\n$999999999\r\n"} {
		if _, err := ReadCommand(bufio.NewReader(strings.NewReader(bad))); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestWriterAndReadReplyRoundTrip(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Simple("OK")
	w.Error("ERR bad\r\nthing")
	w.Int(42)
	w.Bulk("value")
	w.Nil()
	w.Array(2)
	w.Bulk("a")
	w.Int(1)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(&buf)
	want := []func(Reply) bool{
		func(r Reply) bool { return r.Kind == '+' && r.Str == "OK" },
		func(r Reply) bool { return r.Err() != nil && r.Str == "ERR bad thing" },
		func(r Reply) bool { return r.Kind == ':' && r.Int == 42 },
		func(r Reply) bool { return r.Kind == '$' && r.Str == "value" && !r.Nil },
		func(r Reply) bool { return r.Kind == '$' && r.Nil },
		func(r Reply) bool {
			return r.Kind == '*' && len(r.Array) == 2 && r.Array[0].Str == "a" && r.Array[1].Int == 1
		},
	}
	for i, check := range want {
		reply, err := ReadReply(r)
		if err != nil || !check(reply) {
			t.Fatalf("reply %d: %+v %v", i, reply, err)
		}
	}
	if _, err := ReadReply(bufio.NewReader(strings.NewReader("?x\r\n"))); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}
