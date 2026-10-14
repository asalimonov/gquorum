package resp

import (
	"bufio"
	"context"
	"net"
	"time"
)

// Client is one synchronous connection. It is not safe for concurrent use.
type Client struct {
	conn net.Conn
	r    *bufio.Reader
	w    Writer
}

// Dial opens a connection bounded by ctx.
func Dial(ctx context.Context, addr string) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, r: bufio.NewReader(conn), w: NewWriter(conn)}, nil
}

// Do sends one command and waits for its reply until timeout. A transport
// error leaves the connection unusable; close it and dial again.
func (c *Client) Do(timeout time.Duration, args ...string) (Reply, error) {
	if err := c.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return Reply{}, err
	}
	c.w.Command(args...)
	if err := c.w.Flush(); err != nil {
		return Reply{}, err
	}
	return ReadReply(c.r)
}

// Close releases the client's network connection.
func (c *Client) Close() error { return c.conn.Close() }
