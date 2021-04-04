package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"time"

	quic "github.com/lucas-clemente/quic-go"
)

// conn is the low-level implementation of Conn
type quicConn struct {
	// Shared
	mu      sync.Mutex
	pending int
	err     error
	session quic.Session
	stream  quic.Stream
	// Read
	readTimeout time.Duration
	br          *reader

	// Write
	writeTimeout time.Duration
	bw           *writer
}

func DialQ(address string, options ...DialOption) (Conn, error) {
	return DialContextQ(context.Background(), address, options...)
}

// DialContext connects to the Redis server at the given network and
// address using the specified options and context.
func DialContextQ(ctx context.Context, address string, options ...DialOption) (Conn, error) {
	do := dialOptions{}
	for _, option := range options {
		option.f(&do)
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"quic-echo-example"},
	}
	cfg := quic.Config{} // EnableDatagrams: false
	session, err := quic.DialAddr(address, tlsConf, &cfg)
	if err != nil {
		return nil, err
	}
	stream, err := session.OpenStream() // OpenStreamSync(context.Background())
	if err != nil {
		return nil, err
	}

	c := &quicConn{
		session:      session,
		stream:       stream,
		bw:           newWriter(stream),
		br:           newReader(stream),
		readTimeout:  do.readTimeout,
		writeTimeout: do.writeTimeout,
	}

	if do.password != "" {
		authArgs := make([]interface{}, 0, 2)
		if do.username != "" {
			authArgs = append(authArgs, do.username)
		}
		authArgs = append(authArgs, do.password)
		if _, err := c.Do("AUTH", authArgs...); err != nil {
			session.CloseWithError(0, c.err.Error())
			return nil, err
		}
	}

	if do.clientName != "" {
		if _, err := c.Do("CLIENT", "SETNAME", do.clientName); err != nil {
			session.CloseWithError(0, c.err.Error())
			return nil, err
		}
	}

	if do.db != 0 {
		if _, err := c.Do("SELECT", do.db); err != nil {
			session.CloseWithError(0, c.err.Error())
			return nil, err
		}
	}

	return c, nil
}

func (c *quicConn) Close() error {
	c.mu.Lock()
	err := c.err
	if c.err == nil {
		c.err = errors.New("redigo: closed")
		err = c.session.CloseWithError(0, c.err.Error())
	}
	c.mu.Unlock()
	return err
}

func (c *quicConn) fatal(err error) error {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		// Close connection to force errors on subsequent calls and to unblock
		// other reader or writer.
		c.session.CloseWithError(0, c.err.Error())
	}
	c.mu.Unlock()
	return err
}

func (c *quicConn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *quicConn) Send(cmd string, args ...interface{}) error {
	c.mu.Lock()
	c.pending += 1
	c.mu.Unlock()
	if c.writeTimeout != 0 {
		if err := c.stream.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return c.fatal(err)
		}
	}
	if err := c.bw.writeCommand(cmd, args); err != nil {
		return c.fatal(err)
	}
	return nil
}

func (c *quicConn) Flush() error {
	if c.writeTimeout != 0 {
		if err := c.stream.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return c.fatal(err)
		}
	}
	if err := c.bw.flush(); err != nil {
		return c.fatal(err)
	}
	return nil
}

func (c *quicConn) Receive() (interface{}, error) {
	return c.ReceiveWithTimeout(c.readTimeout)
}

func (c *quicConn) ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error) {
	var deadline time.Time
	if timeout != 0 {
		deadline = time.Now().Add(timeout)
	}
	if err := c.stream.SetReadDeadline(deadline); err != nil {
		return nil, c.fatal(err)
	}

	if reply, err = c.br.readReply(); err != nil {
		return nil, c.fatal(err)
	}
	// When using pub/sub, the number of receives can be greater than the
	// number of sends. To enable normal use of the connection after
	// unsubscribing from all channels, we do not decrement pending to a
	// negative value.
	//
	// The pending field is decremented after the reply is read to handle the
	// case where Receive is called before Send.
	c.mu.Lock()
	if c.pending > 0 {
		c.pending -= 1
	}
	c.mu.Unlock()
	if err, ok := reply.(Error); ok {
		return nil, err
	}
	return
}

func (c *quicConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	return c.DoWithTimeout(c.readTimeout, cmd, args...)
}

func (c *quicConn) DoWithTimeout(readTimeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	c.mu.Lock()
	pending := c.pending
	c.pending = 0
	c.mu.Unlock()

	if cmd == "" && pending == 0 {
		return nil, nil
	}

	if c.writeTimeout != 0 {
		if err := c.stream.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return nil, c.fatal(err)
		}
	}

	if cmd != "" {
		if err := c.bw.writeCommand(cmd, args); err != nil {
			return nil, c.fatal(err)
		}
	}

	if err := c.bw.flush(); err != nil {
		return nil, c.fatal(err)
	}

	var deadline time.Time
	if readTimeout != 0 {
		deadline = time.Now().Add(readTimeout)
	}
	if err := c.stream.SetReadDeadline(deadline); err != nil {
		return nil, c.fatal(err)
	}

	if cmd == "" {
		reply := make([]interface{}, pending)
		for i := range reply {
			r, e := c.br.readReply()
			if e != nil {
				return nil, c.fatal(e)
			}
			reply[i] = r
		}
		return reply, nil
	}

	var err error
	var reply interface{}
	for i := 0; i <= pending; i++ {
		var e error
		if reply, e = c.br.readReply(); e != nil {
			return nil, c.fatal(e)
		}
		if e, ok := reply.(Error); ok && err == nil {
			err = e
		}
	}
	return reply, err
}
