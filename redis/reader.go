package redis

import (
	"bufio"
	"io"
)

type reader struct {
	rd *bufio.Reader
}

func newReader(rd io.Reader) *reader {
	return &reader{
		rd: bufio.NewReader(rd),
	}
}

// readLine reads a line of input from the RESP stream.
func (r *reader) readLine() ([]byte, error) {
	// To avoid allocations, attempt to read the line using ReadSlice. This
	// call typically succeeds. The known case where the call fails is when
	// reading the output from the MONITOR command.
	p, err := r.rd.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// The line does not fit in the bufio.Reader's buffer. Fall back to
		// allocating a buffer for the line.
		buf := append([]byte{}, p...)
		for err == bufio.ErrBufferFull {
			p, err = r.rd.ReadSlice('\n')
			buf = append(buf, p...)
		}
		p = buf
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response line terminator")
	}
	return p[:i], nil
}

func (r *reader) readReply() (interface{}, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	switch line[0] {
	case '+':
		switch string(line[1:]) {
		case "OK":
			// Avoid allocation for frequent "+OK" response.
			return okReply, nil
		case "PONG":
			// Avoid allocation in PING command benchmarks :)
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return Error(line[1:]), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(r.rd, p)
		if err != nil {
			return nil, err
		}
		if line, err := r.readLine(); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, protocolError("bad bulk string format")
		}
		return p, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		x := make([]interface{}, n)
		for i := range x {
			x[i], err = r.readReply()
			if err != nil {
				return nil, err
			}
		}
		return x, nil
	}
	return nil, protocolError("unexpected response line")
}
