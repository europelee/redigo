package redis

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

type writer struct {
	wr *bufio.Writer
	// Scratch space for formatting argument length.
	// '*' or '$', length, "\r\n"
	lenScratch [32]byte

	// Scratch space for formatting integers and floats.
	numScratch [40]byte
}

func newWriter(wr io.Writer) *writer {
	return &writer{
		wr: bufio.NewWriter(wr),
	}
}
func (w *writer) flush() error {
	return w.wr.Flush()
}

func (w *writer) writeLen(prefix byte, n int) error {
	w.lenScratch[len(w.lenScratch)-1] = '\n'
	w.lenScratch[len(w.lenScratch)-2] = '\r'
	i := len(w.lenScratch) - 3
	for {
		w.lenScratch[i] = byte('0' + n%10)
		i -= 1
		n = n / 10
		if n == 0 {
			break
		}
	}
	w.lenScratch[i] = prefix
	_, err := w.wr.Write(w.lenScratch[i:])
	return err
}

func (w *writer) writeString(s string) error {
	if err := w.writeLen('$', len(s)); err != nil {
		return err
	}
	if _, err := w.wr.WriteString(s); err != nil {
		return err
	}
	_, err := w.wr.WriteString("\r\n")
	return err
}

func (w *writer) writeBytes(p []byte) error {
	if err := w.writeLen('$', len(p)); err != nil {
		return err
	}
	if _, err := w.wr.Write(p); err != nil {
		return err
	}
	_, err := w.wr.WriteString("\r\n")
	return err
}

func (w *writer) writeInt64(n int64) error {
	return w.writeBytes(strconv.AppendInt(w.numScratch[:0], n, 10))
}

func (w *writer) writeFloat64(n float64) error {
	return w.writeBytes(strconv.AppendFloat(w.numScratch[:0], n, 'g', -1, 64))
}

func (w *writer) writeCommand(cmd string, args []interface{}) error {
	if err := w.writeLen('*', 1+len(args)); err != nil {
		return err
	}
	if err := w.writeString(cmd); err != nil {
		return err
	}
	for _, arg := range args {
		if err := w.writeArg(arg, true); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) writeArg(arg interface{}, argumentTypeOK bool) (err error) {
	switch arg := arg.(type) {
	case string:
		return w.writeString(arg)
	case []byte:
		return w.writeBytes(arg)
	case int:
		return w.writeInt64(int64(arg))
	case int64:
		return w.writeInt64(arg)
	case float64:
		return w.writeFloat64(arg)
	case bool:
		if arg {
			return w.writeString("1")
		} else {
			return w.writeString("0")
		}
	case nil:
		return w.writeString("")
	case Argument:
		if argumentTypeOK {
			return w.writeArg(arg.RedisArg(), false)
		}
		// See comment in default clause below.
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return w.writeBytes(buf.Bytes())
	default:
		// This default clause is intended to handle builtin numeric types.
		// The function should return an error for other types, but this is not
		// done for compatibility with previous versions of the package.
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return w.writeBytes(buf.Bytes())
	}
}
