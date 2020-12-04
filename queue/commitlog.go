package queue

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
)

type actionFlags uint8

const (
	endOfRecord actionFlags = 1 << iota
	actionWithName
)

type logWriter interface {
	io.Writer
	io.StringWriter
	WriteByte(byte) error
	Sync() error
}

type commitLog struct {
	// Maps stream names to an index.
	// Stream names are indexed starting with 0 based on the order
	// of the commits.
	names map[string]uint16
	w     *writer
	size  int
}

type action struct {
	flags      actionFlags
	pos        uint64
	streamName string
}

type actionIface interface {
	write(c *commitLog) (n int, err error)
	read(r *reader) error
}

type appendAction struct {
	a    action
	data []byte
}

type pollardAction struct {
	a          action
	pollardPos uint64
}

type reader struct {
	bufio.Reader
	names []string
}

type writer struct {
	bufio.Writer
	f *os.File
}

func newCommitLog() *commitLog {
	// TODO: Writer
	return &commitLog{names: make(map[string]uint16)}
}

func (c *commitLog) create(fileName string) error {
	panic("TODO")
}

func (c *commitLog) read(fileName string) error {
	panic("TODO")
}

func (c *commitLog) commit(a *action) error {
	n, err := a.write(c)
	if err != nil {
		return err
	}
	c.size += n
	c.w.Sync()
	return nil
}

func newReader(f io.Reader) *reader {
	r := &reader{}
	r.Reset(f)
	return r
}

func (r *reader) peekAction() (actionFlags, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	err = r.UnreadByte()
	if err != nil {
		panic("Oooops")
	}
	return actionFlags(b), nil
}

func newWriter(f *os.File) *writer {
	w := &writer{f: f}
	w.Reset(f)
	return w
}

func (w *writer) Sync() error {
	err := w.Flush()
	if err != nil {
		return err
	}
	err = w.f.Sync()
	return err
}

func encodeStreamName(name string, pos uint64) string {
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], pos)
	return name + "/" + hex.EncodeToString(buffer[:])
}

func (a *action) write(c *commitLog) (n int, err error) {
	var buffer [8]byte
	var n2 int
	flags := a.flags
	if index, ok := c.names[a.streamName]; ok {
		buffer[0] = byte(flags)
		binary.LittleEndian.PutUint16(buffer[1:], index)
		if n2, err = c.w.Write(buffer[:3]); err != nil {
			return
		}
		n += n2
	} else {
		index := uint16(len(c.names))
		c.names[a.streamName] = index
		flags |= actionWithName
		buffer[0] = byte(flags)
		if err = c.w.WriteByte(buffer[0]); err != nil {
			return
		}
		n++
		if n2, err = c.w.WriteString(a.streamName); err != nil {
			return
		}
		n += n2
		if err = c.w.WriteByte(0); err != nil {
			return
		}
		n++
	}
	return
}

func (a *action) read(r *reader) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	a.flags = actionFlags(b)
	var buffer [8]byte
	if (a.flags & actionWithName) == actionWithName {
		str, err := r.ReadString(0)
		if err != nil {
			return err
		}
		a.streamName = str
		r.names = append(r.names, str)
	} else {
		_, err := r.Read(buffer[:2])
		if err != nil {
			return err
		}
		index := binary.LittleEndian.Uint16(buffer[:2])
		if index >= uint16(len(r.names)) {
			return errors.New("Wrong index")
		}
		a.streamName = r.names[index]
	}
	return nil
}

func (a *appendAction) write(c *commitLog) (n int, err error) {
	if n, err = a.a.write(c); err != nil {
		return
	}
	var buffer [8]byte
	binary.LittleEndian.PutUint32(buffer[:4], uint32(len(a.data)))
	if _, err = c.w.Write(buffer[:4]); err != nil {
		return
	}
	n += 4
	if _, err = c.w.Write(a.data); err != nil {
		return
	}
	n += len(a.data)
	return
}

func (a *appendAction) read(r *reader) (err error) {
	if err = a.a.read(r); err != nil {
		return
	}
	var buffer [8]byte
	if _, err = r.Read(buffer[:4]); err != nil {
		return
	}
	l := int(binary.LittleEndian.Uint32(buffer[:]))
	a.data = make([]byte, l)
	if _, err = r.Read(a.data); err != nil {
		return
	}
	return
}

func (a *pollardAction) write(c *commitLog) (n int, err error) {
	if n, err = a.write(c); err != nil {
		return
	}
	var buffer [8]byte
	binary.LittleEndian.PutUint64(buffer[:4], a.pollardPos)
	if _, err = c.w.Write(buffer[:8]); err != nil {
		return
	}
	n += 8
	return
}

func (a *pollardAction) read(r *reader) (err error) {
	if err = a.read(r); err != nil {
		return
	}
	var buffer [8]byte
	if _, err = r.Read(buffer[:8]); err != nil {
		return
	}
	a.pollardPos = binary.LittleEndian.Uint64(buffer[:])
	return
}
