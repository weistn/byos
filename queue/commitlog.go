package queue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sort"

	"github.com/weistn/byos/queue/util"
)

type actionFlags uint8

const (
	endOfRecord actionFlags = 1 << iota
	actionWithName
	flagMask    = 0xfc
	flagAppend  = 4
	flagPollard = 8
	flagDict    = 12
)

type streamLog struct {
	// All streams written to the log are counted, startiung with
	// 0 for the first stream.
	// This is used to avoid serializing the same stream name twice.
	number uint16
	// Index into the FAT
	firstFatIndex uint16
	lastFatIndex  uint16
	// The offset of the first stream byte serialized in the log.
	offset uint64
	// The offset of the first stream byte serialized in the log that should be kept.
	// It always holds that offset <= keepOffset.
	keepOffset uint64
	// The number of stream bytes serialized in the log across all fat entries.
	length int
}

type fatEntry struct {
	// Index into the FAT. A value of 0 means end of list.
	next uint16
	// Position in the log where the stream bytes are serialized
	pos int
	// Number of stream bytes serialized at the given position in the log
	length int
}

type commitLog struct {
	// Maps stream names to an index.
	// Stream names are indexed starting with 0 based on the order
	// of the commits.
	streams   map[string]streamLog
	w         *writer
	size      int
	fat       []fatEntry
	finalized bool
}

type action struct {
	flags      actionFlags
	offset     uint64
	streamName string
}

type actionIface interface {
	write(c *commitLog) (n int, err error)
	read(r *reader) error
	recover(c *commitLog) (n int, err error)
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
	b     *bufio.Reader
	names []string
}

type writer struct {
	b *bufio.Writer
	f *os.File
}

var errIsFinalized = errors.New("The commit log is finalized")

func newCommitLog() *commitLog {
	// TODO: Writer
	return &commitLog{streams: make(map[string]streamLog)}
}

func (c *commitLog) create(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	c.w = newWriter(f)
	return nil
}

func (c *commitLog) recover(fileName string) error {
	// Determine size of the file
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return err
	}
	size := fileInfo.Size()

	// Open the file for read/write
	f, err := os.OpenFile(fileName, os.O_RDWR, 0755)
	if err != nil {
		return err
	}

	r := newReader(f)
	// Read all committed actions until end of file
	for int64(c.size) < size {
		a, err := r.peekAction()
		if err != nil {
			break
		}
		switch a & flagMask {
		case flagAppend:
			var a appendAction
			err = a.read(r)
			if err != nil {
				break
			}
			n, err := a.recover(c)
			if err != nil {
				break
			}
			c.size += n
		case flagPollard:
			var a pollardAction
			err = a.read(r)
			if err != nil {
				break
			}
			n, err := a.recover(c)
			if err != nil {
				break
			}
			c.size += n
		case flagDict:
			// TODO: Check that the dict is ok
			c.finalized = true
			return errIsFinalized
		default:
			break
		}
	}

	// From here on we see garbage. Truncate here and continue
	if int64(c.size) != size {
		f.Truncate(int64(c.size))
	}

	// Append new actions using the writer
	c.w = newWriter(f)
	return nil
}

func (c *commitLog) close() error {
	if c.finalized {
		return errIsFinalized
	}
	return c.w.f.Close()
}

func (c *commitLog) commit(a actionIface) error {
	if c.finalized {
		return errIsFinalized
	}
	n, err := a.write(c)
	if err != nil {
		return err
	}
	c.size += n
	c.w.Sync()
	return nil
}

func (c *commitLog) finalize() error {
	if c.finalized {
		return errIsFinalized
	}

	// Sorted list of stream names
	var names []string
	for name := range c.streams {
		names = append(names, name)
	}
	sort.Strings(names)

	// Write tree to buffer first and then persist it
	buf := bytes.NewBuffer(nil)

	// Write flag
	buf.WriteByte(byte(flagDict))

	// Write the tree
	c.writeDictSubtree(buf, names)

	// Write size of dict and magic number
	trailer := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 42, 0, 42, 0xff, 42, 0xff}
	binary.LittleEndian.PutUint64(trailer[:], uint64(buf.Len()))
	if _, err := buf.Write(trailer[:]); err != nil {
		return err
	}

	// Persist the tree
	if _, err := c.w.b.Write(buf.Bytes()); err != nil {
		return err
	}
	if err := c.w.b.Flush(); err != nil {
		return err
	}
	if err := c.w.f.Sync(); err != nil {
		return err
	}

	c.finalized = true
	return c.w.f.Close()
}

func (c *commitLog) writeDictSubtree(buf *bytes.Buffer, names []string) (pos int, err error) {
	if len(names) == 0 {
		return 0, nil
	}

	middle := len(names) / 2
	pos = buf.Len()
	n := names[middle]
	s := c.streams[n]
	// Write positions of left and write subtree
	var lrpos [8]byte
	if _, err := buf.Write(lrpos[:]); err != nil {
		return 0, err
	}
	// Write stream name
	if _, err := buf.WriteString(n); err != nil {
		return 0, err
	}
	if err := buf.WriteByte(0); err != nil {
		return 0, err
	}

	// Write information about the stream
	// Count fat entries
	var fatCount uint16
	fatIndex := s.firstFatIndex
	foffset := s.offset
	for {
		if c.fat[fatIndex].length > 0 {
			if foffset+uint64(c.fat[fatIndex].length) > s.keepOffset {
				fatCount++
			}
			foffset += uint64(c.fat[fatIndex].length)
		}
		if c.fat[fatIndex].next == 0 {
			break
		}
		fatIndex = c.fat[fatIndex].next
	}

	// Write offset of first and last byte, and write number of fat entries
	var fatBuf [18]byte
	binary.LittleEndian.PutUint64(fatBuf[:8], s.keepOffset)
	binary.LittleEndian.PutUint64(fatBuf[8:16], s.offset+uint64(s.length))
	binary.LittleEndian.PutUint16(fatBuf[16:18], fatCount)
	if _, err := buf.Write(fatBuf[:]); err != nil {
		return 0, err
	}

	// Write fat entries
	fatIndex = s.firstFatIndex
	foffset = s.offset
	for {
		if c.fat[fatIndex].length > 0 {
			if foffset+uint64(c.fat[fatIndex].length) > s.keepOffset {
				var skip uint32
				if foffset < s.keepOffset {
					skip = uint32(s.keepOffset - foffset)
				}
				binary.LittleEndian.PutUint32(fatBuf[:4], uint32(c.fat[fatIndex].pos)+skip)
				binary.LittleEndian.PutUint32(fatBuf[4:8], uint32(c.fat[fatIndex].length)-skip)
				if _, err := buf.Write(fatBuf[:8]); err != nil {
					return 0, err
				}
			}
			foffset += uint64(c.fat[fatIndex].length)
		}
		if c.fat[fatIndex].next == 0 {
			break
		}
		fatIndex = c.fat[fatIndex].next
	}

	// Write positions of left and write subtree
	if middle > 0 {
		left, err := c.writeDictSubtree(buf, names[:middle])
		if err != nil {
			return 0, nil
		}
		binary.LittleEndian.PutUint32(buf.Bytes()[pos:], uint32(left))
	}
	if middle+1 != len(names) {
		right, err := c.writeDictSubtree(buf, names[middle+1:])
		if err != nil {
			return 0, nil
		}
		binary.LittleEndian.PutUint32(buf.Bytes()[pos+4:], uint32(right))
	}

	return pos, nil
}

// Returns the range of the stream that is stored in the log.
// Returns an error if the stream is not in the log.
func (c *commitLog) streamRange(streamName string) (span util.Span, err error) {
	s, ok := c.streams[streamName]
	if !ok {
		return util.Span{}, os.ErrNotExist
	}
	return util.Span{From: s.keepOffset, To: s.offset + uint64(s.length)}, nil
}

func (c *commitLog) readStream(streamName string, offset uint64, data []byte) (n int, err error) {
	s, ok := c.streams[streamName]
	if !ok {
		return 0, os.ErrNotExist
	}
	if offset < s.keepOffset || offset+uint64(len(data)) > s.offset+uint64(s.length) {
		return 0, os.ErrInvalid
	}
	findex := s.firstFatIndex
	foffset := s.offset
	for offset >= foffset+uint64(c.fat[findex].length) {
		if c.fat[findex].next == 0 {
			panic("Oooops")
		}
		foffset += uint64(c.fat[findex].length)
		findex = c.fat[findex].next
	}
	toRead := len(data)
	done := 0
	for toRead > 0 {
		pos := c.fat[findex].pos
		posOffset := int(offset - foffset)
		readCount := c.fat[findex].length - posOffset
		if readCount > toRead {
			readCount = toRead
		}
		n2, err := c.w.f.ReadAt(data[done:done+readCount], int64(pos+posOffset))
		if err != nil {
			return 0, nil
		}
		toRead -= n2
		done += n2
		offset += uint64(readCount)
		foffset += uint64(c.fat[findex].length)
		findex = c.fat[findex].next
	}
	return done, nil
}

func newReader(f io.Reader) *reader {
	r := &reader{b: bufio.NewReader(f)}
	return r
}

func (r *reader) peekAction() (actionFlags, error) {
	b, err := r.b.ReadByte()
	if err != nil {
		return 0, err
	}
	err = r.b.UnreadByte()
	if err != nil {
		panic("Oooops")
	}
	return actionFlags(b), nil
}

func newWriter(f *os.File) *writer {
	w := &writer{f: f, b: bufio.NewWriter(f)}
	return w
}

func (w *writer) Sync() error {
	err := w.b.Flush()
	if err != nil {
		return err
	}
	err = w.f.Sync()
	return err
}

func encodeStreamName(name string, offset uint64) string {
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], offset)
	return name + "/" + hex.EncodeToString(buffer[:])
}

func (a *action) write(c *commitLog) (n int, err error) {
	var buffer [9]byte
	var n2 int
	flags := a.flags
	if s, ok := c.streams[a.streamName]; ok {
		// Write flag and stream index
		buffer[0] = byte(flags)
		binary.LittleEndian.PutUint16(buffer[1:], s.number)
		if n2, err = c.w.b.Write(buffer[:3]); err != nil {
			return
		}
		n += n2
	} else {
		index := uint16(len(c.streams))
		c.streams[a.streamName] = streamLog{number: index, offset: a.offset, keepOffset: a.offset}
		// Write flags anf offset
		flags |= actionWithName
		buffer[0] = byte(flags)
		binary.LittleEndian.PutUint64(buffer[1:], a.offset)
		if _, err = c.w.b.Write(buffer[:9]); err != nil {
			return
		}
		n += 9
		// Write string, followed by a zero.
		//		if n2, err = c.w.WriteString(a.streamName); err != nil {
		if n2, err = c.w.b.Write([]byte(a.streamName)); err != nil {
			return
		}
		n += n2
		if err = c.w.b.WriteByte(0); err != nil {
			return
		}

		n++
	}
	return
}

func (a *action) recover(c *commitLog) (n int, err error) {
	if _, ok := c.streams[a.streamName]; ok {
		// Write flag and stream index
		n += 3
	} else {
		index := uint16(len(c.streams))
		c.streams[a.streamName] = streamLog{number: index, offset: a.offset, keepOffset: a.offset}
		n += 9
		// Write string, followed by a zero.
		n += len(a.streamName) + 1
	}
	return
}

func (a *action) read(r *reader) error {
	// Read flag byte
	b, err := r.b.ReadByte()
	if err != nil {
		return err
	}
	a.flags = actionFlags(b)
	var buffer [8]byte
	if (a.flags & actionWithName) == actionWithName {
		_, err := io.ReadFull(r.b, buffer[:8])
		if err != nil {
			return err
		}
		a.offset = binary.LittleEndian.Uint64(buffer[:8])
		str, err := r.b.ReadString(0)
		if err != nil {
			return err
		}
		// There is a 0 at the end of this string
		a.streamName = str[:len(str)-1]
		r.names = append(r.names, a.streamName)
	} else {
		_, err := io.ReadFull(r.b, buffer[:2])
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
	// Write information about the stream
	if n, err = a.a.write(c); err != nil {
		return
	}
	// Write size of data
	var buffer [8]byte
	binary.LittleEndian.PutUint32(buffer[:4], uint32(len(a.data)))
	if _, err = c.w.b.Write(buffer[:4]); err != nil {
		return
	}
	n += 4
	// FAT
	s := c.streams[a.a.streamName]
	if s.length == 0 {
		// First FAT entry
		s.firstFatIndex = uint16(len(c.fat))
		s.lastFatIndex = s.firstFatIndex
	} else {
		// Append to FAT entry
		l := uint16(len(c.fat))
		c.fat[s.lastFatIndex].next = l
		s.lastFatIndex = l
	}
	s.length += len(a.data)
	c.streams[a.a.streamName] = s
	var f fatEntry
	f.length = len(a.data)
	f.next = 0
	f.pos = c.size + n
	c.fat = append(c.fat, f)
	// Write data
	if _, err = c.w.b.Write(a.data); err != nil {
		return
	}
	n += len(a.data)
	return
}

func (a *appendAction) recover(c *commitLog) (n int, err error) {
	// Write information about the stream
	if n, err = a.a.recover(c); err != nil {
		return
	}
	// Write size of data
	n += 4
	// FAT
	s := c.streams[a.a.streamName]
	if s.length == 0 {
		// First FAT entry
		s.firstFatIndex = uint16(len(c.fat))
		s.lastFatIndex = s.firstFatIndex
	} else {
		// Append to FAT entry
		l := uint16(len(c.fat))
		c.fat[s.lastFatIndex].next = l
		s.lastFatIndex = l
	}
	s.length += len(a.data)
	c.streams[a.a.streamName] = s
	var f fatEntry
	f.length = len(a.data)
	f.next = 0
	f.pos = c.size + n
	c.fat = append(c.fat, f)
	// Write data
	n += len(a.data)
	return
}

func (a *appendAction) read(r *reader) (err error) {
	if err = a.a.read(r); err != nil {
		return
	}
	var buffer [8]byte
	if _, err = io.ReadFull(r.b, buffer[:4]); err != nil {
		return
	}
	l := int(binary.LittleEndian.Uint32(buffer[:]))
	a.data = make([]byte, l)
	if _, err = io.ReadFull(r.b, a.data); err != nil {
		return
	}
	return
}

func (a *pollardAction) write(c *commitLog) (n int, err error) {
	if n, err = a.a.write(c); err != nil {
		return
	}
	var buffer [8]byte
	binary.LittleEndian.PutUint64(buffer[:8], a.pollardPos)
	if _, err = c.w.b.Write(buffer[:8]); err != nil {
		return
	}
	n += 8
	s := c.streams[a.a.streamName]
	s.keepOffset = a.pollardPos
	c.streams[a.a.streamName] = s
	return
}

func (a *pollardAction) recover(c *commitLog) (n int, err error) {
	if n, err = a.a.recover(c); err != nil {
		return
	}
	// Write offset
	n += 8
	s := c.streams[a.a.streamName]
	s.keepOffset = a.pollardPos
	c.streams[a.a.streamName] = s
	return
}

func (a *pollardAction) read(r *reader) (err error) {
	if err = a.a.read(r); err != nil {
		return
	}
	var buffer [8]byte
	if _, err = io.ReadFull(r.b, buffer[:8]); err != nil {
		return
	}
	a.pollardPos = binary.LittleEndian.Uint64(buffer[:])
	return
}
