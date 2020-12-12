package queue

import (
	"encoding/binary"
	"os"
)

type logReader struct {
	f    *os.File
	dict []byte
}

type logReaderPiece struct {
	pos    uint32
	length uint32
}

type logReaderEntry struct {
	firstOffset uint64
	lastOffset  uint64
	pieces      []logReaderPiece
}

func newLogReader() *logReader {
	return &logReader{}
}

func (l *logReader) open(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	// Read the trailer
	if _, err := f.Seek(-16, os.SEEK_END); err != nil {
		return err
	}
	var buf [16]byte
	if _, err := f.Read(buf[:]); err != nil {
		return err
	}
	// Check the trailer
	if buf[8] != 42 || buf[9] != 0 || buf[10] != 42 || buf[11] != 0 {
		return os.ErrInvalid
	}
	if buf[12] != 42 || buf[13] != 0xff || buf[14] != 42 || buf[15] != 0xff {
		return os.ErrInvalid
	}
	size := int64(binary.LittleEndian.Uint64(buf[:]))
	// Read the dict.
	if _, err = f.Seek(-size-16, os.SEEK_CUR); err != nil {
		return err
	}
	l.dict = make([]byte, size)
	if _, err := f.Read(l.dict[:]); err != nil {
		l.dict = nil
		return err
	}
	l.f = f
	return nil
}

func (l *logReader) read(e logReaderEntry, offset uint64, data []byte) (err error) {
	if offset < e.firstOffset || offset+uint64(len(data)) > e.lastOffset {
		return os.ErrInvalid
	}
	piece := 0
	eoffset := e.firstOffset
	for ; offset >= eoffset+uint64(e.pieces[piece].length); piece++ {
		eoffset += uint64(e.pieces[piece].length)
	}
	toRead := len(data)
	done := 0
	for ; toRead > 0; piece++ {
		pos := e.pieces[piece].pos
		posOffset := uint32(offset - eoffset)
		readCount := int(e.pieces[piece].length - posOffset)
		if readCount > toRead {
			readCount = toRead
		}
		n2, err := l.f.ReadAt(data[done:done+readCount], int64(pos+posOffset))
		if err != nil {
			return err
		}
		toRead -= n2
		done += n2
		offset += uint64(readCount)
		eoffset += uint64(e.pieces[piece].length)
	}
	return nil
}

func (l *logReader) search(streamName string) (logReaderEntry, error) {
	// Search the matching position in the dict. Skip the flag byte
	pos := 1
	for {
		i := 0
		for ; i < len(streamName); i++ {
			if l.dict[pos+8+i] < streamName[i] {
				pos = int(binary.LittleEndian.Uint32(l.dict[pos+4:]))
				break
			} else if l.dict[pos+8+i] > streamName[i] {
				pos = int(binary.LittleEndian.Uint32(l.dict[pos:]))
				break
			}
		}
		if i == len(streamName) {
			if l.dict[pos+8+len(streamName)] != 0 {
				pos = int(binary.LittleEndian.Uint32(l.dict[pos:]))
			} else {
				// A match has been found. Skip past the stream name.
				pos += 8 + len(streamName) + 1
				break
			}
		}
		if pos == 0 {
			// Nothing has been found
			return logReaderEntry{}, os.ErrNotExist
		}
	}

	var e logReaderEntry
	e.firstOffset = binary.LittleEndian.Uint64(l.dict[pos:])
	e.lastOffset = binary.LittleEndian.Uint64(l.dict[pos+8:])
	count := binary.LittleEndian.Uint16(l.dict[pos+16:])
	pos += 8 + 8 + 2

	e.pieces = make([]logReaderPiece, int(count))
	for i := 0; i < int(count); i++ {
		e.pieces[i].pos = binary.LittleEndian.Uint32(l.dict[pos:])
		e.pieces[i].length = binary.LittleEndian.Uint32(l.dict[pos+4:])
		pos += 4 + 4
	}

	return e, nil
}
