package queue

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/weistn/byos/queue/util"
)

// The Frontend is the API of the queueing system.
// It uses workers to carry out its jobs.
type Frontend struct {
	log        *commitLog
	logReaders []*logReader
	pathName   string
	// Fully qualified names of all finalized log files. Oldest is first and the commitLog is last.
	// Those opened are listed in logReaders (in the same order).
	logFiles []string
}

// StreamStat contains information about a stored stream.
type StreamStat struct {
	Size uint64
}

// NewFrontend returns a new frontend and (re-)opens the latest commit log.
func NewFrontend(pathName string) (f *Frontend, err error) {
	f = &Frontend{pathName: pathName}
	dir, err := os.Open(pathName)
	if err != nil {
		return nil, err
	}
	names, err := dir.Readdirnames(0)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	for _, n := range names {
		if strings.HasSuffix(n, ".log") && strings.HasPrefix(n, "commit_") {
			f.logFiles = append(f.logFiles, filepath.Join(pathName, n))
		}
	}

	if len(f.logFiles) == 0 {
		// Nothing there. Create a first log file
		f.log = newCommitLog()
		err := f.log.create(filepath.Join(pathName, "commit_0000.log"))
		if err != nil {
			f.log.close()
			return nil, err
		}
		f.logFiles = append(f.logFiles, "commit_0000.log")
	} else {
		// Try to recover the latest log file
		f.log = newCommitLog()
		err := f.log.recover(f.logFiles[0])
		if err == errIsFinalized {
			// The latest commit log is already finalized. Create a new one
			f.log.close()
			// Create a new log file
			n := f.logFiles[len(f.logFiles)-1]
			n = n[7 : len(n)-4]
			number, err := strconv.Atoi(n)
			if err != nil {
				panic("Illegal filename " + n)
			}
			if number >= 9999 {
				panic("TODO: Compaction")
			}
			n = "commit_" + fmt.Sprintf("%04d", number+1) + ".log"
			f.log = newCommitLog()
			err = f.log.create(n)
			if err != nil {
				f.log.close()
				return nil, err
			}
			f.logFiles = append(f.logFiles, n)
		} else {
			if err != nil {
				f.log.close()
				return nil, err
			}
		}
	}
	// Create log reader for all finalized log files (all except the latest one)
	for _, n := range f.logFiles[:len(f.logFiles)-1] {
		f.logReaders = append(f.logReaders, newLogReader(n))
	}
	return f, nil
}

// Close destructs the frontend and closes all files in use.
func (f *Frontend) Close() {
	if f.log == nil {
		return
	}
	f.log.close()
	f.log = nil
	for _, r := range f.logReaders {
		r.close()
	}
}

// Stat returns information about a stored stream or an error
// if the stream is unknown.
func (f *Frontend) Stat(streamName string) (s StreamStat, err error) {
	// Search in the commit log first
	logIndex := len(f.logReaders)
	span, err := f.log.streamRange(streamName)
	s.Size = span.To
	if err == nil || err == os.ErrNotExist {
		return s, err
	}
	// Search in all log readers, starting with the most recent one.
	for logIndex = logIndex - 1; logIndex >= 0; logIndex-- {
		r := f.logReaders[logIndex]
		if !r.isOpen() {
			if err = r.open(); err != nil {
				return s, err
			}
		}
		logentry, err := r.search(streamName)
		if err == nil {
			s.Size = logentry.span.To
			return s, nil
		} else if err != os.ErrNotExist {
			return s, err
		}
	}
	return s, os.ErrNotExist
}

// Read returns data from a stored stream.
// If the stream is too short to deliver all desired data, Read returns less data and no error.
func (f *Frontend) Read(streamName string, offset uint64, data []byte) (n uint64, err error) {
	dataspan := util.Span{From: offset, To: offset + uint64(len(data))}
	// Search in the commit log first
	found := false
	logIndex := len(f.logReaders)
	logspan, err := f.log.streamRange(streamName)
	if err == nil {
		found = true
		take := dataspan.Intersect(logspan)
		if take.IsEmpty() {
			// The desired data does not exist? -> done
			if logspan.To <= dataspan.From {
				return 0, nil
			}
			// Keep on searching in the older log files
		} else {
			// Some of the desired data does not exist? -> shrink data
			if logspan.To < dataspan.To {
				data = data[:int(logspan.To-dataspan.From)]
			}
			// Parts of the desired data is in the commit log ?
			_, err = f.log.readStream(streamName, take.From, data[dataspan.Size()-take.Size():])
			n = take.Size()
		}
	} else if err != os.ErrNotExist {
		return
	}

	// Search in all log readers, starting with the most recent one.
	for logIndex = logIndex - 1; n < uint64(len(data)) && logIndex >= 0; logIndex-- {
		r := f.logReaders[logIndex]
		if !r.isOpen() {
			if err = r.open(); err != nil {
				return 0, err
			}
		}
		logentry, err := r.search(streamName)
		if err == nil {
			found = true
			logspan = logentry.span
			take := dataspan.Intersect(logspan)
			if take.IsEmpty() {
				// The desired data does not exist? -> done
				if logspan.To <= dataspan.From {
					return 0, nil
				}
				// Keep on searching in the older log files
			} else {
				// Some of the desired data does not exist? -> shring data
				if n == 0 && logspan.To < dataspan.To {
					data = data[:int(logspan.To-dataspan.From)]
				}
				// Parts of the desired data is in the commit log ?
				err = r.read(logentry, take.From, data[dataspan.Size()-take.Size()-n:dataspan.Size()-n])
				n += take.Size()
			}
		} else if err != os.ErrNotExist {
			return 0, err
		}
	}

	if !found {
		// Nothing has been found at all
		return 0, os.ErrNotExist
	}
	if n != uint64(len(data)) {
		// Some of the first requested bytes could not be found. Perhaps due to pollard.
		return 0, errors.New("Head of data unavailable")
	}
	return n, nil
}

// Append writes data to a stream and syncs it to disk when required.
func (f *Frontend) Append(streamName string, data []byte, commit bool) error {
	var a appendAction
	a.a.flags = flagAppend
	a.a.streamName = streamName
	a.a.offset = 0
	stat, err := f.Stat(streamName)
	if err != nil && err != os.ErrPermission {
		return err
	} else if err == nil {
		a.a.offset = stat.Size
	}
	a.data = data
	if err = f.log.commit(&a); err != nil {
		return err
	}
	// TODO: Check for the log file being full
	return nil
}

// Pollard drops data from the beginning of the stream.
func (f *Frontend) Pollard(streamName string, offset uint64) error {
	var a pollardAction
	a.a.flags = flagPollard
	a.a.streamName = streamName
	stat, err := f.Stat(streamName)
	if err != nil {
		return err
	}
	a.a.offset = stat.Size
	a.pollardPos = offset
	if err = f.log.commit(&a); err != nil {
		return err
	}
	// TODO: Check for the log file being full
	return nil
}
