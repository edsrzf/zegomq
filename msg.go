package zmq

import (
	"bufio"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

// A Msg represents a ØMQ message. Only one Msg from an endpoint can be
// active at a time. The caller must Close the Msg when finished with it.
type Msg struct {
	length uint64 // length of the current frame
	more   bool   // whether there are more frames after this one
	buf    *bufio.Reader
	lock   *sync.Mutex
}

func newMsg(buf *bufio.Reader, lock *sync.Mutex) (*Msg, os.Error) {
	m := &Msg{buf: buf, lock: lock}
	err := m.readHeader()
	return m, err
}

func (m *Msg) readHeader() os.Error {
	var b [8]byte
	if _, err := m.buf.Read(b[:1]); err != nil {
		return err
	}
	if b[0] == 255 {
		if _, err := m.buf.Read(b[:]); err != nil {
			return err
		}
		m.length = binary.BigEndian.Uint64(b[:])
	} else {
		m.length = uint64(b[0])
	}
	m.length--
	flags, err := m.buf.ReadByte()
	if err != nil {
		return err
	}
	m.more = flags&flagMore != 0
	return nil
}

func (m *Msg) Read(b []byte) (n int, err os.Error) {
	if m.length == 0 && !m.more {
		return 0, os.EOF
	}
	for n < len(b) {
		l := uint64(len(b) - n)
		if m.length < l {
			l = m.length
		}
		nn, err := m.buf.Read(b[n : n+int(l)])
		n += nn
		m.length -= uint64(nn)
		if err != nil {
			return n, err
		}
		if m.length == 0 {
			if m.more {
				m.readHeader()
			} else {
				return n, os.EOF
			}
		}
	}
	return
}

// ReadAll attempts to read the entire Msg. When it is finished, the Msg is
// fully consumed and closed, even if there was an error.
func (m *Msg) ReadAll() (buf []byte, err os.Error) {
	defer m.Close()
	for m.length > 0 || m.more {
		// we're making some assumptions about the runtime's implementation of
		// slices, but they're correct assumptions for the two runtimes so far.
		if m.length > uint64(maxInt) {
			err = os.NewError("Msg will not fit in a slice")
			return
		}
		old := buf
		n := len(buf)
		buf = make([]byte, n+int(m.length))
		copy(buf, old)
		if _, err = m.Read(buf[n:]); err != nil {
			return
		}
	}
	return
}

// discard reads the rest of the data off the wire.
func (m *Msg) discard() {
	io.Copy(ioutil.Discard, m)
}

const maxInt = int(^uint(0) / 2)

// Len returns the message's length. If the length is unknown or too large for an int to
// hold, Len returns -1.
func (m *Msg) Len() int {
	if m.more || m.length > uint64(maxInt) {
		return -1
	}
	return int(m.length)
}

// Close unlocks the associated Socket so that another message can be read,
// discarding any unread data.
func (m *Msg) Close() os.Error {
	m.discard()
	m.lock.Unlock()
	return nil
}
