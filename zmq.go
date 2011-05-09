package zmq

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"os"
	"sync"
)

const (
	flagMore = 1
)

const (
	SOCK_PAIR = iota
	SOCK_PUB
	SOCK_SUB
	SOCK_REQ
	SOCK_REP
	//SOCK_XREQ
	//SOCK_XREP
	SOCK_PULL
	SOCK_PUSH
)

type nilWAdder struct {
	net.Conn
}

func (b nilWAdder) addConn(wc io.WriteCloser) {}

type nilRAdder struct{}

func (b nilRAdder) addConn(fr *frameReader) {}

type reader interface {
	addConn(fr *frameReader)
	RecvMsg() (*MsgReader, os.Error)
	Close() os.Error
}

type bindWriter interface {
	io.WriteCloser
	addConn(wc io.WriteCloser)
}

type Context struct {
	endpoints map[string]net.Conn
	epLock sync.Mutex
}

func NewContext() (*Context, os.Error) {
	return &Context{endpoints: map[string]net.Conn{}}, nil
}

func (c *Context) registerEndpoint(name string) (net.Conn, os.Error) {
	c1, c2 := net.Pipe()
	c.epLock.Lock()
	defer c.epLock.Unlock()
	if _, ok := c.endpoints[name]; ok {
		return nil, os.NewError("endpoint already exists")
	}
	c.endpoints[name] = c2
	return c1, nil
}

func (c *Context) findEndpoint(name string) (net.Conn, os.Error) {
	c.epLock.Lock()
	defer c.epLock.Unlock()
	if conn, ok := c.endpoints[name]; ok {
		return conn, nil
	}
	return nil, os.NewError("endpoint does not exist")
}

// Similar to io.MultiWriter, but we have access to its internals and it has a Close method.
type multiWriter []io.WriteCloser

func newMultiWriter() *multiWriter {
	mw := make(multiWriter, 0, 5)
	return &mw
}

func (mw *multiWriter) Write(p []byte) (n int, err os.Error) {
	n = len(p)
	for _, w := range *mw {
		n2, err2 := w.Write(p)
		if err2 != nil {
			n = n2
			err = err2
		}
	}
	return
}

func (mw *multiWriter) Close() (err os.Error) {
	for _, w := range *mw {
		err2 := w.Close()
		if err2 != nil {
			err = err2
		}
	}
	return
}

func (mw *multiWriter) addConn(wc io.WriteCloser) {
	*mw = append(*mw, wc)
}

// A load-balanced WriteCloser
type lbWriter struct {
	w []io.WriteCloser
	c chan []byte
}

func newLbWriter() *lbWriter {
	c := make(chan []byte, 10)
	return &lbWriter{nil, c}
}

func (w *lbWriter) addConn(wc io.WriteCloser) {
	go writeListen(wc, w.c)
	// TODO: figure out a better way to keep track of writers
	w.w = append(w.w, wc)
}

func writeListen(w io.WriteCloser, c chan []byte) {
	for {
		b, ok := <-c
		if !ok {
			w.Close()
			break
		}
		if _, err := w.Write(b); err != nil {
			// pass it on to a different writer
			c <- b
			break
		}
	}
}

func (w *lbWriter) Write(b []byte) (int, os.Error) {
	w.c <- b
	// TODO: can we do better?
	return len(b), nil
}

func (w *lbWriter) Close() os.Error {
	close(w.c)
	return nil
}

type queuedReader struct {
	fr []*frameReader
	c  chan *MsgReader
}

func newQueuedReader() *queuedReader {
	c := make(chan *MsgReader, 10)
	return &queuedReader{nil, c}
}

func (r *queuedReader) addConn(fr *frameReader) {
	go readListen(fr, r.c)
	// TODO: figure out a better way to keep track of readers
	r.fr = append(r.fr, fr)
}

func readListen(fr *frameReader, c chan *MsgReader) {
	for {
		mr, err := fr.RecvMsg()
		if err != nil {
			break
		}
		c <- mr
	}
}

func (r *queuedReader) RecvMsg() (*MsgReader, os.Error) {
	mr := <-r.c
	return mr, nil
}

func (r *queuedReader) Close() os.Error {
	for _, r := range r.fr {
		r.Close()
	}
	return nil
}

type frameWriter struct {
	bindWriter
	buf *bufio.Writer
}

func newFrameWriter(wc bindWriter) *frameWriter {
	w := &frameWriter{wc, bufio.NewWriter(wc)}
	return w
}

func (fw *frameWriter) sendIdentity(id string) os.Error {
	var b []byte
	if id != "" {
		b = []byte(id)
	}
	_, err := fw.Write(b)
	return err
}

func (fw *frameWriter) Write(b []byte) (n int, err os.Error) {
	// + 1 for flags
	l := len(b) + 1
	if l < 255 {
		n, err = fw.buf.Write([]byte{byte(l)})
	} else {
		var length [9]byte
		length[0] = 255
		binary.BigEndian.PutUint64(length[1:], uint64(l))
		n, err = fw.buf.Write(length[:])
	}
	if err != nil {
		return
	}
	// flags; it’s impossible to have a slice with len > 2^64-1, so the MORE flag is always 0
	// All other flag bits are reserved.
	nn, err := fw.buf.Write([]byte{0})
	n += nn
	if err != nil {
		return
	}
	nn, err = fw.buf.Write(b)
	n += nn
	fw.buf.Flush()
	return
}

type frameReader struct {
	nilRAdder
	lock sync.Mutex
	rc   io.ReadCloser
	buf  *bufio.Reader
}

type MsgReader struct {
	length uint64 // length of the current frame
	more   bool   // whether there are more frames after this one
	buf    *bufio.Reader
	lock   *sync.Mutex
}

func newMsgReader(buf *bufio.Reader, lock *sync.Mutex) (*MsgReader, os.Error) {
	r := &MsgReader{buf: buf, lock: lock}
	err := r.readHeader()
	return r, err
}

func (r *MsgReader) readHeader() os.Error {
	var b [8]byte
	if _, err := r.buf.Read(b[:1]); err != nil {
		return err
	}
	if b[0] == 255 {
		if _, err := r.buf.Read(b[:]); err != nil {
			return err
		}
		r.length = binary.BigEndian.Uint64(b[:])
	} else {
		r.length = uint64(b[0])
	}
	r.length--
	flags, err := r.buf.ReadByte()
	if err != nil {
		return err
	}
	r.more = flags&flagMore != 0
	return nil
}

func (r *MsgReader) Read(b []byte) (n int, err os.Error) {
	for n < len(b) {
		l := uint64(len(b) - n)
		if r.length < l {
			l = r.length
		}
		nn, err := r.buf.Read(b[n : n+int(l)])
		n += nn
		r.length -= uint64(nn)
		if err != nil {
			return n, err
		}
		if r.length == 0 {
			if r.more {
				r.readHeader()
			} else {
				return n, os.EOF
			}
		}
	}
	return
}

func (r *MsgReader) Close() os.Error {
	r.lock.Unlock()
	return nil
}

func newFrameReader(rc io.ReadCloser) *frameReader {
	r := &frameReader{rc: rc, buf: bufio.NewReader(rc)}
	return r
}

func (fr *frameReader) RecvMsg() (*MsgReader, os.Error) {
	fr.lock.Lock()
	return newMsgReader(fr.buf, &fr.lock)
}

func (fr *frameReader) Close() os.Error {
	return fr.rc.Close()
}
