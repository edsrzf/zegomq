// Package zmq implements the ØMQ socket types and wire protocol.
// For more information, see http://zeromq.org.
package zmq

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
)

const (
	flagMore = 1
)

type nilWAdder struct {
	net.Conn
}

func (b nilWAdder) addConn(wc io.WriteCloser) {}

type nilRAdder struct{}

func (b nilRAdder) addConn(fr *frameReader) {}

type readerPool interface {
	addConn(fr *frameReader)
	RecvMsg() (*Msg, error)
	Close() error
}

type writerPool interface {
	io.WriteCloser
	addConn(wc io.WriteCloser)
}

// A Context is a container for sockets and manages in-process communications.
// Contexts are thread safe and may be shared between goroutines without
// synchronization.
type Context struct {
	endpoints map[string]net.Conn
	epLock    sync.Mutex
}

// NewContext returns a new context.
func NewContext() *Context {
	return &Context{endpoints: map[string]net.Conn{}}
}

func (c *Context) registerEndpoint(name string) (net.Conn, error) {
	c1, c2 := net.Pipe()
	c.epLock.Lock()
	defer c.epLock.Unlock()
	if _, ok := c.endpoints[name]; ok {
		return nil, errors.New("endpoint already exists")
	}
	c.endpoints[name] = c2
	return c1, nil
}

func (c *Context) findEndpoint(name string) (net.Conn, error) {
	c.epLock.Lock()
	defer c.epLock.Unlock()
	if conn, ok := c.endpoints[name]; ok {
		return conn, nil
	}
	return nil, errors.New("endpoint does not exist")
}

// Similar to io.MultiWriter, but we have access to its internals and it has a Close method.
type multiWriter struct {
	wc   []io.WriteCloser
	lock sync.Mutex
}

func newMultiWriter() *multiWriter {
	wc := make([]io.WriteCloser, 0, 5)
	return &multiWriter{wc: wc}
}

func (mw *multiWriter) Write(b []byte) (n int, err error) {
	n = len(b)
	mw.lock.Lock()
	for _, w := range mw.wc {
		n2, err2 := w.Write(b)
		if err2 != nil {
			n = n2
			err = err2
		}
	}
	mw.lock.Unlock()
	return
}

func (mw *multiWriter) Close() (err error) {
	mw.lock.Lock()
	for _, w := range mw.wc {
		err2 := w.Close()
		if err2 != nil {
			err = err2
		}
	}
	mw.lock.Unlock()
	return
}

func (mw *multiWriter) addConn(wc io.WriteCloser) {
	mw.lock.Lock()
	mw.wc = append(mw.wc, wc)
	mw.lock.Unlock()
}

// a load-balanced WriteCloser
type lbWriter struct {
	c chan []byte
}

func newLbWriter() *lbWriter {
	c := make(chan []byte, 10)
	return &lbWriter{c}
}

func (w *lbWriter) addConn(wc io.WriteCloser) {
	go writeListen(wc, w.c)
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

func (w *lbWriter) Write(b []byte) (int, error) {
	w.c <- b
	// TODO: can we do better?
	return len(b), nil
}

func (w *lbWriter) Close() error {
	close(w.c)
	return nil
}

type queuedReader struct {
	fr   []*frameReader
	lock sync.Mutex
	c    chan *Msg
}

func newQueuedReader() *queuedReader {
	c := make(chan *Msg, 10)
	return &queuedReader{c: c}
}

func (r *queuedReader) addConn(fr *frameReader) {
	go readListen(fr, r.c)
	// TODO: figure out a better way to keep track of readers
	r.lock.Lock()
	r.fr = append(r.fr, fr)
	r.lock.Unlock()
}

func readListen(fr *frameReader, c chan *Msg) {
	for {
		mr, err := fr.RecvMsg()
		if err != nil {
			break
		}
		c <- mr
	}
}

func (r *queuedReader) RecvMsg() (*Msg, error) {
	mr := <-r.c
	return mr, nil
}

func (r *queuedReader) Close() error {
	r.lock.Lock()
	for _, r := range r.fr {
		r.Close()
	}
	r.lock.Unlock()
	return nil
}

type frameWriter struct {
	writerPool
	buf *bufio.Writer
}

func newFrameWriter(wc writerPool) *frameWriter {
	w := &frameWriter{wc, bufio.NewWriter(wc)}
	return w
}

func (fw *frameWriter) sendIdentity(id string) error {
	var b []byte
	if id != "" {
		b = []byte(id)
	}
	_, err := fw.write(b, 0)
	return err
}

func (fw *frameWriter) write(b []byte, flags byte) (n int, err error) {
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
	err = fw.buf.WriteByte(flags)
	if err != nil {
		return
	}
	n++
	nn, err := fw.buf.Write(b)
	n += nn
	fw.buf.Flush()
	return
}

func (fw *frameWriter) ReadFrom(r io.Reader) (n int64, err error) {
	// use two buffers the same size as in io.Copy
	// We need two because we need to know before we send the first buffer
	// whether or not the second one will complete in order to set the flags.
	buf := make([]byte, 2*32*1024)
	buf1 := buf[:32*1024]
	buf2 := buf[32*1024 : 2*32*1024]

	flags := byte(flagMore)
	nn, err := r.Read(buf1)
	if err != nil && err != io.EOF {
		return n, err
	}
	for flags != 0 {
		nnn, err := r.Read(buf2)
		if err != nil {
			if err == io.EOF {
				flags = 0
			} else {
				return n, err
			}
		}
		n += int64(nn)
		fw.write(buf[:nn], flags)
		nn = nnn
		buf1, buf2 = buf2, buf1
	}
	return
}

type frameReader struct {
	nilRAdder
	lock sync.Mutex
	rc   io.ReadCloser
	buf  *bufio.Reader
}

func newFrameReader(rc io.ReadCloser) *frameReader {
	r := &frameReader{rc: rc, buf: bufio.NewReader(rc)}
	return r
}

func (fr *frameReader) RecvMsg() (*Msg, error) {
	fr.lock.Lock()
	return newMsg(fr.buf, &fr.lock)
}

func (fr *frameReader) Close() error {
	return fr.rc.Close()
}
