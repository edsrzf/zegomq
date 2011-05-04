package zmq

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"os"
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

type binder interface {
	bind(rwc io.ReadWriteCloser)
}

type nilBinder struct {}

func (b nilBinder) bind(rwc io.ReadWriteCloser) {}

type frameReader interface {
	binder
	ReadFrame([]byte) ([]byte, os.Error)
}

type bindWriter interface {
	binder
	io.Writer
}

type Socket struct {
	identity string
	r frameReader
	w bindWriter
}

func NewSocket(typ int, identity string) (*Socket, os.Error) {
	var r frameReader
	var w bindWriter
	switch typ {
	case SOCK_PAIR:
	case SOCK_PUB:
		mw := make(multiWriter, 0, 5)
		w = newFrameWriter(mw)
	case SOCK_SUB:
	case SOCK_REQ:
	case SOCK_REP:
	case SOCK_PULL:
		r = newQueuedReader()
	case SOCK_PUSH:
		w = newLbWriter()
	default:
	}
	return &Socket{identity, r, w}, nil
}

func (s *Socket) ReadFrame(body []byte) ([]byte, os.Error) {
	if s.r == nil {
		return body, os.NewError("socket is not readable")
	}
	return s.r.ReadFrame(body)
}

func (s *Socket) Write(b []byte) (int, os.Error) {
	if s.w == nil {
		return 0, os.NewError("socket is not writable")
	}
	return s.w.Write(b)
}

func (s *Socket) Bind(network, addr string) os.Error {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return err
	}
	// TODO: avoid making extra frameWriters and frameReaders
	if s.w != nil {
		fw := newFrameWriter(conn)
		// TODO: write identity properly
		b := make([]byte, len(s.identity)+2)
		b[0] = byte(len(s.identity))
		b[1] = 0
		copy(b[2:], s.identity)
		fw.Write(b)
		fc := newFrameReader(conn)
		fc.ReadFrame(nil)
		s.w.bind(conn)
	}
	if s.r != nil {
		fw := newFrameWriter(conn)
		fw.Write(nil)
		fc := newFrameReader(conn)
		fc.ReadFrame(nil)
		s.r.bind(conn)
	}
	return nil
}

// Similar to io.MultiWriter, but we have access to its internals and it has a Close method.
type multiWriter []io.WriteCloser

func (mw multiWriter) Write(p []byte) (n int, err os.Error) {
	n = len(p)
	for _, w := range mw {
		n2, err2 := w.Write(p)
		if err2 != nil {
			n = n2
			err = err2
		}
	}
	return
}

func (mw multiWriter) Close() (err os.Error) {
	for _, w := range mw {
		err2 := w.Close()
		if err2 != nil {
			err = err2
		}
	}
	return
}

func (mw multiWriter) bind(rwc io.ReadWriteCloser) {
	mw = append(mw, rwc)
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

func (w *lbWriter) bind(rwc io.ReadWriteCloser) {
	go writeListen(rwc, w.c)
	// TODO: figure out a better way to keep track of writers
	w.w = append(w.w, rwc)
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
	// TODO: change to ReadCloser
	r []io.ReadWriteCloser
	c chan []byte
}

func newQueuedReader() *queuedReader {
	c := make(chan []byte, 10)
	return &queuedReader{nil, c}
}

func (r *queuedReader) bind(rwc io.ReadWriteCloser) {
	fr := newFrameReader(rwc)
	go readListen(fr, r.c)
	// TODO: figure out a better way to keep track of readers
	r.r = append(r.r, rwc)
}

func readListen(fr *defFrameReader, c chan []byte) {
	for {
		// TODO: stop the allocation!
		b, err := fr.ReadFrame(nil)
		if err != nil {
			break
		}
		c <- b
	}
}

func (r *queuedReader) ReadFrame(body []byte) ([]byte, os.Error) {
	b := <-r.c
	return b, nil
}

func (r *queuedReader) Close() os.Error {
	for _, r := range r.r {
		r.Close()
	}
	return nil
}

type frameWriter struct {
	nilBinder
	wc io.WriteCloser
	buf *bufio.Writer
}

type defFrameReader struct {
	rc io.ReadCloser
	buf *bufio.Reader
}

func newFrameWriter(wc io.WriteCloser) *frameWriter {
	w := &frameWriter{wc: wc, buf: bufio.NewWriter(wc)}
	return w
}

func newFrameReader(rc io.ReadCloser) *defFrameReader {
	r := &defFrameReader{rc, bufio.NewReader(rc)}
	return r
}

func (fc *frameWriter) Write(b []byte) (n int, err os.Error) {
	// + 1 for flags
	l := len(b) + 1
	if l < 255 {
		n, err = fc.buf.Write([]byte{byte(l)})
	} else {
		var length [9]byte
		length[0] = 255
		binary.BigEndian.PutUint64(length[1:], uint64(l))
		n, err = fc.buf.Write(length[:])
	}
	if err != nil {
		return
	}
	// flags; it’s impossible to have a slice with len > 2^64-1, so the MORE flag is always 0
	// All other flag bits are reserved.
	nn, err := fc.buf.Write([]byte{0})
	n += nn
	if err != nil {
		return
	}
	nn, err = fc.buf.Write(b)
	n += nn
	fc.buf.Flush()
	return
}

func (fc *frameWriter) Close() os.Error {
	return fc.wc.Close()
}

func (fc *defFrameReader) ReadFrame(body []byte) ([]byte, os.Error) {
	var length uint64
	var b [8]byte
	if _, err := fc.buf.Read(b[:1]); err != nil {
		return nil, err
	}
	if b[0] == 255 {
		if _, err := fc.buf.Read(b[:]); err != nil {
			return nil, err
		}
		length = binary.BigEndian.Uint64(b[:])
	} else {
		length = uint64(b[0])
	}
	if uint64(len(body)) < length {
		body = make([]byte, length)
	}
	if _, err := fc.buf.Read(body); err != nil {
		return nil, err
	}
	if body[0]&1 != 0 {
		panic("can't handle MORE")
	}
	return body[1:], nil
}

func (fc *defFrameReader) Close() os.Error {
	return fc.rc.Close()
}
