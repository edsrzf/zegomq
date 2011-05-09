package zmq

import (
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
)

type Socket struct {
	c        *Context
	identity string
	r        reader
	w        *frameWriter
}

func (c *Context) NewSocket(typ int, identity string) (*Socket, os.Error) {
	var r reader
	var w *frameWriter
	switch typ {
	case SOCK_PUB:
		mw := newMultiWriter()
		w = newFrameWriter(mw)
	case SOCK_SUB:
		r = newQueuedReader()
	case SOCK_PULL:
		r = newQueuedReader()
	case SOCK_PUSH:
		lbw := newLbWriter()
		w = newFrameWriter(lbw)
	case SOCK_PAIR, SOCK_REQ, SOCK_REP:
		fallthrough
	default:
		return nil, os.NewError("socket type unimplemented")
	}
	return &Socket{c, identity, r, w}, nil
}

func (s *Socket) RecvMsg() (*Msg, os.Error) {
	if s.r == nil {
		return nil, os.NewError("socket is not readable")
	}
	return s.r.RecvMsg()
}

func (s *Socket) Write(b []byte) (int, os.Error) {
	if s.w == nil {
		return 0, os.NewError("socket is not writable")
	}
	return s.w.Write(b)
}

func (s *Socket) Connect(addr string) os.Error {
	url := strings.Split(addr, "://", 2)
	if len(url) != 2 {
		return os.NewError("invalid address")
	}
	scheme, path := url[0], url[1]
	var conn net.Conn
	var err os.Error
	switch scheme {
	case "inproc":
		conn, err = s.c.findEndpoint(path)
	case "ipc":
		conn, err = net.Dial("unix", path)
	case "tcp":
		conn, err = net.Dial("tcp", path)
	default:
		err = os.NewError("unsupported URL scheme")
	}
	if err != nil {
		return err
	}
	// TODO: avoid making extra frameWriters and frameReaders
	fw := newFrameWriter(nilWAdder{conn})
	fw.sendIdentity(s.identity)

	fr := newFrameReader(conn)
	msg, err := fr.RecvMsg()
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, msg)
	msg.Close()

	if s.w != nil {
		s.w.addConn(conn)
	}
	if s.r != nil {
		s.r.addConn(fr)
	}
	return nil
}

func (s *Socket) Close() (err os.Error) {
	if s.w != nil {
		err = s.w.Close()
	}
	if s.r != nil {
		err2 := s.r.Close()
		if err == nil {
			err = err2
		}
	}
	return
}
