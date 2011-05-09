package zmq

import (
	"io"
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

// NewSocket creates a new Socket within the Context c.
// Initially the Socket is not associated with any endpoints.
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

// RecvMsg returns the next message from the Socket.
// If there is no Msg available, this call will block until there is one.
// If the next Msg comes from an endpoint with an already active Msg,
// this call will block until the existing Msg is closed.
func (s *Socket) RecvMsg() (*Msg, os.Error) {
	if s.r == nil {
		return nil, os.NewError("socket is not readable")
	}
	return s.r.RecvMsg()
}

// Write sends a single Msg to the Socket.
func (s *Socket) Write(b []byte) (int, os.Error) {
	if s.w == nil {
		return 0, os.NewError("socket is not writable")
	}
	return s.w.write(b, 0)
}

// ReadFrom reads data from r until EOF and sends it as a single Msg.
func (s *Socket) ReadFrom(r io.Reader) (n int64, err os.Error) {
	if s.w == nil {
		return 0, os.NewError("socket is not writable")
	}
	return s.w.ReadFrom(r)
}

// Connect adds a new endpoint to the Socket.
// The endpoint is a string of the form "transport://address".
// The following transports are available:
//	inproc, local in-process, synchronized communication
//	ipc, local inter-process communication
//	tcp, unicast transport using TCP
func (s *Socket) Connect(endpoint string) os.Error {
	url := strings.Split(endpoint, "://", 2)
	if len(url) != 2 {
		return os.NewError("invalid address")
	}
	transport, addr := url[0], url[1]
	var conn net.Conn
	var err os.Error
	switch transport {
	case "inproc":
		conn, err = s.c.findEndpoint(addr)
	case "ipc":
		conn, err = net.Dial("unix", addr)
	case "tcp":
		conn, err = net.Dial("tcp", addr)
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
	msg.Close()

	if s.w != nil {
		s.w.addConn(conn)
	}
	if s.r != nil {
		s.r.addConn(fr)
	}
	return nil
}

// Close closes all endpoints. Any outstanding Msgs from this socket become
// invalid.
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
