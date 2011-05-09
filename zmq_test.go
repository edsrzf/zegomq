package zmq

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
	"testing"
)

type msgConn struct {
	count   uint64
	lengths []uint64
	// contains the next header, if any
	buf []byte
}

func (c *msgConn) Read(b []byte) (int, os.Error) {
	n := len(b)
	if uint64(n) > c.count {
		if len(c.lengths) == 0 {
			return 0, os.NewError("read too far")
		}
		n -= int(c.count)
		c.count = c.lengths[0]
		c.lengths = c.lengths[1:]
		if c.count > 254 {
			c.buf = make([]byte, 10)
			c.buf[0] = 255
			binary.BigEndian.PutUint64(c.buf[1:], c.count)
			n -= 10
		} else {
			c.buf = make([]byte, 2)
			c.buf[0] = byte(c.count)
			n -= 2
		}
		if len(c.lengths) > 0 {
			c.buf[len(c.buf)-1] = flagMore
		}
	}
	m := copy(b, c.buf)
	c.buf = c.buf[m:]
	n -= m
	c.count -= uint64(n)
	return len(b), nil
}

type msgTest struct {
	lengths []uint64
	initialLength int
}

var msgTests = []*msgTest{
	&msgTest{[]uint64{1}, 0},
	&msgTest{[]uint64{254}, 253},
	&msgTest{[]uint64{255}, 254},
	&msgTest{[]uint64{1000}, 999},
	&msgTest{[]uint64{1, 1}, -1},
	&msgTest{[]uint64{0x7FFFFFFF}, 0x7FFFFFFE},
	&msgTest{[]uint64{0x80000000}, 0x7FFFFFFF},
	&msgTest{[]uint64{0x80000001}, -1},
}

func makeTestMsg(t *msgTest) *Msg {
	buf := bufio.NewReader(&msgConn{lengths: t.lengths})
	var lock sync.Mutex
	lock.Lock()
	msg, err := newMsg(buf, &lock)
	if err != nil {
		panic(err.String())
	}
	return msg
}

func TestMsgLen(t *testing.T) {
	for i, test := range msgTests {
		msg := makeTestMsg(test)
		if msg.Len() != test.initialLength {
			t.Errorf("#%d: got %d wanted %d", i, msg.Len(), test.initialLength)
		}
	}
}
