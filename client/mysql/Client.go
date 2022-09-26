package mysql

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"
)

type Client struct {
	conn net.Conn
	rw   io.ReadWriter
	seq  uint8
}

type ReadWriter struct {
	io.Reader
	io.Writer
}

func ConnectTo(cfg Config) (*Client, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), 10*time.Second)
	if err != nil {
		return nil, err
	}

	tc := conn.(*net.TCPConn)
	tc.SetKeepAlive(true)
	tc.SetNoDelay(true)

	r := bufio.NewReaderSize(conn, 16*1024)

	return &Client{
		conn: conn,
		rw: &ReadWriter{
			Reader: r,
			Writer: conn,
		},
		seq: 0,
	}, nil
}

func (s *Client) Close() error {
	return s.conn.Close()
}

type Msg struct {
	Seq uint8
	Len uint64
}

func (s *Client) ReadMsg() (*Msg, error) {
	header := []byte{0, 0, 0, 0}
	if _, err := io.ReadFull(s.rw, header); err != nil {
		return nil, err
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length == 0 {
		s.seq++
		return &Msg{Seq: s.seq}, nil
	}

	if length == 1 {
		return nil, fmt.Errorf("invalid payload")
	}

	seq := uint8(header[3])
	if s.seq != seq {
		return nil, fmt.Errorf("invalid seq %d", seq)
	}

	p.seq++
	data := make([]byte, length)
	if _, err := io.ReadFull(p.r, data); err != nil {
		return nil, err
	} else {
		if length < MaxPayloadLength {
			return data, nil
		}
		var buf []byte
		buf, err = p.readPacket()
		if err != nil {
			return nil, err
		}
		if len(buf) == 0 {
			return data, nil
		} else {
			return append(data, buf...), nil
		}
	}
}
