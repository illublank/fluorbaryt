package mysql

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type PacketIO struct {
	r   *bufio.Reader
	w   io.Writer
	seq uint8
}

func (p *PacketIO) readPacket() ([]byte, error) {
	//to read header
	header := []byte{0, 0, 0, 0}
	if _, err := io.ReadFull(p.r, header); err != nil {
		return nil, err
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length == 0 {
		p.seq++
		return []byte{}, nil
	}

	if length == 1 {
		return nil, fmt.Errorf("invalid payload")
	}

	seq := uint8(header[3])
	if p.seq != seq {
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

func (p *PacketIO) writePacket(data []byte) error {
	length := len(data) - 4
	if length >= MaxPayloadLength {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff
		data[3] = p.seq

		if n, err := p.w.Write(data[:4+MaxPayloadLength]); err != nil {
			return fmt.Errorf("write find error")
		} else if n != 4+MaxPayloadLength {
			return fmt.Errorf("not equal max pay load length")
		} else {
			p.seq++
			length -= MaxPayloadLength
			data = data[MaxPayloadLength:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.seq

	if n, err := p.w.Write(data); err != nil {
		return errors.New("write find error")
	} else if n != len(data) {
		return errors.New("not equal length")
	} else {
		p.seq++
		return nil
	}
}

func (p *PacketIO) HandleError(data []byte) {
	pos := 1
	code := binary.LittleEndian.Uint16(data[pos:])
	pos += 2
	pos++
	state := string(data[pos : pos+5])
	pos += 5
	msg := string(data[pos:])
	fmt.Printf("code:%d, state:%s, msg:%s\n", code, state, msg)
}
