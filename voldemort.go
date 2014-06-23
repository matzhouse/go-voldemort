package voldemort

import (
	"bytes"
	proto "code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"errors"
	"fmt"
	vproto "github.com/matzhouse/go-voldemort-protobufs"
	"log"
	"net"
	"sync"
)

type VoldemortConn struct {
	c  *net.TCPConn
	mu sync.Mutex
}

func DialVoldemort(raddr *net.TCPAddr, proto string) (c *VoldemortConn, err error) {

	conn, err := net.DialTCP("tcp", nil, raddr)

	if err != nil {
		return nil, err
	}

	conn.Write([]byte("pb0"))

	result := bytes.NewBuffer(nil)
	var buf [2]byte // protocol response only returns a 2 byte response - ok or no

	_, err = conn.Read(buf[0:])

	if err != nil {
		return nil, err
	}

	result.Write(buf[0:2])

	// protocol response doesn't return a new line
	if string(result.Bytes()) != "ok" {
		if string(result.Bytes()) == "no" {
			err = errors.New(fmt.Sprintf("bad protocol set response : %s", result.Bytes()))
			return nil, err
		} else {
			err = errors.New(fmt.Sprintf("unknown protocol response : %s", result.Bytes()))
			return nil, err
		}
	}

	vc := new(VoldemortConn)
	vc.c = conn
	vc.c.SetNoDelay(true)

	return vc, nil

}

// Nice getter for a string key returning a string value
func Get(conn *VoldemortConn, store string, key string) (value string, err error) {

	Routingdecision := true
	rt := vproto.RequestType(0)

	vr := &vproto.VoldemortRequest{
		Store:       &store,
		Type:        &rt,
		ShouldRoute: &Routingdecision,
	}

	req := &vproto.GetRequest{Key: []byte(key)}

	vr.Get = req

	input, err := proto.Marshal(vr)

	if err != nil {
		return "", err
	}

	output, err := Do(conn, input)

	if err != nil {
		return "", err
	}

	resp := new(vproto.GetResponse)

	err = proto.Unmarshal(output, resp)

	if err != nil {
		log.Fatal("unmarshaling error: ", err)
	}

	vd := resp.GetVersioned()

	return string(vd[0].GetValue()), nil

}

func Close(conn *VoldemortConn) {
	conn.c.Close()
}

// creates the voldemort request <4 byte length, big endian encoded><message bytes> and receives the same
func Do(conn *VoldemortConn, input []byte) (output []byte, err error) {

	conn.mu.Lock()
	defer conn.mu.Unlock()

	buf := new(bytes.Buffer)

	var length uint32
	length = uint32(len(input))

	err = binary.Write(buf, binary.BigEndian, length)

	if err != nil {
		return nil, err
	}

	_, err = buf.Write(input)

	if err != nil {
		return nil, err
	}

	// send the command to voldemort
	conn.c.Write(buf.Bytes())

	// reset the buffer for the received content
	buf.Reset()

	// Get first 4 bytes from conn to get length of response from voldemort
	var buflen [4]byte
	n, err := conn.c.Read(buflen[0:4])
	if err != nil {
		log.Fatal(err)
	}

	// write correct length of response to buffer
	buf.Write(buflen[0:n])

	var respLen uint32
	err = binary.Read(buf, binary.BigEndian, &respLen)

	if err != nil {
		log.Fatal(err)
	}

	buf.Reset()
	var respBuf [512]byte

	n, err = conn.c.Read(respBuf[0:respLen])

	buf.Write(respBuf[0:n])

	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil

}
