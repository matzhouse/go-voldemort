package voldemort

import (
	"bytes"
	proto "code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"errors"
	"fmt"
	vproto "github.com/matzhouse/go-voldemort-protobufs"
	"net"
	"sync"
	"time"
)

type VoldemortConn struct {
	c  *net.TCPConn
	mu sync.Mutex
}

func Dial(raddr *net.TCPAddr, proto string) (c *VoldemortConn, err error) {

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

func get(conn *VoldemortConn, store string, req *vproto.GetRequest) (resp *vproto.GetResponse, err error) {

	Routingdecision := true
	rt := vproto.RequestType(0)

	vr := &vproto.VoldemortRequest{
		Store:       &store,
		Type:        &rt,
		ShouldRoute: &Routingdecision,
	}

	vr.Get = req

	input, err := proto.Marshal(vr)
	if err != nil {
		return nil, err
	}

	output, err := Do(conn, input)

	if err != nil {
		return nil, err
	}

	// no record found
	if output == nil {
		return nil, nil
	}

	resp = new(vproto.GetResponse)

	err = proto.Unmarshal(output, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil

}

func put(conn *VoldemortConn, store string, req *vproto.PutRequest) (resp *vproto.PutResponse, err error) {

	Routingdecision := true
	rt := vproto.RequestType(2)

	vr := &vproto.VoldemortRequest{
		Store:       &store,
		Type:        &rt,
		ShouldRoute: &Routingdecision,
	}

	vr.Put = req

	input, err := proto.Marshal(vr)
	if err != nil {
		return nil, err
	}

	output, err := Do(conn, input)

	if err != nil {
		return nil, err
	}

	// no record found
	if output == nil {
		return nil, nil
	}

	resp = new(vproto.PutResponse)

	err = proto.Unmarshal(output, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil

}

func getversion(conn *VoldemortConn, store string, key string) (vc *vproto.VectorClock, err error) {

	req := &vproto.GetRequest{
		Key: []byte(key),
	}

	resp, err := get(conn, store, req)
	if err != nil {
		return nil, err
	}

	if resp == nil {
		// return new VectorClock
		vc := new(vproto.VectorClock)
		now := int64(time.Now().Unix())
		vc.Timestamp = &now

		id := int32(0)
		v := int64(1) // 1-32767

		ce := &vproto.ClockEntry{
			NodeId:  &id,
			Version: &v,
		}

		vc.Entries = append(vc.Entries, ce)

		return vc, nil
	} else {
		now := int64(time.Now().Unix())

		cv := resp.Versioned[0].Version.Entries[0].Version
		ncv := int64(1) + *cv

		resp.Versioned[0].Version.Entries[0].Version = &ncv

		resp.Versioned[0].Version.Timestamp = &now

		return resp.Versioned[0].Version, nil
	}

}

// Nice getter for a string key returning a string value
func Get(conn *VoldemortConn, store string, key string) (value string, err error) {

	req := &vproto.GetRequest{
		Key: []byte(key),
	}

	resp, err := get(conn, store, req)

	if err != nil {
		return "", err
	}

	// null response
	if resp == nil {
		return "", nil
	}

	vd := resp.GetVersioned()

	return string(vd[0].GetValue()), nil // simple get newest

}

func Put(conn *VoldemortConn, store string, key string, value string) (b bool, err error) {

	vc, err := getversion(conn, store, key)

	if err != nil {
		return false, err
	}

	req := &vproto.PutRequest{
		Key: []byte(key),
		Versioned: &vproto.Versioned{
			Value:   []byte(value),
			Version: vc,
		},
	}

	resp, err := put(conn, store, req)

	if err != nil {
		return false, err
	}

	if resp != nil {
		if resp.Error != nil {
			return false, errors.New(*resp.Error.ErrorMessage)
		}
	}

	return true, nil

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
		return nil, err
	}

	// write correct length of response to buffer
	buf.Write(buflen[0:n])

	var respLen uint32
	err = binary.Read(buf, binary.BigEndian, &respLen)

	if respLen == 0 {
		return nil, nil
	}

	if err != nil {
		return nil, err
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
