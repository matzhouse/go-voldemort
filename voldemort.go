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

type VoldemortClient struct {
	Protocol   string
	ServerAddr string
	ServerPort int
	Store      string
	Conn       *net.TCPConn
	Req        *vproto.VoldemortRequest
	mu         sync.Mutex
}

func (vc *VoldemortClient) NewConnection() (err error) {

	fulladdress := fmt.Sprintf("%s:%d", vc.ServerAddr, vc.ServerPort)

	addr, err := net.ResolveTCPAddr("tcp", fulladdress)

	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, addr)

	if err != nil {
		return err
	}

	vc.Conn = conn

	return nil

}

func (vc *VoldemortClient) Get(key string) (value string) {

	vc.Req = &vproto.VoldemortRequest{}

	var Routingdecision bool
	var Store string
	var ReqType vproto.RequestType

	Routingdecision = true
	ReqType = 0
	Store = vc.Store

	vc.Req.Store = &Store
	vc.Req.Type = &ReqType
	vc.Req.ShouldRoute = &Routingdecision

	vc.Req.Get = &vproto.GetRequest{Key: []byte(key)}

	vout, err := proto.Marshal(vc.Req)

	if err != nil {
		log.Fatal(err)
	}

	resp := vc.DoRequest(string(vout))

	vd := resp.GetVersioned()

	return string(vd[0].GetValue())

}

func (vc *VoldemortClient) DoRequest(request string) (response *vproto.GetResponse) {

	c := vc.Conn

	buf := new(bytes.Buffer)

	var length uint32
	length = uint32(len([]byte(request)))

	err := binary.Write(buf, binary.BigEndian, length)
	if err != nil {
		log.Fatal("error with binary encoding of length of request : ", err)
	}
	buf.WriteString(request)

	if err != nil {
		log.Fatal("problem writing request to buffer: ", err)
	}

	c.Write(buf.Bytes())

	out, err := vc.readResponse(c)
	if err != nil {
		log.Fatal("problem reading: ", err)
	}

	resp := new(vproto.GetResponse)

	err = proto.Unmarshal(out, resp)
	if err != nil {
		log.Fatal("unmarshaling error: ", err)
	}

	return resp

}

func (vc *VoldemortClient) SetProtocol() (b bool, err error) {

	log.Println("setting protocol : ", vc.Protocol)

	vc.Conn.Write([]byte(vc.Protocol))

	result := bytes.NewBuffer(nil)
	var buf [2]byte // protocol response only returns a 2 byte response - ok or no

	_, err = vc.Conn.Read(buf[0:])

	if err != nil {
		return false, err
	}

	result.Write(buf[0:2])

	// protocol response doesn't return a new line
	if string(result.Bytes()) == "ok" {
		return true, nil
	} else if string(result.Bytes()) == "no" {
		log.Println("bad protocol set response : ", result.Bytes())
		errString := fmt.Sprintf("bad protocol set response : %s", result.Bytes())
		err = errors.New(errString)
		return false, err
	}

	return false, nil

}

func (vc *VoldemortClient) readResponse(conn net.Conn) ([]byte, error) {

	var buflen [4]byte
	result := bytes.NewBuffer(nil)

	n, err := conn.Read(buflen[0:4])

	if err != nil {
		log.Fatal(err)
	}

	result.Write(buflen[0:n])

	var intLen uint32

	err = binary.Read(result, binary.BigEndian, &intLen)

	if err != nil {
		log.Fatal(err)
	}

	result = bytes.NewBuffer(nil)
	var buf [512]byte

	n, err = conn.Read(buf[0:intLen])

	result.Write(buf[0:n])

	if err != nil {
		return nil, err
	}

	return result.Bytes(), nil

}
