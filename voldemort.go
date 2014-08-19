package voldemort

import (
	"bytes"
	proto "code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"fmt"
	vproto "github.com/matzhouse/go-voldemort-protobufs"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
)

// The VoldemortConn struct is used to hold all the data for the Voldemort cluster you need to query
type VoldemortConn struct {
	// String address of server this conn is using
	s string

	// Voldemort protocol
	proto string

	// TCP connection used to talk to Voldemort instance
	c *net.TCPConn

	// Information about the Voldemort cluster (received from the cluster once you connect to it)
	cl *Cluster

	// A simple mutex to make the conn thread safe
	mu sync.Mutex

	GetMetrics metrics.Timer
	PutMetrics metrics.Timer
}

// The cluster struct holds all the information taken from the Voldemort cluster when it's first connected
type Cluster struct {
	// Name of Voldemort cluster
	Name string `xml:"name"`

	// Array of Voldemort servers
	Servers []Server `xml:"server"`
}

// The server struct holds all the information about a Voldermort server
type Server struct {
	Id         int    `xml:"id"`
	Host       string `xml:"host"`
	Http       int    `xml:"http-port"`
	Socket     int    `xml:"socket-port"`
	Partitions string `xml:"partitions"`
	State      bool
}

// Returns a VoldemortConn that can be used to talk to a Voldemort cluster
func Dial(raddr *net.TCPAddr, proto string, reg metrics.Registry) (c *VoldemortConn, err error) {

	conn, err := net.DialTCP("tcp", nil, raddr)
	conn.SetNoDelay(true)
	//conn, err := net.Dial(network, address)

	if err != nil {
		return nil, err
	}

	err = setProtocol(conn, proto)

	if err != nil {
		return nil, err
	}

	vc := &VoldemortConn{
		s:          raddr.String(),
		proto:      proto,
		c:          conn,
		GetMetrics: metrics.NewTimer(),
		PutMetrics: metrics.NewTimer(),
	}

	reg.Register(raddr.String(), vc.GetMetrics)
	reg.Register(raddr.String(), vc.PutMetrics)

	/*
		vc := new(VoldemortConn)

		vc.s = raddr.String()
		vc.proto = proto
	*/

	cl, err := vc.bootstrap()

	if err != nil {
		log.Println(err)
		log.Println("warning - this client will only be able to talk to the current node")
	}

	vc.cl = cl

	return vc, nil

}

func setProtocol(conn *net.TCPConn, proto string) (err error) {

	_, err = conn.Write([]byte(proto))

	result := bytes.NewBuffer(nil)
	var buf [2]byte // protocol response only returns a 2 byte response - ok or no

	_, err = conn.Read(buf[0:])

	if err != nil {
		return err
	}

	result.Write(buf[0:2])

	// protocol response doesn't return a new line
	if string(result.Bytes()) != "ok" {
		if string(result.Bytes()) == "no" {
			err = errors.New(fmt.Sprintf("bad protocol set response : %s", result.Bytes()))
			return err
		} else {
			err = errors.New(fmt.Sprintf("unknown protocol response : %s", result.Bytes()))
			return err
		}
	}

	return nil

}

func (vc *VoldemortConn) Metrics() (reg metrics.Registry) {
	return vc.Metrics()
}

func (vc *VoldemortConn) Server() (name string) {
	return vc.s
}

func (vc *VoldemortConn) bootstrap() (n *Cluster, err error) {

	n, err = vc.getclusterdata()

	if err != nil {
		return nil, err
	}

	// update the state
	for k, v := range n.Servers {
		saddr := fmt.Sprintf("%s:%d", v.Host, v.Socket)
		if saddr == vc.c.RemoteAddr().String() {
			vc.cl.Servers[k].State = true
		}
	}

	return n, nil

}

func (conn *VoldemortConn) get(store string, req *vproto.GetRequest, shouldroute bool) (resp *vproto.GetResponse, err error) {

	rt := vproto.RequestType(0)

	vr := &vproto.VoldemortRequest{
		Store:       &store,
		Type:        &rt,
		ShouldRoute: &shouldroute,
	}

	vr.Get = req

	input, err := proto.Marshal(vr)
	if err != nil {
		return nil, err
	}

	output, err := conn.Do(input)

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

	if resp.Error != nil {
		err = errors.New(*resp.Error.ErrorMessage)
		return nil, err
	}

	return resp, nil

}

func (conn *VoldemortConn) put(store string, req *vproto.PutRequest) (resp *vproto.PutResponse, err error) {

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

	output, err := conn.Do(input)

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

func (conn *VoldemortConn) getclusterdata() (cl *Cluster, err error) {

	req := &vproto.GetRequest{
		Key: []byte("cluster.xml"),
	}

	resp, err := conn.get("metadata", req, false)

	if err != nil {
		return nil, err
	}

	if resp == nil {
		err = errors.New("metadata not available")
		return nil, err
	}

	cl = &Cluster{}

	err = xml.Unmarshal([]byte(resp.GetVersioned()[0].GetValue()), cl)

	if err != nil {
		return nil, err
	}

	conn.cl = cl

	return cl, nil

}

func (conn *VoldemortConn) getversion(store string, key string) (vc *vproto.VectorClock, err error) {

	req := &vproto.GetRequest{
		Key: []byte(key),
	}

	resp, err := conn.get(store, req, true)
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
func (conn *VoldemortConn) Get(store string, key string) (value string, err error) {

	if store == "" || key == "" {
		err = errors.New("store and key cannot be empty")
		return "", err
	}

	req := &vproto.GetRequest{
		Key: []byte(key),
	}

	// update a metrics registry
	start := time.Now()
	resp, err := conn.get(store, req, true)
	elapsed := time.Since(start)

	conn.GetMetrics.Update(elapsed)

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

func (conn *VoldemortConn) Put(store string, key string, value string) (b bool, err error) {

	if store == "" || key == "" {
		err = errors.New("store and key cannot be empty")
		return false, err
	}

	vc, err := conn.getversion(store, key)

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

	resp, err := conn.put(store, req)

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

func (conn *VoldemortConn) Close() {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	conn.c.Close()
}

// creates the voldemort request <4 byte length, big endian encoded><message bytes> and receives the same
func (conn *VoldemortConn) Do(input []byte) (output []byte, err error) {

	conn.mu.Lock()
	defer conn.mu.Unlock()

	noconn := errors.New("tcp connection not available")

	buf := new(bytes.Buffer)

	var length uint32
	length = uint32(len(input))

	err = binary.Write(buf, binary.BigEndian, length)

	_, err = buf.Write(input)

	if err != nil {
		return nil, err
	}

	// send the command to voldemort
	_, err = conn.c.Write(buf.Bytes())

	if err != nil {
		if err == io.EOF {
			return nil, noconn
		} else {
			return nil, err
		}
	}

	// reset the buffer for the received content
	buf.Reset()

	// Get first 4 bytes from conn to get length of response from voldemort
	var buflen [4]byte
	n, err := conn.c.Read(buflen[0:4])

	if err != nil {
		if err == io.EOF {
			return nil, noconn
		} else {
			return nil, err
		}
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

	var tempBuf []byte
	var totalcount int

	for {

		tempBuf = make([]byte, 512)
		n, err = conn.c.Read(tempBuf[0:512])

		totalcount = totalcount + n

		if err != nil {
			if err == io.EOF {
				return nil, noconn
			} else {
				return nil, err
			}
		}

		buf.Write(tempBuf[0:n])

		if int(respLen) == totalcount {
			break
		}

	}

	return buf.Bytes(), nil

}
