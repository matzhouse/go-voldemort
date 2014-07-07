package voldemort

import (
	"fmt"
	"log"
	"net"
	"time"
)

type VoldemortPool struct {
	// Channel used to control access to multiple VoldemortConn objects
	pool chan *VoldemortConn

	failures chan *VoldemortConn

	// used to track how many connections we should have from each server
	// if a conn goes down, we should then be able to find the one with less and therefore retry
	servers map[string]int

	// Track size of pool
	size int
}

func NewPool(bserver *net.TCPAddr, proto string) (*VoldemortPool, error) {

	// we need to dial one server in the beginning to get all the details against the cluster
	vc, err := Dial(bserver, proto)

	if err != nil {
		return nil, err
	}

	// Find out how many servers there are so we can make a nice pool - only ony of each for now!
	poolSize := len(vc.cl.Servers)

	// This channel will be used to hold all the conns and distribute them to clients
	p := make(chan *VoldemortConn, poolSize)

	// The failure chan will be unbuffered
	f := make(chan *VoldemortConn)

	var (
		nvc   *VoldemortConn
		faddr string
	)

	// initialise the map - this creates the structure and all counters (int) will be 0
	servers := make(map[string]int)

	for j := 0; j < 1; j++ {

		for _, v := range vc.cl.Servers {

			faddr = fmt.Sprintf("%s:%d", v.Host, v.Socket)

			log.Printf("Adding server to pool - %s", faddr)

			addr, err := net.ResolveTCPAddr("tcp", faddr)
			if err != nil {
				log.Fatal(err)
			}

			nvc, err = Dial(addr, proto)
			if err != nil {
				log.Printf("server - %s - unavailable - cannot add to the pool", addr)
				continue
			}

			// Update the connection counter for this server
			servers[faddr]++

			// Add the conn to the channel so it can be used
			p <- nvc

		}

	}

	// Initialise the pool with all the required variables
	vp := &VoldemortPool{pool: p, failures: f, size: poolSize, servers: servers}

	// start the watcher!
	go vp.watcher()

	return vp, nil

}

// Get a VoldemortConn struct from the channel and return it
func (vp *VoldemortPool) GetConn() (vc *VoldemortConn) {

	vc = <-vp.pool

	// decrease the size param - not locked or anything so mainly used for simple stats
	vp.size--

	return

}

// watcher is run in a go routine and sits around just watching for failures
// when it spots one it throws it over the another reconnect() running in another go routine
func (vp *VoldemortPool) watcher() {

	var vc *VoldemortConn

	log.Println("conn watcher running")

	for {

		vc = <-vp.failures

		log.Println("failure collected")

		go vp.reconnect(vc)

	}

}

// the client will try and reconnect forever but with incremental backoff to 1 minute {1,2,4,8,16,32,60}
func (vp *VoldemortPool) reconnect(vc *VoldemortConn) {

	log.Printf("trying to reconnect - %s", vc.s)

	var (
		retry int = 1
		d     time.Duration
	)

	for {

		vaddr, err := net.ResolveTCPAddr("tcp", vc.s)
		if err != nil {
			log.Printf("reconnecting to %s - address error - %s", vc.s, err)
		}

		newvc, err := Dial(vaddr, vc.proto)

		if err == nil {

			log.Printf("new connection found - %s", vc.s)

			// Wait 1 minute before actually doing queries to let the node catch up
			time.Sleep(1 * time.Minute)

			vp.ReleaseConn(newvc, true)
			return

		}

		log.Printf("error reconnecting to %s - %s - retrying in %d seconds", vc.s, err, retry)

		d, err = time.ParseDuration(fmt.Sprintf("%ds", retry))

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(d)

		if retry >= 60 {
			retry = 60
			continue
		}

		retry = retry * 2

	}

	return

}

func (vp *VoldemortPool) ReleaseConn(vc *VoldemortConn, state bool) {

	if !state {
		// OH dear - it looks like a conn has failed - time to sort that out!
		// we need a new conn here
		log.Println("server failure - %s", vc.s)

		vp.failures <- vc

		return

	}

	vp.pool <- vc

	vp.size++

	return

}

func (vp *VoldemortPool) Empty() {

	var vc *VoldemortConn

	for i := 0; i < vp.size; i++ {

		log.Println("closing conn - %s", vc.s)
		vc = vp.GetConn()
		vc.Close()

	}

	log.Println("all connections closed")
}
