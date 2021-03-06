package voldemort

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
)

type VoldemortPool struct {
	// Channel used to control access to multiple VoldemortConn objects
	pool chan *VoldemortConn

	failures chan *VoldemortConn

	// used to track how many connections we should have from each server
	// if a conn goes down, we should then be able to find the one with less and therefore retry
	servers map[string]int

	// keep a count of active servers - servers that are capable of being queried
	active      int
	active_lock sync.Mutex

	timeout    time.Duration // timeout before getConn returns an error
	conn_count int           // number of connections per server

	// Track size of pool - the pool in the amount of servers not currently out on jobs
	size      int
	size_lock sync.Mutex

	closed bool // state of the pool - false if open/true if closed

	Metrics metrics.Registry
}

func NewPool(bserver *net.TCPAddr, proto string, pool_timeout time.Duration, conn_count int) (*VoldemortPool, error) {

	// we need to dial one server in the beginning to get all the details against the cluster
	vc, err := Dial(
		bserver,
		proto,
		metrics.NewRegistry(),
	)

	if err != nil {
		return nil, err
	}

	// Find out how many servers there are so we can make a nice pool.
	poolSize := len(vc.cl.Servers)

	// This channel will be used to hold all the conns and distribute them to clients - size is number of available server multiplied by the number of connections per server
	p := make(chan *VoldemortConn, poolSize*conn_count)

	// The failure chan will be unbuffered
	f := make(chan *VoldemortConn)

	var (
		nvc   *VoldemortConn
		faddr string
	)

	// initialise the map - this creates the structure and all counters (int) will be 0
	servers := make(map[string]int)

	var activeCount int

	reg := metrics.NewRegistry()

	// create the correct number of connections per server
	for j := 0; j < conn_count; j++ {

		for _, v := range vc.cl.Servers {

			faddr = fmt.Sprintf("%s:%d", v.Host, v.Socket)

			log.Printf("Adding server to pool - %s", faddr)

			addr, err := net.ResolveTCPAddr("tcp", faddr)
			if err != nil {
				log.Fatal(err)
			}

			nvc, err = Dial(addr, proto, reg)
			if err != nil {
				log.Printf("server - %s - unavailable - cannot add to the pool", addr)
				continue
			}

			activeCount++
			// Update the connection counter for this server
			servers[faddr]++

			// Add the conn to the channel so it can be used
			p <- nvc

		}

	}

	// Initialise the pool with all the required variables
	vp := &VoldemortPool{
		pool:     p,
		failures: f,
		size:     poolSize,
		timeout:  pool_timeout,
		active:   activeCount,
		servers:  servers,
		closed:   false,
		Metrics:  reg,
	}

	// start the watcher!
	go vp.watcher()

	// close the original connection to Voldemort - we don't need it now we have the pool.
	vc.Close()

	return vp, nil

}

type Servermetrics struct {
	Servers []*SMetric
}

type SMetric struct {
	Name  string
	Count int64
	Mean  float64
	Max   int64
	Min   int64
}

func (vp *VoldemortPool) Metricdata() (Ss *Servermetrics) {

	Ss = &Servermetrics{
		Servers: []*SMetric{},
	}

	var s *SMetric

	vp.Metrics.Each(func(name string, i interface{}) {

		switch metric := i.(type) {
		case metrics.Timer:
			t := metric.Snapshot()

			s = &SMetric{
				Name:  name,
				Count: t.Count(),
				Mean:  t.Mean(),
				Max:   t.Max(),
				Min:   t.Min(),
			}

			Ss.Servers = append(Ss.Servers, s)

			/*
				fmt.Fprintf(w, "%s \n", name)

				fmt.Fprintf(
					w,
					"Mean: %b \nMax: %d \nMin: %d \nCount: %d \n",
					t.Mean(),
					t.Max(),
					t.Min(),
					t.Count(),
				)
			*/
		}
	})

	return

}

// Get a VoldemortConn struct from the channel and return it
func (vp *VoldemortPool) GetConn() (vc *VoldemortConn, err error) {

	if vp.active == 0 {
		return nil, errors.New("no active servers available")
	}

	if vp.timeout == 0 {

		vc = <-vp.pool

		return vc, nil

	} else {

		// return after 250 milliseconds regardless of result - protect the app!
		select {
		case vc = <-vp.pool:

			return vc, nil
		case _ = <-time.After(vp.timeout):
			return nil, errors.New("timeout getting a connection to voldemort")
		}

	}

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

		newvc, err := Dial(vaddr, vc.proto, vp.Metrics)

		if err == nil {

			log.Printf("new connection found - %s", vc.s)

			// Wait 1 minute before actually doing queries to let the node catch up
			time.Sleep(2 * time.Second)

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

}

func (vp *VoldemortPool) ReleaseConn(vc *VoldemortConn, state bool) {

	if !state {
		// OH dear - it looks like a conn has failed - time to sort that out!
		// we need a new conn here
		log.Printf("server failure - %s", vc.s)
		vp.failures <- vc
		return
	}

	// make sure the pool isn't closed
	if vp.closed == false {
		vp.pool <- vc
	}

	return

}

func (vp *VoldemortPool) Empty() {

	var vc *VoldemortConn

	// close the pool
	vp.closed = true
	close(vp.pool)

	// now that we have closed the pool run through what's left on it and close all the conns
	select {
	case vc = <-vp.pool:
		log.Printf("closing conn - %s", vc.s)
		vc.Close()
	default:
		return
	}

	log.Println("all voldemort connections closed")
}
