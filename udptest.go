package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	//"sync"
	"time"
)

// holds a udp message (received or sent)
type message struct {
	bytes []byte
	raddr *net.UDPAddr
}

////////////////////////////////////////////////////////
// udpClient
////////////////////////////////////////////////////////

const MAX_TRIES int = 3

// holds stats for client performance
// trystats[ntries] = num messages successful
// trystats[0] = num messages unsuccessful (timed out)
type trystats [MAX_TRIES + 1]int

// adds stats to a stats object
func (s1 *trystats) Add(s2 *trystats) {
	for i, val := range s2 {
		s1[i] += val
	}
}

// totals all entries
func (s *trystats) Total() (total int) {
	for _, val := range s {
		total += val
	}
	return
}

// reads requests off input channel, uses udpRequest() to process
// requests that time out are written to timeout channel
// runs until requests input channel is closed
// sends stats when done
func udpClient(requests, timeouts chan *message, clientname string, stats chan *trystats) {
	// keep track of stats
	mystats := trystats{}
	defer func() { stats <- &mystats }() // push our stats when done

	// read requests off input channel until closed
	for request := range requests {
		ok, ntries := udpRequest(request, MAX_TRIES)
		switch {
		case !ok: // timeout
			timeouts <- request
			mystats[0] += 1 // note that map[nonexist] == zero
		default: // success
			mystats[ntries] += 1 // note that map[nonexist] == zero
		}
	}
}

// creates a udp Conn, sends data, and waits for ack
// retries a few times until it gets an ack
func udpRequest(msg *message, maxtries int) (ok bool, ntries int) {
	// create conn
	conn, _ := dial(msg.raddr)
	defer conn.Close()

	// loop, sending until we receive
	for ntries = 1; ntries <= maxtries; ntries++ {
		// send
		write(conn, msg.bytes)

		// read bytes (possibly timing out)
		buf := make([]byte, 2048)
		_, _, timeout, _ := readwithtimeout(conn, buf, 2*time.Second)
		if !timeout {
			return true, ntries // success
		}
	}

	// failure
	return false, ntries
}

////////////////////////////////////////////////////
// udpServer
////////////////////////////////////////////////////

// responds to UDP messages
// runs until quit is signaled, then signals done
func udpServer(target string, quit chan int, done chan int) {
	defer func() { done <- 0 }() // signal when we're done

	// create conn
	addr, _ := resolve(target)
	conn, _ := listen(addr)
	defer conn.Close()

	// create pipeline
	myquit := make(chan int, 1)
	msgs, rerrs := udpReader(conn, myquit)
	count, serrs := udpHandler(conn, msgs)

	// manage pipeline
loop:
	for {
		select {
		case err := <-rerrs: // read error
			fmt.Fprintln(os.Stderr, "SERVER:  ERROR reading:", err.Error())
			panic(err)
		case err := <-serrs: // send error
			fmt.Fprintln(os.Stderr, "SERVER:  ERROR sending:", err.Error())
			panic(err)
		case <-count: // successful send
		case <-quit: // told to stop
			break loop // break without label would just break select
		}
	}

	// clean up
	myquit <- 0       // signal reader to stop
	for range count { // wait for handler to stop
	}
}

// reads UDP messages and pushes them to a channel
// errors are pushed to an error channel
// quits, closing msg/err channels, when quit channel signaled/closed
func udpReader(conn *net.UDPConn, quit chan int) (msgs chan *message, errs chan error) {
	msgs = make(chan *message, 100)
	errs = make(chan error, 10)

	reader := func() {
		defer close(msgs)
		defer close(errs)
		for {
			select {
			case <-quit: // quit?
				return
			default: // read
				buf := make([]byte, 2048)
				_, raddr, timeout, err := readWithTimeout(conn, buf, time.Millisecond)
				switch {
				case timeout:
					continue
				case err != nil:
					errs <- err
				default:
					//fmt.Printf("SERVER:  Read %v bytes from %v: '%s'\n", num, raddr, buf[:num])
					msgs <- &message{buf, raddr}
				}
			}
		}
	}
	go reader()
	return
}

// sends UDP messages in reply to messages read from channel
// count of sends written to channel
// errors pushed to error channel
// quits, closing count/err channels, when msgs channel is closed
func udpHandler(conn *net.UDPConn, msgs chan *message) (count chan int, errs chan error) {
	count = make(chan int, 100)
	errs = make(chan error, 10)

	handler := func() {
		defer close(count)
		defer close(errs)
		i := 0
		for msg := range msgs { // read channel until closed
			if rand.Intn(10) == 0 {
				continue // drop 1/10 packets
			}
			//send reply
			bytes := []byte(fmt.Sprintf("world! (%v)", i))
			num, err := conn.WriteToUDP(bytes, msg.raddr)
			switch {
			case err != nil:
				errs <- err
			case num < len(msg.bytes):
				fallthrough // what to do?
			default:
				//fmt.Printf("SERVER:  Replied with %v bytes: '%s'\n", num, bytes[:num])
				i++
				count <- i
			}
		}
	}
	go handler()
	return
}

/////////////////////////////////////////////////////////
// main
/////////////////////////////////////////////////////////

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UnixNano())

	const target string = "localhost:1234"
	const numrequests int = 10 * 1000
	const numclients int = 100

	// start server
	quit, done := make(chan int), make(chan int)
	go udpServer(target, quit, done)

	// start request source & timeout sink
	requests := startRequestGenerator(target, numrequests)
	timeouts := startTimeoutSink(done)

	// start clients
	stats := make(chan *trystats)
	for i := 0; i < numclients; i++ {
		go udpClient(requests, timeouts, fmt.Sprintf("%v", i), stats)
	}

	// consume stats (& wait for all clients to exit)
	allstats := trystats{}
	for i := 0; i < numclients; i++ {
		allstats.Add(<-stats)
	}
	printStats(&allstats)

	// clean up
	close(quit)     // signal server to stop
	close(timeouts) // signal timeoutSink to stop
	<-done          // wait for server & timeoutSink (twice)
	<-done
}

// starts a goroutine to generate request messages
// stand-in for queue reader
// closes request msg channel when done
func startRequestGenerator(target string, numrequests int) (requests chan *message) {
	requests = make(chan *message, 100)
	raddr, _ := resolve(target)

	generator := func() {
		defer close(requests)
		for i := 0; i < numrequests; i++ {
			bytes := []byte(fmt.Sprintf("Hello (%v)", i))
			requests <- &message{bytes, raddr}
		}
	}
	go generator()
	return
}

// starts a goroutine to handle timed out requests
// returns the channel upon which they should be pushed
func startTimeoutSink(done chan int) (input chan *message) {
	input = make(chan *message, 10)

	sink := func() {
		defer func() { done <- 0 }() // signal when done
		for msg := range input {
			fmt.Printf("SINK:  Got timed out request:  %s\n", msg.bytes)
		}
	}
	go sink()
	return
}

// prints client trystats
func printStats(stats *trystats) {
	fmt.Println("\nTotal number of requests attempted: ", stats.Total())
	fmt.Println("  timed out:            ", stats[0])
	fmt.Println("  successful (1st try): ", stats[1])
	for i := 2; i <= MAX_TRIES; i++ {
		fmt.Printf("             (%v tries):  %v\n", i, stats[i])
	}
}

/////////////////////////////////////////////////////
// UDP helper functions
/////////////////////////////////////////////////////

// wraps net.ResolveUDPAddr() with simple error handling
func resolve(target string) (addr *net.UDPAddr, err error) {
	addr, err = net.ResolveUDPAddr("udp", target)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR from ResolveUDPAddr():", err.Error())
		panic(err)
	}
	return
}

// wraps net.DialUDP() with simple error handling
func dial(addr *net.UDPAddr) (conn *net.UDPConn, err error) {
	conn, err = net.DialUDP("udp", nil, addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR from DialUDP():", err.Error())
		panic(err)
	}
	return
}

// wraps net.ListenUDP() with simple error handling
func listen(addr *net.UDPAddr) (conn *net.UDPConn, err error) {
	conn, err = net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR from ListenUDP():", err.Error())
		panic(err)
	}
	return
}

// wraps net.Conn.Write() with simple error handling
// only use with Conns created with DialUDP()
func write(conn *net.UDPConn, bytes []byte) (n int, err error) {
	n, err = conn.Write(bytes)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR from Write():", err.Error())
		panic(err)
	}
	if n != len(bytes) {
		estr := fmt.Sprintf("ERROR - only %v bytes written", n)
		fmt.Fprintln(os.Stderr, estr)
		panic(estr)
	}
	return
}

// wraps net.UDPConn.ReadFromUDP() with a timeout
// returns timeout=true, err=nil if timeout
func readWithTimeout(conn *net.UDPConn, buf []byte, wait time.Duration) (n int, raddr *net.UDPAddr, timeout bool, err error) {
	setreadtimeout(conn, wait)
	n, raddr, err = conn.ReadFromUDP(buf)
	switch {
	case err != nil:
		switch e := err.(type) {
		case net.Error:
			if e.Timeout() {
				timeout = true
				err = nil
			}
		}
	}
	return
}

//wraps readWithTimeout() with simple error handling
func readwithtimeout(conn *net.UDPConn, buf []byte, wait time.Duration) (n int, raddr *net.UDPAddr, timeout bool, err error) {
	n, raddr, timeout, err = readWithTimeout(conn, buf, wait)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR from readWithTimeout():", err.Error())
		panic(err)
	}
	return
}

// wraps net.UDPConn.SetReadDeadline() with simple error handling
func setreadtimeout(conn *net.UDPConn, d time.Duration) {
	err := conn.SetReadDeadline(time.Now().Add(d))
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR from SetReadDeadline():", err.Error())
		panic(err)
	}
}
