package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sync"
	"time"
)

// creates a udp Conn and sends udp data
func udpClient(target string, clientnum int, done *sync.WaitGroup) {
	// create conn
	addr, _ := resolve(target)
	conn, _ := dial(addr)
	defer conn.Close()

	// loop, sending until we receive
	for i := 1; ; i++ {
		// send bytes
		bytes := []byte(fmt.Sprintf("hello from #%v", clientnum))
		write(conn, bytes)

		// read bytes (possibly timing out)
		buf := make([]byte, 2048)
		_, _, timeout, _ := readwithtimeout(conn, buf, 2*time.Second)
		if !timeout {
			//fmt.Printf("CLIENT #%v:  Got %v bytes in reply (try #%v): '%s'\n", clientnum, nbytes, i, buf[:nbytes])
			break // success
		}
		fmt.Printf("CLIENT #%v:  Timed out on request #%v; retrying...\n", clientnum, i)
	}

	// done
	done.Done() // let people know
}

// responds to UDP messages
// runs until quit is signaled, then signals done
func udpServer(target string, quit chan int, done chan int) {
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

	// done
	myquit <- 0       // signal reader to stop
	for range count { // wait for handler to stop
	}
	done <- 0 // signal we're done
}

// holds a udp message (received or sent)
type message struct {
	bytes []byte
	raddr *net.UDPAddr
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
			if rand.Intn(50) == 0 {
				continue // drop 2% of packets
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

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UnixNano())

	const target string = "localhost:1234"
	const num int = 10000

	// start server
	srvquit, srvdone := make(chan int), make(chan int)
	go udpServer(target, srvquit, srvdone)

	// start clients
	var clients sync.WaitGroup
	for i := 0; i < num; i++ {
		clients.Add(1)
		go udpClient(target, i, &clients)
		time.Sleep(time.Microsecond) // allow server to run
	}

	// quit
	clients.Wait() // wait for clients
	srvquit <- 0   // signal server to stop
	<-srvdone      // wait for everyone
}

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

// wraps net.Conn.WriteToUDP() with simple error handling
// only use with Conns created with ListenUDP()
func writeto(conn *net.UDPConn, bytes []byte, addr *net.UDPAddr) (n int, err error) {
	n, err = conn.WriteToUDP(bytes, addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR from WriteToUDP():", err.Error())
		panic(err)
	}
	if n != len(bytes) {
		estr := fmt.Sprintf("ERROR - only %v bytes written", n)
		fmt.Fprintln(os.Stderr, estr)
		panic(estr)
	}
	return
}

// wraps net.UDPConn.Read() with simple error handling
func read(conn *net.UDPConn, buf []byte) (n int, err error) {
	n, err = conn.Read(buf)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR from Read():", err.Error())
		panic(err)
	}
	return
}

// wraps net.UDPConn.ReadFromUDP() with simple error handling
func readfrom(conn *net.UDPConn, buf []byte) (n int, raddr *net.UDPAddr, err error) {
	n, raddr, err = conn.ReadFromUDP(buf)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR from ReadFromUDP():", err.Error())
		panic(err)
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
