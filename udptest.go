package main

import (
	"fmt"
	"net"
	"sync"
	//"time"
	//"runtime"
)

// creates a udp Conn and sends udp data
func udpClient(target string, num int, done *sync.WaitGroup) {
	// create conn
	addr, _ := resolve(target)
	conn, _ := dial(addr)
	defer conn.Close()

	// send bytes
	bytes := []byte(fmt.Sprintf("hello from %v", num))
	n, _ := write(conn, bytes)
	fmt.Printf("CLIENT:  Wrote %v bytes to %v: '%s'\n", n, addr, bytes[:n])

	// read bytes
	buf := make([]byte, 2048)
	n, _ = read(conn, buf)
	fmt.Printf("CLIENT:  Got %v bytes in reply: '%s'\n", n, buf[:n])

	// done
	done.Done()
}

// creates a udp Conn and reads data
func udpServer(target string, num int, done chan int) {
	// create conn
	addr, _ := resolve(target)
	conn, _ := listen(addr)
	defer conn.Close()

	for i := 0; i < num; i++ {
		// read bytes
		buf := make([]byte, 2048)
		n, raddr, _ := readfrom(conn, buf)
		fmt.Printf("SERVER:  Read %v bytes from %v: '%s'\n", n, raddr, buf[:n])

		// send reply
		bytes := []byte(fmt.Sprintf("world! (%v)", i))
		n, _ = writeto(conn, bytes, raddr)
		fmt.Printf("SERVER:  Replied with %v bytes: '%s'\n", n, bytes[:n])
	}

	// done
	done <- 0
}

func main() {
	//runtime.GOMAXPROCS(runtime.NumCPU())

	const target string = "localhost:1234"
	const num int = 100

	// start server
	done := make(chan int, 2)
	go udpServer(target, num, done)

	// start clients
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		wg.Add(1)
		go udpClient(target, i, &wg)
		//time.Sleep(10 * time.Millisecond)
	}

	// wait for completion
	<-done
	wg.Wait()
}

// wraps net.ResolveUDPAddr() with simple error handling
func resolve(target string) (addr *net.UDPAddr, err error) {
	addr, err = net.ResolveUDPAddr("udp", target)
	if err != nil {
		fmt.Println("ERROR from ResolveUDPAddr():", err.Error())
		panic(err)
	}
	return
}

// wraps net.DialUDP() with simple error handling
func dial(addr *net.UDPAddr) (conn *net.UDPConn, err error) {
	conn, err = net.DialUDP("udp", nil, addr)
	if err != nil {
		fmt.Println("ERROR from DialUDP():", err.Error())
		panic(err)
	}
	return
}

// wraps net.ListenUDP() with simple error handling
func listen(addr *net.UDPAddr) (conn *net.UDPConn, err error) {
	conn, err = net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("ERROR from ListenUDP():", err.Error())
		panic(err)
	}
	return
}

// wraps net.Conn.Write() with simple error handling
// only use with Conns created with DialUDP()
func write(conn *net.UDPConn, bytes []byte) (n int, err error) {
	n, err = conn.Write(bytes)
	if err != nil {
		fmt.Println("ERROR from Write():", err.Error())
		panic(err)
	}
	if n != len(bytes) {
		estr := fmt.Sprintf("ERROR - only %v bytes written", n)
		fmt.Println(estr)
		panic(estr)
	}
	return
}

// wraps net.Conn.WriteToUDP() with simple error handling
// only use with Conns created with ListenUDP()
func writeto(conn *net.UDPConn, bytes []byte, addr *net.UDPAddr) (n int, err error) {
	n, err = conn.WriteToUDP(bytes, addr)
	if err != nil {
		fmt.Println("ERROR from WriteToUDP():", err.Error())
		panic(err)
	}
	if n != len(bytes) {
		estr := fmt.Sprintf("ERROR - only %v bytes written", n)
		fmt.Println(estr)
		panic(estr)
	}
	return
}

// wraps net.UDPConn.Read() with simple error handling
func read(conn *net.UDPConn, buf []byte) (n int, err error) {
	n, err = conn.Read(buf)
	if err != nil {
		fmt.Println("Error from Read():", err.Error())
		panic(err)
	}
	return
}

// wraps net.UDPConn.ReadFromUDP() with simple error handling
func readfrom(conn *net.UDPConn, buf []byte) (n int, raddr *net.UDPAddr, err error) {
	n, raddr, err = conn.ReadFromUDP(buf)
	if err != nil {
		fmt.Println("Error from ReadFromUDP():", err.Error())
		panic(err)
	}
	return
}
