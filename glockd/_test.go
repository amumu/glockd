package main

import(
	"net"
	"fmt"
	"flag"
	"os"
	"math/rand"
	"time"
	"strings"
)

const (
	THREADS = 512
)

var channel = make(chan int, THREADS)
var cfg_host string

func client(tcpAddr *net.TCPAddr) (*net.TCPConn, error) {
	return net.DialTCP("tcp", nil, tcpAddr)
}

func lock_client(tcpAddr *net.TCPAddr) {
	conn, err := client(tcpAddr)
	err = err
	locks_to_get := ( rand.Int() % 15 )
	shared_to_get := ( rand.Int() % 15 )
	reply := make([]byte, 2048)
	for i:=1; i<=locks_to_get; i++ {
		_, err = conn.Write([]byte( fmt.Sprintf("g %d\n", ( rand.Int() % 1000 ) ) ))
		_, err = conn.Read(reply)
	}
	for i:=1; i<=shared_to_get; i++ {
		_, err = conn.Write([]byte( fmt.Sprintf("g %d\n", ( rand.Int() % 1000 ) ) ))
		_, err = conn.Read(reply)
	}
	time.Sleep( time.Duration( rand.Int() % 30 ) * time.Second )
	conn.Close()
	channel <- 1
}

func stats_client(tcpAddr *net.TCPAddr) {
	fmt.Printf( "stats_client %s\n", tcpAddr )
	conn, err := client(tcpAddr)
	err = err
	reply := make([]byte, 1024)
	replystring := ""
	good := "cilso"
	for true {
		_, err = conn.Write([]byte("q\n"))
		_, err = conn.Read(reply)
		replystring = strings.Join( strings.Fields( string(reply) ), " " );
		valid := false
		switch replystring[0] {
			case good[0], good[1], good[2], good[3], good[4]: valid = true;
		}
		if valid == false {
			fmt.Printf( "ERROR GOT BAD RESPONSE: (%v) '%v'", err, replystring )
			os.Exit(1)
		}
		fmt.Printf( "\n\n---\n%s\n---\n\n", string(reply) )
		time.Sleep( time.Second )
	}
	channel <- 1
}

func main() {
	flag.StringVar(&cfg_host, "host", "", "host:port to connect to for testing")
	flag.Parse()
	if cfg_host == "" {
		fmt.Printf( "Please specify a hostname:port to connect to with -host\n" )
		os.Exit(1)
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", cfg_host)
	if err != nil {
		fmt.Printf( "dialing %v failed: %v\n", cfg_host, err )
		os.Exit(2)
	}
	go stats_client(tcpAddr)
	for i:=1; i<=(THREADS-1); i++ {
		go lock_client(tcpAddr)
	}
	for i:=1; i<=(THREADS-1); i++ {
		<-channel
	}
	os.Exit(0)
	<-channel
}
