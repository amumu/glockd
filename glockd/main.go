package main

import(
	"os"
	"fmt"
	"flag"
	"syscall"
	"runtime"
)

// Structure for requesting a lock with
type lock_request struct {
	lock string
	action int
	reply chan lock_reply
	client string
}

// Structure for a response generated during a lock request
type lock_reply struct {
	lock string
	response string
}

var cfg_port int
var cfg_pidfile string
var cfg_verbose bool
var cfg_ws int

func main() {
	runtime.GOMAXPROCS( runtime.NumCPU() )

	flag.IntVar(&cfg_port, "port", 9999, "Listen on the following TCP ws. 0 Disables. Default: 9999")
	flag.IntVar(&cfg_ws, "ws", 9998, "Listen on the following TCP Port. 0 Disables. Default: 9998")
	flag.StringVar(&cfg_pidfile, "pidfile", "", "pidfile to use (required)")
	flag.BoolVar(&cfg_verbose, "verbose", false, "be verbose about what's going on. Default:false");
	flag.Parse()

	if cfg_pidfile == "" {
		println( "Please specify a pidfile" )
		os.Exit(2)
	}

	pidfile, err1 := os.OpenFile(cfg_pidfile, os.O_CREATE | os.O_RDWR, 0666)
	err2 := syscall.Flock(int(pidfile.Fd()), syscall.LOCK_NB | syscall.LOCK_EX)
	if err1 != nil {
		fmt.Printf( "Error opening pidfile: %s: %v\n", cfg_pidfile, err1 )
		os.Exit(3)
	}
	if err2 != nil {
		fmt.Printf( "Error locking  pidfile: %s: %v\n", cfg_pidfile, err2 )
		os.Exit(4)
	}
	syscall.Ftruncate( int(pidfile.Fd()), 0 )
	syscall.Write( int(pidfile.Fd()), []byte(fmt.Sprintf( "%d", os.Getpid())) )

	// Spawn a goroutine for stats
	go mind_stats()
	// Spawn a goroutine for locks
	go mind_locks()
	// Spawn a goroutine for shared locks
	go mind_shared_locks()
	// Spawn a goroutine for the websockets interface
	go mind_websockets()
	// Block on looping for incoming connections
	go mind_tcp()

	wait := make(chan bool)
	<-wait
}

