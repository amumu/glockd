package main

import(
	"net"
	"os"
	"fmt"
	"bufio"
	"strings"
	"flag"
	"syscall"
)

const (
	RECV_BUF_LEN = 1024
)

// Structure for bumping a stat
type stat_bump struct {
	stat string
	val int
}

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

// valid command list
var commands = []string { "d", "sd","i", "si", "g", "sg", "r", "sr", "q" }

// stats bump channel and data structure
var stats_channel = make(chan stat_bump, 1024)
var stats = map[string] int {
	"command_d": 0,
	"command_sd": 0,
	"command_i": 0,
	"command_si": 0,
	"command_g": 0,
	"command_sg": 0,
	"command_r": 0,
	"command_sr": 0,
	"command_q": 0,
	"connections": 0,
	"locks": 0,
	"shared_locks": 0,
	"orphans": 0,
	"shared_orphans": 0,
	"invalid_comments": 0,
}

// Locks request channel and data structure
var lock_channel = make(chan lock_request, 1024)
var locks = map[string] string {}

// Shared locks request channel and data structure
var shared_lock_channel = make(chan lock_request, 1024)
var shared_locks = map[string] []string {}

var cfg_port int
var cfg_pidfile string
var cfg_verbose bool

func main() {
	flag.IntVar(&cfg_port, "port", 47200, "Listen on the following TCP Port (default: 47200)")
	flag.StringVar(&cfg_pidfile, "pidfile", "", "pidfile to use (required)")
	flag.BoolVar(&cfg_verbose, "verbose", false, "be verbose about what's going on (default:false)");
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
	// Block on looping for incoming connections
	mind_network()
}

func mind_locks() {
	var response string;
	for true {
		// Block this specific goroutine until we get a request
		req := <-lock_channel
		// Immediately check to see if the lock exists in the global state
		// since literally everything else in the function depends on this
		// information
		_, present := locks[req.lock]
		switch req.action {
			case -1:
				// The client wants to rellease the lock
				if present {
					// Cool. Done 
					delete( locks, req.lock )
					response = "1 Released Lock\n"
					// Bump
					stats_channel <- stat_bump{ stat: "locks", val: -1 }
				} else {
					// No dice
					response = "0 Cannot Release Lock\n"
				}
			case 0:
				// The client is checking on a lock
				if present {
					// Yep, locked
					response = "1 Locked\r\n"
				} else {
					// Nope, not locked
					response = "0 Not Locked\r\n"
				}
				break
			case 1:
				// The client wants to obtain a lock
				if present {
					// But can't because it's already locked
					response = "0 Cannot Get Lock\r\n"
				} else {
					// Cool, done.
					locks[req.lock] = req.client
					response = "1 Got Lock\r\n"
					// Bump
					stats_channel <- stat_bump{ stat: "locks", val: 1 }
				}
				break
		}
		// Reply back to the client on the channel it provided us with in the request
		req.reply <- lock_reply{ lock: req.lock, response: response }
	}
}

func shared_locks_unset( lock string, index int ) {
	// This function exists to make the following hack (shamelessly stolen from https://code.google.com/p/go-wiki/wiki/SliceTricks)
	// readable since it was insanely long inline and indented with longer variable names...
	shared_locks[lock] = shared_locks[lock][:index+copy(shared_locks[lock][index:], shared_locks[lock][index+1:])]
}

func mind_shared_locks() {
	var client_present int
	var response string
	for true {
		// Block this specific goroutine until we get a request
		req := <-shared_lock_channel
		// Reset the state for client_present (the index of the 
		// client in the slice in the map for the shared lock)
		client_present = -1
		// fine out whether this lock even exists. This information
		// is used in essentially everything else we do here
		_, present := shared_locks[req.lock]
		// Reset our response variable. State flushing
		response = ""
		if present {
			// We only want to find the client_present index (if
			// any) in the lock slice is the lock slice exists :)
			for k, v := range shared_locks[req.lock] {
				if v == req.client {
					client_present = k
					break;
				}
			}
		}
		switch req.action {
			case -1:
				// Client wants to release a shared lock
				if present && client_present != -1 {
					// Since the lock exists and client_present is 
					// not -1 (which would be not present) we can 
					// Remove the client from the lock slice
					shared_locks_unset( req.lock, client_present )
					if len(shared_locks[req.lock]) == 0 {
						// Since the lock slice is now empty we can
						// remove the slice from the lock map
						delete(shared_locks, req.lock)
					}
					response = "1 Released Lock\r\n"
				} else {
					// But we can't because we have no such lock
					response = "0 Cannot Release Lock\r\n"
				}
			case 0:
				// Client wants info about a lock
				if present {
					// Locked, give 'em a number
					response = fmt.Sprintf("%d Locked\r\n", len(shared_locks[req.lock]) )
				} else {
					// Not locked. 0 because: sanity
					response = "0 Not Locked\r\n"
				}
			case 1:
				// Client wants to lock something
				if present {
					// This lock exists in the lock map
					if client_present == -1 {
						// And the client doesnt exist in the slice, add 'em in
						shared_locks[req.lock] = append( shared_locks[req.lock], req.client )
					}
				} else {
					// This lock doesnt exist in the lock map so create
					// it with a new slice containing this client
					shared_locks[req.lock] = []string{ req.client }
				}
				// This always works... So we just need to return a count
				response = fmt.Sprintf("%d Got Lock\r\n", len(shared_locks[req.lock]) )
		}
		// Reply back to the client on the channel it provided us with in the request
		req.reply <- lock_reply{ lock: req.lock, response: response }
	}
}

func mind_stats() {
	// This function produces no output, it simply mutates the state
	for true {
		// Block this specific goroutine until we have a message incoming about a stats bump
		bump := <-stats_channel
		// Bump that stat
		stats[bump.stat] += bump.val
	}
}

func mind_network() {
	// Fire up the tcpip listening port
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", cfg_port) )
	if err != nil {
		// Or, you know... die...
		println("error listening:", err.Error())
		os.Exit(1)
	}
	// Loop forever
	for {
		// Got a connecting client
		conn, err := listener.Accept()
		// Maybe
		if err != nil {
			println("Error accept:", err.Error())
			continue
		}
		// Seems legit. Spawn a goroutine to handle this new client
		go lock_client(conn)
	}
}

func is_valid_command( command string ) bool {
	// Just a helper function to determine if a command is valid or not.
	for _, ele := range commands {
		if ele == command {
			// valid
			return true
		}
	}
	// not
	return false
}

func lock_req(lock string, action int, shared bool, my_client string) ( []byte, string ) {
	// Create a channel on which the lock or shared lock goroutine can contact us back on
	var reply_channel = make(chan lock_reply)
	// Send a non-blocking message to the proper goroutine about what we want
	if shared {
		shared_lock_channel <- lock_request{ lock:lock, action:action, reply:reply_channel, client:my_client }
	} else {
		lock_channel <- lock_request{ lock:lock, action:action, reply:reply_channel, client:my_client }
	}
	// Block until we recieve a reply
	rsp := <-reply_channel
	// Format and return our response
	var response = []byte(rsp.response)
	var terse = string(response[0])

	if cfg_verbose && terse != "0" {
		var display string
		if shared {
			display = "shared lock"
		} else {
			display = "lock"
		}
		switch action {
			case 1:
				fmt.Printf( "%s obtained %s for %s\n", my_client, display, lock )
			case -1:
				fmt.Printf( "%s released %s for %s\n", my_client, display, lock )
		}
	}

	return response, terse
}

func client_disconnected(my_client string, mylocks map[string] bool, myshared map[string] bool) {
	// Since the client has disconnected... we need to release all of the locks that it held
	if cfg_verbose {
		fmt.Printf( "%s disconnected\n", my_client )
	}
	for lock, _ := range mylocks {
		if ( cfg_verbose ) {
			fmt.Printf( "%s orphaned lock %s\n", my_client, lock )
		}
		lock_req(lock, -1, false, my_client)
		stats_channel <- stat_bump{ stat: "orphans", val: 1 }
	}
	// We also need to release all the shared locks that it held
	for lock, _ := range myshared {
		if ( cfg_verbose ) {
			fmt.Printf( "%s orphaned shared lock %s\n", my_client, lock )
		}
		lock_req(lock, -1, true, my_client)
		stats_channel <- stat_bump{ stat: "shared_orphans", val: 1 }
	}
	// Nothing left to do... That's all the client had...
}

func lock_client(conn net.Conn) {
	// Lots of variables local to thie goroutine. Because: reasons
	var command []string
	var rsp []byte
	var val string
	var lock string
	my_client := conn.RemoteAddr().String()
	mylocks := make(map [string] bool)
	myshared := make(map [string] bool)

	if cfg_verbose {
		fmt.Printf( "%s connected\n", my_client )
	}
	// The following handles orphaning locks... It only runs after the 
	// for true {} loop (which means on disconnect or error which are
	// the only things that breaks it)
	defer client_disconnected( my_client, mylocks, myshared )

	// Accept commands loop
	for true {

		// Read from the client
		buf, _, err := bufio.NewReader(conn).ReadLine()
		if err != nil {
			// If we got an error just exit
			return
		}
		command = strings.SplitN( strings.TrimSpace(string(buf)), " ", 2 )
		if false == is_valid_command(command[0]) {
			stats_channel <- stat_bump{ stat: "invalid_commands", val: 1 }
			// if we got an invalid command, skip it
			continue
		}

		// We always want a lock, even if the lock is ""
		if len(command) == 1 {
			command  = append(command, "")
		}

		// Nothing sane about assuming sanity
		lock = strings.Join( strings.Fields( command[1] ), " ")

		// Actually deal with the command now...
		switch command[0] {
			case "q":
				// loop over stats and generated a response
				rsp = []byte("")
				for idx, val := range stats {
					rsp = []byte( string(rsp) + fmt.Sprintf("%s: %d\n", idx, val) )
				}
			case "i":
				// does the lock exist locally?
				_, present := mylocks[lock]
				if present {
					// if we have the lock, don't bother the lock goroutine
					rsp = []byte("1 Locked\n")
				} else {
					// otherwise check the canonical source
					rsp, _ = lock_req( lock, 0, false, my_client )
				}
			case "g":
				// does the lock exist locally?
				_, present := mylocks[lock]
				if present {
					// if we have the lock then the answer is always "got it"
					rsp = []byte("1 Got Lock\n")
				} else {
					// otherwise request it from the canonical goroutine
					rsp, val = lock_req( lock, 1, false, my_client )
					if val == "1" {
						mylocks[lock] = true
					}
				}
			case "r":
				// does the lock exist locally?
				_, present := mylocks[lock]
				if present {
					// We only request the lock release if it exists locally, 
					// otherwise we have no permissions to unlock it
					rsp, val = lock_req( lock, -1, false, my_client )
					if val == "1" {
						// if we released the lock successfully then purge it 
						// from this goroutines map.
						delete(mylocks, lock )
					}
				}
			case "si":
				// Since we always want an "up to date" and accurate count
				// (not just a boolean true/false like exclusive locks)
				// Always pass this through to the canonical source
				rsp, val = lock_req( lock, 0, true, my_client )
			case "sg":
				rsp, val = lock_req( lock, 1, true, my_client )
				if val == "1" {
					// Since we now have this lock... add it to the goroutine
					// lock map.  Used for orphaning
					myshared[lock] = true
				}
			case "sr":
				rsp, val = lock_req( lock, -1, true, my_client )
				if val == "1" {
					// Since we now no longer have this lock... remove it from
					// the goroutine lock map. No need to orphan it any longer
					delete(myshared, lock )
				}
			case "d":
				rsp = []byte("")
				// loop over all the locks
				for idx, val := range locks {
					// if we want all locks, or this specific lock matches the lock we 
					// want then add it to the response output
					if lock == "" || lock == idx {
						rsp = []byte( string(rsp) + fmt.Sprintf("%s: %s\n", idx, val))
					}
				}
			case "sd":
				rsp = []byte("")
				// loop over all the locks
				for idx, val := range shared_locks {
					// if we want all locks, or this specific lock matches the lock we
					// want then add it to the response output
					if lock == "" || lock == idx {
						for _, locker := range val {
							rsp = []byte( string(rsp) + fmt.Sprintf("%s: %s\n", idx, locker))
						}
					}
				}
		}

		// Write our response back to the client
		_, _ = conn.Write( rsp );

		// Always bump the command stats
		stats_channel <- stat_bump{ stat: "command_"+command[0], val: 1 }
	}
}

