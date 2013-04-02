package main

import(
	"fmt"
	"bufio"
	"strings"
	"net"
)

// valid command list
var commands = []string { "d", "sd","i", "si", "g", "sg", "r", "sr", "q", "dump" }

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
	stats_channel <- stat_bump{ stat: "connections", val: -1 }
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

	stats_channel <- stat_bump{ stat: "connections", val: 1 }

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
			if cfg_verbose {
				fmt.Printf( "%s invalid command '%s'\n", my_client, strings.Trim( string(buf), string(0) ) )
			}
			// if we got an invalid command, skip it
			continue
		}

		// Always bump the command stats
		stats_channel <- stat_bump{ stat: "command_"+command[0], val: 1 }

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
				for _, idx := range stat_keys() {
					switch idx {
						case "locks":
							rsp = []byte( string(rsp) + fmt.Sprintf("%s: %d\n", idx, len(locks)) )
							continue
						case "shared_locks":
							rsp = []byte( string(rsp) + fmt.Sprintf("%s: %d\n", idx, len(shared_locks)) )
							continue
					}
					rsp = []byte( string(rsp) + fmt.Sprintf("%s: %d\n", idx, stats[idx]) )
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
				if val != "0" {
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
			case "dump":
				if lock == "shared" {
					rsp = []byte( fmt.Sprintf("%v\n", shared_locks) )
				} else {
					rsp = []byte( fmt.Sprintf("%v\n", locks) )
				}
		}

		// Write our response back to the client
		_, _ = conn.Write( rsp );

	}
}

