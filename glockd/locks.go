package main

// Locks request channel and data structure
var lock_channel = make(chan lock_request, 8192)
var locks = map[string] string {}

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

