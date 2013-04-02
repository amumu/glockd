package main

import(
	"fmt"
	"net"
	"os"
)

const (
	RECV_BUF_LEN = 1024
)

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
	var reply_channel = make(chan lock_reply, 1)
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

