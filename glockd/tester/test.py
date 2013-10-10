#!/usr/bin/env python
import socket
import random
import string
import re
from optparse import OptionParser

# pip install websocket-client
from websocket import create_connection

parser = OptionParser()
parser.add_option("-u", "--unix", action="store", type="string", dest="unix", default="/tmp/glockd.sock", help="path to the unix socket")
parser.add_option("-t", "--tcp", action="store", type="string", dest="tcp", default="127.0.0.1:9999", help="address:port to the tcp socket")
parser.add_option("-w", "--ws", action="store", type="string", dest="ws", default="ws://127.0.0.1:9998/", help="url for the websocket listener")
(options, args) = parser.parse_args()

class gsock:
	def cmd(self, data):
		self.socket.sendall("%s\n" %data)
		(v, s) = self.socket.recv(1024000).strip().split(" ", 1)
		v = int(v)
		return (v, s)

	def close(self):
		self.socket.close()

class gunix(gsock):
    def __init__(self, path):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.connect(path)

class gtcp(gsock):
	def __init__(self, address):
		self.socket = socket.socket(socket.AF_INET)
		(h, p) = address.split(":")
		p = int(p)
		self.socket.connect((h, p))

class gws():
	def __init__(self, address):
		self.socket = create_connection(address)

	def cmd(self, data):
		self.socket.send(data)
		(v, s) = self.socket.recv().strip().split(" ", 1)
		v = int(v)
		return (v, s)

	def close(self):
		self.socket.close()

def test_registry(one, two):
	(i1, v1) = one.cmd( 'me' )
	(i2, v2) = two.cmd( 'me' )
	prefix = ok if v1 != v2 else no
	print prefix + "[me ] client1 and client2 have unique default identifiers"

	(i, v) = one.cmd( 'iam client1' )
	prefix = ok if i == 1 else no
	print prefix + "[iam] client1 changed its name"

	(i, v) = one.cmd( 'me' )
	v1 = v1.split(" ", 1)
	v1[1] = "client1"
	v1 = " ".join(v1)
	prefix = ok if v == v1 else no
	print prefix + "[me ] client1 now shows proper new name via the me command"

def test_exclusive(one, two):
	(i, v) = one.cmd( 'i ' + random_lock_string )
	prefix = ok if i == 0 else no
	print prefix + "[i  ] exclusive lock should not yet be held"

	(i, v) = one.cmd( 'g ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[g  ] first client should get exclusive lock"
	
	(i, v) = two.cmd( 'i ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[i  ] exclusive lock should now be held"

	(i, v) = one.cmd( 'g ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[g  ] first client should get exclusive lock again if rerequested"

	(i, v) = two.cmd( 'g ' + random_lock_string )
	prefix = ok if i == 0 else no
	print prefix + "[g  ] second client should not get exclusive lock obtained by first client"

	(i, v) = two.cmd( 'r ' + random_lock_string )
	prefix = ok if i == 0 else no
	print prefix + "[r  ] second client should not be able to release an exclusive lock that it does not have"

	(i, v) = one.cmd( 'r ' + random_lock_string )
	refix = ok if i == 1 else no
	print prefix + "[r  ] first client should be able to release its exclusive lock"

	(i, v) = two.cmd( 'g ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[g  ] second client should be able to get the recently released exclusive lock"

	(i, v) = one.cmd( 'g ' + random_lock_string )
	prefix = ok if i == 0 else no
	print prefix + "[g  ] first client should not get exclusive lock obtained by second client"

def test_shared(one, two):
	(i, v) = one.cmd( 'si ' + random_lock_string )
	prefix = ok if i == 0 else no
	print prefix + "[si ] shared lock should not be held"

	(i, v) = one.cmd( 'sr ' + random_lock_string )
	prefix = ok if i == 0 else no
	print prefix + "[sr ] first client should not be able to release a shared lock that it has not obtained"

	(i, v) = one.cmd( 'sg ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[sg ] first client should be able to get shared lock and see that it is the first client to do so"

	(i, v) = one.cmd( 'sg ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[sg ] first client should be able to get shared lock again but not increment the counter"

	(i, v) = two.cmd( 'si ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[si ] second client should now see the lock held (rval: 1)"

	(i, v) = two.cmd( 'sg ' + random_lock_string )
	prefix = ok if i == 2 else no
	print prefix + "[sg ] second client should also get shared lock and see that it is the second client to do so"

	(i, v) = two.cmd( 'sg ' + random_lock_string )
	prefix = ok if i == 2 else no
	print prefix + "[sg ] second client should also get shared lock again but not increment the counter"

	(i, v) = one.cmd( 'si ' + random_lock_string )
	prefix = ok if i == 2 else no
	print prefix + "[si ] first client should now see the lock held by two clients"

	(i, v) = one.cmd( 'sr ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[sr ] first client should be able to release a shared lock that it has obtained"

	(i, v) = two.cmd( 'si ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[si ] second client should now see the lock held by one client"

def test_orphan(one, two):
	(i, v) = two.cmd( 'i ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[i  ] second client should have the exclusive lock"
	
	(i, v) = two.cmd( 'si ' + random_lock_string )
	prefix = ok if i == 1 else no
	print prefix + "[si ] second client should have the shared lock"

	two.close()
	print ok + "[---] second client has disconnected"

	(i, v) = one.cmd( 'i ' + random_lock_string )
	prefix = ok if i == 0 else no
	print prefix + "[i  ] first client should now see the exclusive lock as unlocked"

	(i, v) = one.cmd( 'si ' + random_lock_string )
	prefix = ok if i == 0 else no
	print prefix + "[i  ] first client should now see the shared lock as unlocked"

	one.close()
	print ok + "[---] first client has disconnected"

def test(one, two):
	print "\tTesting registry"
	test_registry(one, two)
	print "\tTesting exclusive locks"
	test_exclusive(one, two)
	print "\tTesting shared locks"
	test_shared(one, two)
	print "\tTesting orphaning of locks"
	test_orphan(one, two)

def test_unix():
	print "Testing UNIX Sockets (%s)" % options.unix
	one = gunix( options.unix )
	two = gunix( options.unix )
	test( one, two )

def test_tcp():
	print "Testing TCP Sockets (%s)" % options.tcp
	one = gtcp( options.tcp )
	two = gtcp( options.tcp )
	test( one, two )

def test_ws():
	print "Testing WebSockets (%s)" % options.ws
	one = gws( options.ws )
	two = gws( options.ws )
	test( one, two )


def test_all():
	test_unix()
	test_tcp()
	test_ws()

ok = u"\t\t\033[92m\u2713\033[0m "
no = u"\t\t\033[91m\u2717\033[0m "
random_lock_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(40))

test_all()
