GOPATH := $(shell readlink -f ./)/

all: build

build:
	env go install ./src/github.com/garyburd/go-websocket/websocket/
	env go build

clean:
	rm -rvf pkg
	rm -vf glockd

run: clean build exec

exec:
	./glockd -pidfile=glockd.pid

env:
	env
