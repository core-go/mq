package amq

import (
	"github.com/go-stomp/stomp"
	"log"
	"os"
	"time"
)

func NewConn(userName string, password string, addr string) (*stomp.Conn, error) {
	var options []func(*stomp.Conn) error
	if len(userName) > 0 && len(password) > 0 {
		clientID, err := os.Hostname()
		if err != nil {
			log.Println("Cannot get hostname", err)
		}
		options = []func(*stomp.Conn) error{
			stomp.ConnOpt.Login(userName, password),
			// stomp.ConnOpt.Host("/"),
			stomp.ConnOpt.Header("client-id", clientID),
		}
	}
	return stomp.Dial("tcp", addr, options...)
}

func NewConnWithHeartBeat(userName string, password string, addr string, sendTimeout time.Duration, recvTimeout time.Duration) (*stomp.Conn, error) {
	var options []func(*stomp.Conn) error
	if len(userName) > 0 && len(password) > 0 {
		clientID, err := os.Hostname()
		if err != nil {
			log.Println("Cannot get hostname", err)
		}
		options = []func(*stomp.Conn) error{
			stomp.ConnOpt.Login(userName, password),
			// stomp.ConnOpt.Host("/"),
			stomp.ConnOpt.Header("client-id", clientID),
			stomp.ConnOpt.HeartBeat(sendTimeout, recvTimeout),
		}
	} else {
		options = []func(*stomp.Conn) error{stomp.ConnOpt.HeartBeat(sendTimeout, recvTimeout)}
	}
	return stomp.Dial("tcp", addr, options...)
}
