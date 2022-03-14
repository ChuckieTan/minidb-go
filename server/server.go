package server

import (
	"encoding/gob"
	"fmt"
	"minidb-go/tbm"
	"minidb-go/transporter"
	"net"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const (
	NETWORK = "tcp"
	ADDRESS = "127.0.0.1:8080"
)

type Server struct {
	tbm *tbm.TableManager
}

func NewServer(isOpen, isCreate bool, path string) *Server {
	server := &Server{}
	if isOpen && isCreate {
		logrus.Fatal("create and open can't be both true")
		return nil
	}
	if isCreate {
		log.Infof("create database: %v", path)
		server.tbm = tbm.Create(path)
	} else if isOpen {
		log.Infof("open database: %v", path)
		server.tbm = tbm.Open(path)
	} else {
		logrus.Fatal("create or open database")
	}
	return server
}

func (server *Server) Start() {
	go func() {
		listener, err := net.Listen(NETWORK, ADDRESS)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			listener.Close()
			if server.tbm != nil {
				server.tbm.Close()
			}
		}()
		log.Infof("server start at %v://%v", NETWORK, ADDRESS)
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatal(err)
			}
			go server.handle(conn)
		}
	}()
	WaitForExit()
}

func (server *Server) handle(conn net.Conn) {
	log.Infof("new connection from %v", conn.RemoteAddr())
	defer func() {
		log.Infof("lose connection from %v", conn.RemoteAddr())
		conn.Close()
	}()
	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	for {
		request := &transporter.Request{}
		err := dec.Decode(request)
		if err != nil {
			log.Error(err)
			return
		}

		response := ExecuteStmt(server.tbm, request)
		err = enc.Encode(response)
		if err != nil {
			log.Error(err)
			return
		}
	}
}

func WaitForExit() {
	// 设置退出信号
	exit := make(chan bool)

	// 等待用户输入
	go func() {
		var input string
		for {
			_, err := fmt.Scanln(&input)
			if err != nil {
				log.Error(err)
				continue
			}

			if input == "exit" {
				exit <- true
				return
			}
		}
	}()

	// 等待退出信号
	<-exit
	log.Info("exit")
}
