package server

import (
	"encoding/gob"
	"minidb-go/tbm"
	"minidb-go/transporter"
	"net"

	log "github.com/sirupsen/logrus"
)

const (
	NETWORK = "tcp"
	ADDRESS = "127.0.0.1:8080"
)

type Server struct {
	tbm *tbm.TableManager
}

func NewServer(tbm *tbm.TableManager) *Server {
	return &Server{
		tbm: tbm,
	}
}

func (server *Server) Start() {
	listener, err := net.Listen(NETWORK, ADDRESS)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("Server start at %v://%v", NETWORK, ADDRESS)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go server.handle(conn)
	}
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
