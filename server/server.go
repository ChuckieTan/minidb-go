package server

import (
	"encoding/gob"
	"errors"
	"minidb-go/tbm"
	"minidb-go/transporter"
	"net"

	log "github.com/sirupsen/logrus"
)

const (
	NETWORK = "tcp"
	ADDRESS = ":8080"
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
	log.Info("Server start at port %v use %v", ADDRESS, NETWORK)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go server.handle(conn)
	}
}

func (server *Server) handle(conn net.Conn) {
	log.Info("new connection from %v", conn.RemoteAddr())
	defer func() {
		log.Info("lose connection from %v", conn.RemoteAddr())
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

func getNextStmt(conn net.Conn) (string, error) {
	var stmt string
	buf := make([]byte, 1)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return stmt, err
		}
		if n != 1 {
			return "", errors.New("read error")
		}
		if buf[0] == ';' {
			break
		}
		stmt += string(buf)
	}
	return stmt, nil
}
