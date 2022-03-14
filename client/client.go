package client

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"minidb-go/serialization/tm"
	"minidb-go/transporter"
	"net"
	"os"

	log "github.com/sirupsen/logrus"
)

const (
	NETWORK = "tcp"
	ADDRESS = "127.0.0.1:8080"
)

type Client struct {
}

func NewClient() *Client {
	return &Client{}
}

func (client *Client) Start() {
	conn, err := net.Dial(NETWORK, ADDRESS)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	input := bufio.NewReader(os.Stdin)
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	fmt.Println("Welcome to minidb-go")
	fmt.Print("minidb> ")

	xid := tm.XID(0)
	stmt := ""
	for {
		line, err := input.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Error(err)
		}
		line = line[:len(line)-1]
		if line == "exit;" {
			return
		}
		if line == "" {
			fmt.Print("minidb> ")
			continue
		}
		if line[len(line)-1] == ';' {
			stmt = line
		} else {
			fmt.Print("    >>> ")
			stmt += line
			continue
		}

		// 发送数据
		request := &transporter.Request{
			Xid:  xid,
			Stmt: stmt,
		}
		err = enc.Encode(request)
		if err != nil {
			log.Error(err)
			return
		}

		// 接收数据
		response := &transporter.Response{}
		err = dec.Decode(response)
		if err != nil {
			log.Error(err)
		}
		if response.Err != "" {
			fmt.Println(response.Err)
			fmt.Print("minidb> ")
			continue
		}
		xid = response.Xid
		if response.ResultList != nil {
			fmt.Print(response.ResultList)
		}
		fmt.Print("minidb> ")
	}
}
