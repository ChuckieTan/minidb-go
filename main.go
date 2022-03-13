package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"minidb-go/client"
	"minidb-go/parser/ast"
	"minidb-go/server"
	"minidb-go/storage/bplustree"
	"minidb-go/tbm"
	"minidb-go/util"
	"os"

	log "github.com/sirupsen/logrus"
)

// 设置 log 输出格式
func init() {
	//设置output,默认为stderr,可以为任何io.Writer，比如文件*os.File
	log.SetOutput(os.Stdout)
	//设置最低loglevel
	log.SetLevel(log.InfoLevel)
	log.SetReportCaller(true)
	log.SetFormatter(&util.MyFormatter{})

	// 注册 gob 接口类型
	gob.Register(bplustree.BPlusTree{})
	gob.Register(ast.SQLInt(0))
	gob.Register(ast.SQLFloat(0))
	gob.Register(ast.SQLText(""))
	gob.Register(ast.SQLColumn(""))
}

func main() {
	isServer := flag.Bool("server", false, "run as server")
	isClient := flag.Bool("client", false, "run as client")
	isCreate := flag.Bool("create", false, "create database")
	isOpen := flag.Bool("open", false, "open database")
	path := flag.String("path", "", "database path")
	flag.Parse()

	if *isServer == *isClient {
		log.Fatal("server and client can't be both true")
	}

	if *isServer {
		if *isCreate == *isOpen {
			log.Fatal("create and open can't be both true")
		}

		log.Info("run as server")
		var tableManager *tbm.TableManager
		defer func() {
			if tableManager != nil {
				tableManager.Close()
			}
		}()
		if *isCreate {
			log.Info("create database")
			tableManager = tbm.Create(*path)
		} else if *isOpen {
			log.Info("open database")
			tableManager = tbm.Open(*path)
		} else {
			log.Error("create or open database")
			return
		}

		server := server.NewServer(tableManager)
		// 启动服务器
		go server.Start()
		// 等待退出
		WaitForExit()
	} else if *isClient {
		log.Info("run as client")
		client := client.NewClient()
		client.Start()
	} else {
		log.Error("run as server or client")
		return
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
