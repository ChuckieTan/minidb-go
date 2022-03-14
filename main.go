package main

import (
	"encoding/gob"
	"flag"
	"minidb-go/client"
	"minidb-go/parser/ast"
	"minidb-go/server"
	"minidb-go/storage/bplustree"
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
	sqlInt := ast.SQLInt(0)
	sqlFloat := ast.SQLFloat(0)
	sqlText := ast.SQLText("")
	sqlColumn := ast.SQLColumn("")
	gob.RegisterName("minidb-go/parser/ast.SQLInt", &sqlInt)
	gob.RegisterName("minidb-go/parser/ast.SQLFloat", &sqlFloat)
	gob.RegisterName("minidb-go/parser/ast.SQLText", &sqlText)
	gob.RegisterName("minidb-go/parser/ast.SQLColumn", &sqlColumn)
}

func main() {
	isServer := flag.Bool("server", false, "run as server")
	isClient := flag.Bool("client", false, "run as client")
	isCreate := flag.Bool("create", false, "create database")
	isOpen := flag.Bool("open", false, "open database")
	path := flag.String("path", "", "database path")
	flag.Parse()

	if *isServer && *isClient {
		log.Fatal("server and client can't be both true")
	}

	if *isServer {
		log.Info("run as server")
		server := server.NewServer(*isOpen, *isCreate, *path)
		server.Start()
	} else if *isClient {
		log.Info("run as client")
		client := client.NewClient()
		client.Start()
	} else {
		log.Fatal("run as server or client")
	}
}
