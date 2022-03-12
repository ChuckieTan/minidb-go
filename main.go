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

// func main() {
// 	// 创建数据库
// 	db := tbm.Create("test.db")
// 	// 创建表
// 	table := db.CreateTable("user")
// 	// 创建索引
// 	index := table.CreateIndex("idx_name", "name")
// 	// 创建索引
// 	index = table.CreateIndex("idx_age", "age")
// 	// 创建索引
// 	index = table.CreateIndex("idx_score", "score")
// 	// 创建索引
// 	index = table.CreateIndex("idx_score_age", "score", "age")
// 	// 创建索引
// 	index = table.CreateIndex("idx_score_age_name", "score", "age", "name")

// 	// 插入数据
// 	table.Insert(1, "张三", 18, 100.0)
// 	table.Insert(2, "李四", 19, 99.0)
// 	table.Insert(3, "王五", 20, 98.0)
// 	table.Insert(4, "赵六", 21, 97.0)
// 	table.Insert(5, "钱七", 22, 96.0)
// 	table.Insert(6, "孙八", 23, 95.0)
// 	table.Insert(7, "周九", 24, 94.0)
// 	table.Insert(8, "吴十", 25, 93.0)
// 	table.Insert(9, "郑十一", 26, 92.0)
// 	table.Insert(10, "王十二", 27, 91.0)
// 	table.Insert(11, "李十三", 28, 90.0)
// 	table.Insert(12, "张十四", 29, 89.0)
// 	table.Insert(13, "李十五", 30, 88.0)
// }

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
