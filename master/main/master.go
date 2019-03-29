package main

import (
	"flag"
	"fmt"
	"github.com/xuanxiaox/crontab/master"
	"runtime"
	"time"
)

var (
	filePath string
)

//解析命令行参数
func InitArgs() {
	flag.StringVar(&filePath, "config", "src/github.com/xuanxiaox/crontab/master/main/master.json", "设置配置文件")
}

//设置多线程
func InitPro() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {

	var (
		err error
	)

	//
	InitArgs()

	//设置多线程
	InitPro()

	//初始化配置
	if err = master.InitConfig(filePath); err != nil {
		goto ERR
	}

	//发现worker
	if err = master.InitWorkerMgr(); err != nil {
		goto ERR
	}
	//初始化日志管理器
	if err = master.InitLogMgr(); err != nil {
		goto ERR
	}

	//初始化管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	//启动http服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	fmt.Println(master.G_config)

	for {
		time.Sleep(1 * time.Second)
	}

ERR:
	fmt.Println(err)
}
