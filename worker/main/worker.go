package main

import (
	"flag"
	"fmt"
	"github.com/xuanxiaox/crontab/worker"
	"runtime"
	"time"
)

var fileName string

func InitCPUNUM() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func InitParams() {
	flag.StringVar(&fileName, "config", "src/github.com/xuanxiaox/crontab/worker/main/worker.json", "worker配置文件")
	flag.Parse()
}

func main() {
	var (
		err error
	)
	//设置可同时执行的最大CPU数
	InitCPUNUM()

	//初始化参数
	InitParams()

	//初始哈配置文件
	if err = worker.InitConfig(fileName); err != nil {
		goto ERR
	}

	//注册worker到ETCD
	if err = worker.InitRegister(); err != nil {
		goto ERR
	}

	//初始化日志处理协程
	if err = worker.InitLogSink(); err != nil {
		goto ERR
	}

	//初始化任务执行器
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}

	//初始化任务调度
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}

	//初始化任务管理器
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}
	return

ERR:
	fmt.Println(123)
	fmt.Println(err)
}
