package worker

import (
	"context"
	"fmt"
	"github.com/xuanxiaox/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)

	if client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(G_config.MongodbUri).SetConnectTimeout(time.Duration(G_config.LogCommitTime)*time.Millisecond)); err != nil {
		return
	}

	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, G_config.LogChanLength),
		autoCommitChan: make(chan *common.LogBatch, G_config.LogChanLength),
	}

	go G_logSink.writeLoop()
	return
}

func (logSink *LogSink) writeLoop() {
	var (
		jobLog       *common.JobLog
		batch        *common.LogBatch
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch
	)

	for {
		select {
		case jobLog = <-logSink.logChan:
			//写入mongodb
			if batch == nil {
				//初始化批次
				batch = &common.LogBatch{}
				//设置定时器
				commitTimer = time.AfterFunc(time.Duration(G_config.LogCommitTime)*time.Millisecond,
					func(logBatch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- logBatch
						}
					}(batch),
				)
			}

			batch.Logs = append(batch.Logs, jobLog)

			if len(batch.Logs) == G_config.LogBatchLength {
				//保存日志
				logSink.saveLog(batch)
				//清空批次
				batch = nil
				//停止自动提交
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan:
			//若到时自动提交的batch和原batch不一样,说明在定时到的时候，正好满足规定长度提交了
			if timeoutBatch != batch {
				continue
			}
			//保存日志
			logSink.saveLog(batch)
			//清空批次
			batch = nil
		}
	}
}

func (logSink *LogSink) saveLog(batch *common.LogBatch) {
	var (
		err error
	)

	if _, err = logSink.logCollection.InsertMany(context.TODO(), batch.Logs); err != nil {
		fmt.Println("插入mongoDB失败")
		fmt.Println(err)
	}
}

func (logSink *LogSink) AppendLog(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		//队列满了丢弃
	}
}
