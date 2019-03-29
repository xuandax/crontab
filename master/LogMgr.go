package master

import (
	"context"
	"fmt"
	"github.com/xuanxiaox/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)

	if client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(G_config.MongodbUri).SetConnectTimeout(time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)); err != nil {
		return
	}

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

func (logMgr *LogMgr) ListLog(jobName string, skip int, limit int) (jobLogs []*common.JobLog, err error) {
	var (
		filter    *common.FilterLog
		sortOrder *common.SortLogByStartTime
		cursor    *mongo.Cursor
		jobLog    *common.JobLog
	)

	filter = &common.FilterLog{
		JobName: jobName,
	}

	sortOrder = &common.SortLogByStartTime{
		SortOrder: -1,
	}

	//先初始化出来，防止没数据报错
	jobLogs = make([]*common.JobLog, 0)

	//获取游标
	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, options.Find().SetSkip(int64(skip)).SetLimit(int64(limit)).SetSort(sortOrder)); err != nil {
		fmt.Println(err)
		return
	}

	//延迟释放游标
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}

		//反序列化
		if err = cursor.Decode(jobLog); err != nil {
			continue
		}
		jobLogs = append(jobLogs, jobLog)
	}
	return
}
