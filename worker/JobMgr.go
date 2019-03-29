package worker

import (
	"context"
	"github.com/xuanxiaox/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

var (
	G_jobMgr *JobMgr
)

type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResp    *clientv3.GetResponse
		kvPair     *mvccpb.KeyValue
		job        *common.Job
		revision   int64
		watchChan  clientv3.WatchChan
		watchResp  clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName    string
		jobEvent   *common.JobEvent
	)
	//获取/cron/jobs/目录下的所有任务，并取得当前集群的revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kvPair = range getResp.Kvs {
		if job, err = common.UnPackJob(kvPair.Value); err != nil {
			err = nil
			continue
		}
		//构建event事件
		jobEvent = common.BuildJobEvent(common.EVENT_SAVE_TYPE, job)
		//推送给Scheduler
		G_scheduler.schedulePush(jobEvent)
	}

	//启用协程从revison往后监听变化
	go func() {
		//获取revison, +1从下一个版本开始监听
		revision = getResp.Header.Revision + 1
		//监听
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(revision), clientv3.WithPrefix())

		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					if job, err = common.UnPackJob(watchEvent.Kv.Value); err != nil {
						continue
					}
					//构建event事件
					jobEvent = common.BuildJobEvent(common.EVENT_SAVE_TYPE, job)
					//推送给Scheduler
					G_scheduler.schedulePush(jobEvent)
				case mvccpb.DELETE:
					//获取任务名
					jobName = common.GetJobName(string(watchEvent.Kv.Key))
					//构建event事件
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.EVENT_DELETE_TYPE, job)
					//推送给Scheduler
					G_scheduler.schedulePush(jobEvent)
				}
			}
		}
	}()
	return
}

func (jobMgr *JobMgr) WatchKills() (err error) {
	var (
		killKey    string
		watchChan  clientv3.WatchChan
		watchResp  clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName    string
		job        *common.Job
		jobEvent   *common.JobEvent
	)
	killKey = common.JOB_KILLER_DIR

	watchChan = jobMgr.client.Watch(context.TODO(), killKey, clientv3.WithPrefix())

	go func() {
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					jobName = common.GetKillName(string(watchEvent.Kv.Key))
					job = &common.Job{
						Name: jobName,
					}
					//创建事件
					jobEvent = common.BuildJobEvent(common.EVENT_KILL_TYPE, job)
					//推送事件
					G_scheduler.schedulePush(jobEvent)
				case mvccpb.DELETE:
				}
			}
		}
	}()
	return
}

func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndPoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout),
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	//启动任务监听
	if err = G_jobMgr.watchJobs(); err != nil {
		return
	}

	//启动强杀监听
	if err = G_jobMgr.WatchKills(); err != nil {
		return
	}
	return
}

//创建一把锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {

	jobLock = InitJobLock(jobMgr.kv, jobMgr.lease, jobName)

	return
}
