package master

import (
	"context"
	"encoding/json"
	"github.com/xuanxiaox/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_jobMgr *JobMgr
)

//初始化管理器
func InitJobMgr() (err error) {

	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndPoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	//连接etcd
	if client, err = clientv3.New(config); err != nil {
		return
	}

	//创建KV
	kv = clientv3.NewKV(client)

	//创建lease
	lease = clientv3.NewLease(client)

	//赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}

func (jobMgr *JobMgr) KillJob(name string) (err error) {
	var (
		killerKey string
		grantResp *clientv3.LeaseGrantResponse
		leaseId   clientv3.LeaseID
	)

	killerKey = common.JOB_KILLER_DIR + name

	if grantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	leaseId = grantResp.ID

	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return
}

func (jobMgr *JobMgr) ListJob() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		job     *common.Job
	)

	dirKey = common.JOB_SAVE_DIR

	if getResp, err = jobMgr.client.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	jobList = make([]*common.Job, 0)

	for _, kvPair = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey      string
		deleteResp  *clientv3.DeleteResponse
		oldJobValue common.Job
	)

	jobKey = common.JOB_SAVE_DIR + name

	if deleteResp, err = jobMgr.client.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	if len(deleteResp.PrevKvs) != 0 {
		if err = json.Unmarshal(deleteResp.PrevKvs[0].Value, &oldJobValue); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobValue
	}
	return
}

func (jobMgr *JobMgr) SaveJob(job common.Job) (oldJob *common.Job, err error) {
	var (
		jobKey      string
		jobValue    []byte
		putResp     *clientv3.PutResponse
		oldJobValue common.Job
	)
	//设置etcd的key
	jobKey = common.JOB_SAVE_DIR + job.Name
	//json序列化
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	//将数据写入etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	//判断写入之前当前key是否有值
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobValue); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobValue
	}

	return
}
