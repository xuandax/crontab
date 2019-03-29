package master

import (
	"context"
	"github.com/xuanxiaox/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_workerMgr *WorkerMgr
)

func InitWorkerMgr() (err error) {
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

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_workerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

//获取所有健康worker的IP
func (workerMgr *WorkerMgr) getWorkerList() (workers []string, err error) {
	var (
		workerKey string
		getResp   *clientv3.GetResponse
		keyValue  *mvccpb.KeyValue
	)
	workerKey = common.JOB_WORKER_DIR
	if getResp, err = workerMgr.client.Get(context.TODO(), workerKey, clientv3.WithPrefix()); err != nil {
		return
	}

	workers = make([]string, 0)

	if len(getResp.Kvs) > 0 {
		for _, keyValue = range getResp.Kvs {
			workers = append(workers, string(keyValue.Value))
		}
	}
	return
}
