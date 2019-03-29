package worker

import (
	"context"
	"github.com/xuanxiaox/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"net"
	"time"
)

type Register struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	localIp string //worker 的ip地址
}

var (
	G_register *Register
)

func InitRegister() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		localIp string
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

	if localIp, err = getLocalIp(); err != nil {
		return
	}

	G_register = &Register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIp: localIp,
	}

	//启动注册
	go G_register.keepOnline()
	return
}

//获取本地ip4地址
func getLocalIp() (ip string, err error) {
	var (
		addrs   []net.Addr
		addr    net.Addr
		ipNet   *net.IPNet
		isIpNet bool
	)
	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}
	for _, addr = range addrs {
		if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ip = ipNet.IP.String()
				return
			}
		}
	}
	err = common.ERR_NET_IP_IS_NOT_EXSIST
	return
}

//worker自动注册到/cron/worker/Ip目录，并自动续租
func (register *Register) keepOnline() {
	var (
		err               error
		workerKey         string
		cancelCtx         context.Context
		cancelFunc        context.CancelFunc
		grantResp         *clientv3.LeaseGrantResponse
		keepAliveRespChan <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp     *clientv3.LeaseKeepAliveResponse
	)

	for {
		workerKey = common.JOB_WORKER_DIR + register.localIp

		cancelFunc = nil

		//授权
		if grantResp, err = register.client.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}

		//自动续租
		if keepAliveRespChan, err = register.client.KeepAlive(context.TODO(), grantResp.ID); err != nil {
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())
		//注册到etcd
		if _, err = register.client.Put(cancelCtx, workerKey, register.localIp, clientv3.WithLease(grantResp.ID)); err != nil {
			goto RETRY
		}

		select {
		case keepAliveResp = <-keepAliveRespChan:
			if keepAliveResp == nil { //若续租失败
				goto RETRY
			}
		}

	RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}
}
