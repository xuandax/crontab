package master

import (
	"encoding/json"
	"github.com/xuanxiaox/crontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

var (
	G_ApiServer *ApiServer
)

func InitApiServer() (err error) {

	var (
		listener          net.Listener
		mux               *http.ServeMux
		httpServer        *http.Server
		staticDir         http.Dir
		staticFileHandler http.Handler
	)

	//启动tcp监听
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}

	//设置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)

	staticDir = http.Dir(G_config.WebRoot)
	staticFileHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticFileHandler))

	//创建http服务
	httpServer = &http.Server{
		WriteTimeout: time.Duration(G_config.WriteTimeout) * time.Millisecond,
		ReadTimeout:  time.Duration(G_config.ReadTimeout) * time.Millisecond,
		Handler:      mux,
	}

	//赋值单例
	G_ApiServer = &ApiServer{
		httpServer: httpServer,
	}

	//启动http服务
	go httpServer.Serve(listener)
	return
}

//获取健康节点列表
func handleWorkerList(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		workers []string
		bytes   []byte
	)

	if workers, err = G_workerMgr.getWorkerList(); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", workers); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, "fail", nil); err == nil {
		w.Write(bytes)
	}
	return
}

func handleJobLog(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		name       string
		skipParam  string
		limitParam string
		skip       int
		limit      int
		jobLogs    []*common.JobLog
		bytes      []byte
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	name = r.Form.Get("name")
	skipParam = r.Form.Get("skip")
	limitParam = r.Form.Get("limit")
	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}

	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 10
	}

	if jobLogs, err = G_logMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", jobLogs); err == nil {
		w.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(-1, "fail", nil); err == nil {
		w.Write(bytes)
	}
	return
}

func handleJobKill(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		err  error
		resp []byte
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	name = r.PostForm.Get("name")

	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}

	if resp, err = common.BuildResponse(0, "success", nil); err == nil {
		w.Write(resp)
	}
	return

ERR:
	if resp, err = common.BuildResponse(-1, "fail", nil); err == nil {
		w.Write(resp)
	}
	return
}

func handleJobList(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		jobList []*common.Job
		resp    []byte
	)

	if jobList, err = G_jobMgr.ListJob(); err != nil {
		goto ERR
	}

	if resp, err = common.BuildResponse(0, "success", jobList); err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err = common.BuildResponse(1, "success", jobList); err == nil {
		w.Write(resp)
	}
	return
}

func handleJobDelete(w http.ResponseWriter, r *http.Request) {
	var (
		name        string
		err         error
		oldJobValue *common.Job
		resp        []byte
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	name = r.PostForm.Get("name")

	if oldJobValue, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}

	if resp, err = common.BuildResponse(0, "success", &oldJobValue); err == nil {
		w.Write(resp)
	}
	return

ERR:
	if resp, err = common.BuildResponse(-1, "fail", nil); err == nil {
		w.Write(resp)
	}
	return
}

func handleJobSave(w http.ResponseWriter, r *http.Request) {
	var (
		job      common.Job
		formData string
		oldJob   *common.Job
		resp     []byte
		err      error
	)
	//解析表单
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	//获取表单信息
	formData = r.PostForm.Get("job")

	//解析序列化表单信息
	if err = json.Unmarshal([]byte(formData), &job); err != nil {
		goto ERR
	}

	//存储到etcd
	if oldJob, err = G_jobMgr.SaveJob(job); err != nil {
		goto ERR
	}

	//应答
	if resp, err = common.BuildResponse(0, "success", oldJob); err == nil {
		w.Write(resp)
	}

	return
ERR:
	//应答
	if resp, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(resp)
	}
	return
}
