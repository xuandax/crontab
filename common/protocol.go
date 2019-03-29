package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

//任务提交表单字段
type Job struct {
	Name     string `json:"name"`     //任务名称
	Command  string `json:"command"`  //shell命令
	CronExpr string `json:"cronExpr"` //cron表达式
}

//返回应答的参数
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

//任务事件
type JobEvent struct {
	EventType int //事件类型
	Job       *Job
}

//任务执行计划
type JobSchedulePlan struct {
	Job      *Job
	Expr     *cronexpr.Expression //解析后的表达式
	NextTime time.Time            //下次执行时间
}

//任务执行信息
type JobExecuteInfo struct {
	Job           *Job
	PlanStartTime time.Time          //计划开始时间
	ExecuteTime   time.Time          //实际执行时间
	CancelCtx     context.Context    //用于取消执行任务
	CancelFunc    context.CancelFunc //取消函数
}

//任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo
	Output      []byte    //执行结果
	Err         error     //错误原因
	StartTime   time.Time //执行开始时间
	EndTime     time.Time //执行结束时间
}

//任务日志
type JobLog struct {
	JobName      string `json:"jobName" bson:"jobName"`           //任务名称
	Command      string `json:"command" bson:"command"`           //执行命令
	Output       string `json:"output" bson:"output"`             //输出结果
	Err          string `json:"err" bson:"err"`                   //错误原因
	PlanTime     int64  `json:"planTime" bson:"planTime"`         //计划调度时间
	ScheduleTime int64  `json:"scheduleTime" bson:"scheduleTime"` //实际调度时间
	StartTime    int64  `json:"startTime" bson:"startTime"`       //执行开始时间
	EndTime      int64  `json:"endTime" bson:"endTime"`           //执行结束时间
}

//日志队列
type LogBatch struct {
	Logs []interface{}
}

//日志过滤条件
type FilterLog struct {
	JobName string `bson:"jobName"`
}

//按照开始时间倒序排列日志
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"` //
}

//构建api的json返回接口
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	var (
		response Response
	)
	response = Response{
		Errno: errno,
		Msg:   msg,
		Data:  data,
	}
	//json序列化
	resp, err = json.Marshal(response)
	return
}

/**
包装Job反序列化
*/
func UnPackJob(value []byte) (workerJob *Job, err error) {
	var (
		job *Job
	)
	job = &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return
	}
	workerJob = job
	return
}

//只获取名称
func GetJobName(jobKey string) (name string) {
	return strings.TrimSuffix(jobKey, JOB_SAVE_DIR)
}

//只获取杀死任务名称
func GetKillName(jobKey string) (name string) {
	return strings.TrimSuffix(jobKey, JOB_KILLER_DIR)
}

/**
构建jobEvent
*/
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

/**
构建任务执行计划
*/
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)

	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

/**
构建任务执行信息
*/
func BuildJobExecuteInfo(plan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:           plan.Job,
		PlanStartTime: plan.NextTime,
		ExecuteTime:   time.Now(),
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}
