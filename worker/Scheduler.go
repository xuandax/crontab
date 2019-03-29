package worker

import (
	"fmt"
	"github.com/xuanxiaox/crontab/common"
	"time"
)

type Scheduler struct {
	jobEvenetChan        chan *common.JobEvent
	jobPlanTable         map[string]*common.JobSchedulePlan
	jobExecuteInfoTable  map[string]*common.JobExecuteInfo
	jobExecuteResultChan chan *common.JobExecuteResult
}

var (
	G_scheduler *Scheduler
)

//处理jobEvent
func (scheduler *Scheduler) handlerJobEvent(jobEvent *common.JobEvent) {
	var (
		err             error
		jobSchedulePlan *common.JobSchedulePlan
		planExsist      bool
		jobExecuteInfo  *common.JobExecuteInfo
		executeExsist   bool
	)
	switch jobEvent.EventType {
	case common.EVENT_SAVE_TYPE: //新增时
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.EVENT_DELETE_TYPE: //删除时
		if jobSchedulePlan, planExsist = scheduler.jobPlanTable[jobEvent.Job.Name]; planExsist {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case common.EVENT_KILL_TYPE:
		if jobExecuteInfo, executeExsist = scheduler.jobExecuteInfoTable[jobEvent.Job.Name]; executeExsist {
			jobExecuteInfo.CancelFunc() //触发command杀死执行任务
		}
	}
}

//处理执行完成后的结果
func (scheduler *Scheduler) handlerJobResult(jobExecuteResult *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	delete(scheduler.jobExecuteInfoTable, jobExecuteResult.ExecuteInfo.Job.Name)

	//构建jobLog
	if jobExecuteResult.Err != common.ERR_LOCK_ALREADY_REQUIRE {
		jobLog = &common.JobLog{
			JobName:      jobExecuteResult.ExecuteInfo.Job.Name,
			Command:      jobExecuteResult.ExecuteInfo.Job.Command,
			Output:       string(jobExecuteResult.Output),
			PlanTime:     jobExecuteResult.ExecuteInfo.PlanStartTime.UnixNano() / 1000 / 1000,
			ScheduleTime: jobExecuteResult.ExecuteInfo.ExecuteTime.UnixNano() / 1000 / 1000,
			StartTime:    jobExecuteResult.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      jobExecuteResult.EndTime.UnixNano() / 1000 / 1000,
		}
		if jobExecuteResult.Err != nil {
			jobLog.Err = jobExecuteResult.Err.Error()
		} else {
			jobLog.Err = ""
		}
	}

	G_logSink.AppendLog(jobLog)

	//fmt.Println("任务执行完成", jobExecuteResult.ExecuteInfo.Job.Name, "|", string(jobExecuteResult.Output), "|", jobExecuteResult.Err, "|", jobExecuteResult.StartTime, "|", jobExecuteResult.EndTime)

}

//调度协程
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent         *common.JobEvent
		scheduleAfter    time.Duration
		scheduleTimer    *time.Timer
		jobExecuteResult *common.JobExecuteResult
	)

	//初始化时计算任务执行时间
	scheduleAfter = scheduler.trySchedule()
	scheduleTimer = time.NewTimer(scheduleAfter)

	for {
		select {
		case jobEvent = <-scheduler.jobEvenetChan:
			scheduler.handlerJobEvent(jobEvent)
		case <-scheduleTimer.C:
		case jobExecuteResult = <-scheduler.jobExecuteResultChan:
			scheduler.handlerJobResult(jobExecuteResult)

		}
		//重新计算
		scheduleAfter = scheduler.trySchedule()
		//重置定时器
		scheduleTimer.Reset(scheduleAfter)

	}
}

//推送到Scheduler
func (scheduler *Scheduler) schedulePush(jobEvent *common.JobEvent) {
	scheduler.jobEvenetChan <- jobEvent
}

//执行完成推送到Scheduler
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobExecuteResultChan <- jobResult
}

//计算任务执行时间
func (scheduler *Scheduler) trySchedule() (schedulerAfter time.Duration) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		nowTime         time.Time
		nearTime        *time.Time
	)

	nowTime = time.Now()

	if len(scheduler.jobPlanTable) == 0 {
		schedulerAfter = 1 * time.Second
		return
	}

	//遍历所有的执行计划
	for _, jobSchedulePlan = range scheduler.jobPlanTable {
		if jobSchedulePlan.NextTime.Before(nowTime) || jobSchedulePlan.NextTime.Equal(nowTime) {
			//尝试执行任务。可能存在上次任务还没执行完
			scheduler.tryStartJob(jobSchedulePlan)
			jobSchedulePlan.NextTime = jobSchedulePlan.Expr.Next(nowTime) //更新下次执行时间
		}

		//获取最近的执行时间
		if nearTime == nil || jobSchedulePlan.NextTime.Before(*nearTime) {
			nearTime = &jobSchedulePlan.NextTime
		}

		//获取时间间隔(最近要执行的时间 - 当前时间)
		schedulerAfter = (*nearTime).Sub(nowTime)
	}

	return
}

func (scheduler *Scheduler) tryStartJob(jobSchedulePlan *common.JobSchedulePlan) {
	var (
		jobExecuteInfo      *common.JobExecuteInfo
		JobExecuteExsisting bool
	)
	//若执行列表中有此任务，跳过
	if jobExecuteInfo, JobExecuteExsisting = scheduler.jobExecuteInfoTable[jobSchedulePlan.Job.Name]; JobExecuteExsisting {
		fmt.Println("执行任务已存在", jobExecuteInfo.Job.Name)
		return
	}

	//生成执行信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobSchedulePlan)

	//将执行任务放入执行列表中
	scheduler.jobExecuteInfoTable[jobSchedulePlan.Job.Name] = jobExecuteInfo

	//执行任务
	G_Executor.ExecuteJob(jobExecuteInfo)

	fmt.Println("执行任务信息结束111", jobExecuteInfo.PlanStartTime, "|", jobExecuteInfo.ExecuteTime)
}

//初始化
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEvenetChan:        make(chan *common.JobEvent, 1000),
		jobPlanTable:         make(map[string]*common.JobSchedulePlan),
		jobExecuteInfoTable:  make(map[string]*common.JobExecuteInfo),
		jobExecuteResultChan: make(chan *common.JobExecuteResult, 1000),
	}
	//启动调度协程
	go G_scheduler.scheduleLoop()

	return
}
