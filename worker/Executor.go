package worker

import (
	"github.com/xuanxiaox/crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

type Executor struct {
}

var (
	G_Executor *Executor
)

//执行任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd              *exec.Cmd
			output           []byte
			err              error
			jobExecuteResult *common.JobExecuteResult
			jobLock          *JobLock
		)

		jobExecuteResult = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		//随机睡眠
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		err = jobLock.TryJobLock()
		defer jobLock.JobUnLock()

		if err != nil {
			jobExecuteResult.StartTime = time.Now()
			jobExecuteResult.Err = err
			jobExecuteResult.EndTime = time.Now()
		} else {
			jobExecuteResult.StartTime = time.Now()
			cmd = exec.CommandContext(info.CancelCtx, "D:\\PHP\\Git\\bin\\bash.exe", "-C", info.Job.Command)

			output, err = cmd.Output()

			jobExecuteResult.Err = err
			jobExecuteResult.Output = output
			jobExecuteResult.EndTime = time.Now()
			//执行完成添加到通道通知任务调度
			G_scheduler.PushJobResult(jobExecuteResult)
		}
	}()
}

func InitExecutor() (err error) {
	G_Executor = &Executor{}
	return
}
