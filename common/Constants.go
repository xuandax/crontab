package common

const (
	//任务保存目录
	JOB_SAVE_DIR = "/cron/jobs/"

	//任务杀死目录
	JOB_KILLER_DIR = "/cron/killer/"

	//锁路径
	JOB_LOCK_DIR = "/cron/lock/"

	//事件保存类型
	EVENT_SAVE_TYPE = 1

	//事件删除类型
	EVENT_DELETE_TYPE = 2

	//杀死事件类型
	EVENT_KILL_TYPE = 3

	//worker自动续租目录
	JOB_WORKER_DIR = "/cron/worker/"
)
