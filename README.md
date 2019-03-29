# crontab
使用go结合ETCD和MongoDB实现的可分布式的定时任务管理系统，主要分master和worker两部分。

master:主要实现可以后台页面对定时任务进行新增、修改、强杀、删除、日志查看、服务发现worker节点。
woerk:可以多个，主要实现监听定时任务的设置及变化，执行定时任务脚本、将执行结果存储到MongoDB中，注册服务到etcd中。
