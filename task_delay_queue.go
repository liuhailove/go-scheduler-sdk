package gs

//
//type taskNotify struct {
//	e *executor
//}
//
//// DoDelayTask implement Executor interface
//func (tn *taskNotify) DoDelayTask(taskId string) error {
//	tn.e.log.Debug("task in delay queue,taskId=%s", taskId)
//	runningTasks := tn.e.RunningTask()
//	if task, ok := runningTasks[taskId]; ok {
//		res, serverAddr, err := tn.e.post("/api/reportDelay", string(returnReportDelay(task.Id, task.LogId, task.Param.LogDateTime, time.Now().UnixNano()/1e6, tn.e.address)))
//		if err != nil {
//			tn.e.log.Info("report delay error,res=%s,serverAddr=%s,err=%v", res, serverAddr, err)
//			return err
//		}
//		defer res.Body.Close()
//	}
//	return nil
//}
//
//// taskFactory a factory method to build executor
//func taskFactory(taskType string) godelayqueue.Executor {
//	return &taskNotify{e: globalInstance}
//}
