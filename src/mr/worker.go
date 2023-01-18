package mr

import (
	"hash/fnv"
	"math/rand"
	"time"

	"6.824/logger"
)

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	var err error

	caller := RPCCaller[Task, *Task]{RPCName: GetTask, DebugFmt: "got Task"}
	DoneCaller := RPCCaller[Task, *Task]{RPCName: MarkDone, DebugFmt: "mark done"}

	rand.Seed(time.Now().UnixNano())
	doing := true
	for doing {
		emptyArgs, task := Task{}, Task{}
		err = caller.remoteCall(emptyArgs, &task)
		if err != nil {
			logger.Debug(logger.DError, "rpc call failed, maybe coordinator exited, bye bye!")
		}

		switch task.Type {
		case MAP:
			// todo
			delay := rand.Intn(6) + 1
			logger.Debug(logger.DInfo, "sleep %d s", delay)
			time.Sleep(time.Second * time.Duration(delay))
			err = ExecuteMapTask(mapf, &task)
			if err != nil {
				//todo
			}

			doneTask := Task{}
			err = DoneCaller.remoteCall(task, &doneTask)
			if err != nil {
				logger.Debug(logger.DError, "done rpc call failed, maybe coordinator exited, bye bye!")
			}
			//logger.Debug(logger.DInfo, "Execute map task successfully!")
		case REDUCE:
			//todo
			delay := rand.Intn(6) + 1
			logger.Debug(logger.DInfo, "sleep %d s", delay)
			time.Sleep(time.Second * time.Duration(delay))
			err = ExecuteReduceTask(reducef, &task)
			if err != nil {

			}

			doneTask := Task{}
			err = DoneCaller.remoteCall(task, &doneTask)
			if err != nil {
				logger.Debug(logger.DError, "done rpc call failed, maybe coordinator exited, bye bye!")
			}
			// logger.Debug(logger.DInfo, "Execute reduce task successfully!")
		case WAITING:
			// todo
			time.Sleep(time.Second)
		case EXIT:
			doing = false
		case UNDEFIDED:
			logger.Debug(logger.DError, "got UNDEFIDED task %+v", task)
		}
		time.Sleep(time.Second)
	}
	logger.Debug(logger.DInfo, "bye bye!")
}

func ExecuteMapTask(mapf func(string, string) []KeyValue, task *Task) error {
	return nil
}

func ExecuteReduceTask(reducef func(string, []string) string, task *Task) error {
	return nil
}

// need to test
/*
1. seq(1 worker)
2. seq crash
3. curr
4. curr crash
*/

//func () remoteCall(context RPCCaller[_, _]) error {
//	rpcName, args, reply, cnt := context.RPCName, context.Args, context.Reply, 0
//	var err error
//
//	for true {
//		if cnt == 10 {
//			break
//		}
//		err = call(rpcName, args, reply)
//		if err == nil {
//			logger.Debug(logger.DDebug, context.DebugFmt+" [%v]", reply)
//			break
//		} else {
//			cnt++
//			logger.Debug(logger.DError, context.DebugFmt+" %d times failed, err [%w]", cnt, err)
//			time.Sleep(time.Second)
//		}
//	}
//
//	return err
//}

//func remoteCall(context RPCCaller[any, any, any, any]) (*Task, error) {
//	args, reply, cnt := context.Args, context.Reply, 0
//	var err error
//
//	for true {
//		if cnt == 10 {
//			break
//		}
//		err = call(GetTask, &args, &reply)
//		if err == nil {
//			logger.Debug(logger.DDebug, "Got task [%v]", reply)
//			break
//		} else {
//			cnt++
//			logger.Debug(logger.DError, "get task failed [%d] times, err [%w]", cnt, err)
//			time.Sleep(time.Second)
//		}
//	}
//
//	return &reply, err
//}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
