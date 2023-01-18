package mr

import (
	"6.824/logger"
	"hash/fnv"
	"math/rand"
	"time"
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
			// todo
		}

		switch task.Type {
		case MAP:
			// todo
			//delay := rand.Intn(6)
			//logger.Debug(logger.DInfo, "sleep %d s", delay)
			//time.Sleep(time.Second * 10)
			err = ExecuteMapTask(mapf, &task)
			if err != nil {
				// todo
			}

			doneTask := Task{}
			err = DoneCaller.remoteCall(task, &doneTask)
			if err != nil {
				// todo
			}

			//logger.Debug(logger.DInfo, "Execute map task successfully!")
			//return
		case REDUCE:
			//todo
			err = ExecuteReduceTask(reducef, &task)
			if err != nil {
				// todo
			}
			logger.Debug(logger.DInfo, "Execute reduce task successfully!")
		case WAITING:
			// todo
			time.Sleep(time.Second)
		case EXIT:
			doing = false
		}
		time.Sleep(time.Second)
	}
}

func ExecuteMapTask(mapf func(string, string) []KeyValue, task *Task) error {
	return nil
}

func ExecuteReduceTask(reducef func(string, []string) string, task *Task) error {
	return nil
}

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
