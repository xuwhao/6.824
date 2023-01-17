package mr

import (
	"6.824/logger"
	"hash/fnv"
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

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	doing := true
	for doing {
		// get a map/reduce/waiting/exit task
		task := Task{}
		caller := RPCCaller[*Task, *Task]{RPCName: GetTask, Args: &task, Reply: &task, DebugFmt: "got Task"}
		err := caller.remoteCall()
		if err != nil {
			// todo
		}
		switch task.Type {
		case MAP:
			// todo
			err := ExecuteMapTask(mapf, &task)
			if err != nil {
				// todo
			}
			logger.Debug(logger.DInfo, "Execute map task successfully!")
			doing = false
		case REDUCE:
			//todo
			err := ExecuteReduceTask(reducef, &task)
			if err != nil {
				// todo
			}
			logger.Debug(logger.DInfo, "Execute reduce task successfully!")
			doing = false
		case WAITING:
			// todo
			time.Sleep(time.Second)
		case EXIT:
			return
		}
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
