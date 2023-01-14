package mr

import (
	"6.824/logger"
	"hash/fnv"
	"log"
	"net/rpc"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	for true {
		task, err := CallGetTask()
		if err != nil {
			// todo
		}

		switch task.Type {
		case MAP:
		// todo
		case REDUCE:
		//todo
		case WAITING:
		// todo
		case EXIT:
			return
		}
	}
}

func CallGetTask() (Task, error) {
	args := ""
	reply := Task{}

	err := call(GetTask, &args, &reply)

	if err == nil {
		logger.Debug(logger.DDebug, "Got task: %v", reply)
	} else {
		logger.Debug(logger.DError, "get task failed, err: %w, try again later...", err)
		cnt := 2
		for cnt < 10 {
			time.Sleep(time.Second)
			err = call(GetTask, &args, &reply)
			if err != nil {
				logger.Debug(logger.DError, "try %d times, err: %w", cnt, err)
			} else {
				logger.Debug(logger.DDebug, "Got task: %v", reply)
				break
			}
			cnt++
		}

	}

	return reply, err
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {

	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
