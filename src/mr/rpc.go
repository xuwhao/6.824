package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"6.824/logger"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const RPCFunctionPrefix = "Coordinator"
const (
	GetTask  string = RPCFunctionPrefix + ".AssignTask"
	MarkDone string = RPCFunctionPrefix + ".MarkDone"
)

type TaskType int
type TaskID int
type Task struct {
	Type      TaskType
	Id        TaskID
	NReduce   int
	InputFile string
	Version   int
}

func (task *Task) String() string {
	return fmt.Sprintf("%p, {Type: %d, Id: %d, NReduce: %d, InputFile: %s, Version: %d}",
		task, task.Type, task.Id, task.NReduce, task.InputFile, task.Version)
}

const (
	MAP TaskType = iota
	REDUCE
	WAITING
	EXIT
)

type RPCCaller[T any, E any] struct {
	RPCName  string
	Args     T
	Reply    E
	DebugFmt string
}

func (caller RPCCaller[T, E]) remoteCall() error {
	rpcName, args, reply, cnt := caller.RPCName, caller.Args, caller.Reply, 0
	var err error

	for true {
		if cnt == 10 {
			break
		}
		err = call(rpcName, args, reply)
		if err == nil {
			logger.Debug(logger.DDebug, caller.DebugFmt+" task[%v]", reply)
			break
		} else {
			cnt++
			logger.Debug(logger.DError, caller.DebugFmt+" %d times failed, err [%w]", cnt, err)
			time.Sleep(time.Second)
		}
	}

	return err
}

func call(rpcname string, args interface{}, reply interface{}) error {

	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
