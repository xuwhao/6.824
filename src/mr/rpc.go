package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"

	"6.824/logger"
)

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
	Type       TaskType
	Id         TaskID
	NReduce    int
	InputFiles []string
	Version    int
}

func (task *Task) String() string {
	return fmt.Sprintf("{%p, {Type: %d, Id: %d, NReduce: %d, InputFiles: %+v, Version: %d}}",
		task, task.Type, task.Id, task.NReduce, task.InputFiles, task.Version)
}

const (
	UNDEFIDED TaskType = iota
	MAP
	REDUCE
	WAITING
	EXIT
)

type RPCCaller[T any, E any] struct {
	RPCName  string
	DebugFmt string
}

func (caller RPCCaller[T, E]) remoteCall(args T, reply E) error {
	err := call(caller.RPCName, args, reply)
	if err == nil {
		logger.Debug(logger.DDebug, caller.DebugFmt+" task[%v]", reply)
	} else {
		logger.Debug(logger.DError, caller.DebugFmt+" err [%w]", err)
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
