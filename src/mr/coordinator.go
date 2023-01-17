package mr

import (
	"6.824/logger"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Phase int

const (
	ORIGIN   Phase = iota // assign map tasks
	MAPPING               // all map tasks assigned but not done
	MAPPED                // all map tasks have been done, assign reducing tasks
	REDUCING              //all reduce tasks assigned but not done
	REDUCED               // all done
)

const EXPIRE = 2

type TaskContext struct {
	Lock      *sync.RWMutex
	Task      *Task
	TaskPhase Phase
}

func (ctx *TaskContext) String() string {
	return fmt.Sprintf("%p, {Task: %v, TaskPhase: %d", ctx, ctx.Task, ctx.TaskPhase)
}

type Coordinator struct {
	ContextMap *ConcurrentMap[TaskID, *TaskContext]
	Lock       *sync.RWMutex
	Phase      Phase
	NReduce    int
	ReduceID   int
	callbackCh chan func()       // task callback to check if it is timeout or crashed
	assignCh   chan *TaskContext // task callback to check if it is timeout or crashed
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

func (c *Coordinator) AssignTask(args *Task, reply *Task) error {
	c.Lock.RLock()
	switch c.Phase {
	case ORIGIN:
		//todo
		c.assignMapTask(args, reply)
	case MAPPING:
		//todo
		c.assignWaitingTask(args, reply)
	case MAPPED:
		//todo
		c.assignReduceTask(args, reply)
	case REDUCING:
		//todo
		c.assignWaitingTask(args, reply)
	case REDUCED:
		//todo
		c.assignExitTask(args, reply)
	}
	c.Lock.RUnlock()
	return nil
}

func (c *Coordinator) assignExitTask(args *Task, reply *Task) {

}

func (c *Coordinator) assignReduceTask(args *Task, reply *Task) {

}

func (c *Coordinator) assignWaitingTask(args *Task, reply *Task) {

}

func (c *Coordinator) assignMapTask(args *Task, reply *Task) {
	// assign map task
	ctx := <-c.assignCh
	ctx.Lock.Lock()
	ctx.TaskPhase = MAPPING
	*reply = *(ctx.Task)
	logger.Debug(logger.DDebug, "assign map task context [%+v]", ctx)
	ctx.Lock.Unlock()

	// crash test callback for crash handler
	c.callbackCh <- func() {
		timer := time.NewTimer(time.Second * EXPIRE)
		<-timer.C
		ctx.Lock.RLock()

		if ctx.TaskPhase != MAPPED {
			ctx.Lock.RUnlock()

			ctx.Lock.Lock()
			ctx.TaskPhase = ORIGIN
			ctx.Task.Version += 1
			c.assignCh <- ctx
			logger.Debug(logger.DWarn, "task crashed! map task context [%+v]", ctx)
			ctx.Lock.Unlock()
			return
		}
		
		ctx.Lock.RUnlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) crashHandler() {
	go func() {
		for callback := range c.callbackCh {
			go callback()
		}
	}()
}

func NewTaskContext(id TaskID, file string, nReduce int) *TaskContext {
	return &TaskContext{
		Lock: &sync.RWMutex{},
		Task: &Task{
			Type:      MAP,
			Id:        id,
			NReduce:   nReduce,
			InputFile: file,
			Version:   0,
		},
		TaskPhase: ORIGIN,
	}
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	contextMap := &ConcurrentMap[TaskID, *TaskContext]{
		Lock: &sync.RWMutex{},
		Map:  map[TaskID]*TaskContext{}}

	c := Coordinator{
		ContextMap: contextMap,
		Lock:       &sync.RWMutex{},
		Phase:      ORIGIN,
		NReduce:    nReduce,
		ReduceID:   0,
		callbackCh: make(chan func(), len(files)+nReduce),
		assignCh:   make(chan *TaskContext, len(files)+nReduce),
	}

	for idx, file := range files {
		ctx := NewTaskContext(TaskID(idx), file, nReduce) // create map task contexts
		contextMap.Add(TaskID(idx), ctx)                  // add map task contexts to map
		c.assignCh <- ctx                                 // push tasks that unassigned for mapping to channel
	}

	logger.Debug(logger.DInfo, "create Coordinator [%+v], contextMap [%+v]", c, *c.ContextMap)

	c.server()
	c.crashHandler()
	return &c
}
