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
	REDUCED               // all reduce tasks have been done
	DONE
)

const EXPIRE = 5

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
	NMap       int
	ReduceID   int
	Processing int
	callbackCh chan func()       // task callback to check if it is timeout or crashed
	assignCh   chan *TaskContext // task callback to check if it is timeout or crashed
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) MarkDone(args *Task, reply *Task) error {
	ctx, ok := c.ContextMap.Get(args.Id)
	if !ok {
		//todo
	}
	ctx.Lock.Lock() // use X lock because of the lower probability of worker crashed
	defer ctx.Lock.Unlock()

	logger.Debug(logger.DDebug, "try to mark task context done [%v]", ctx)
	if args.Version == ctx.Task.Version { // if not match, task args is assigned to other worker because caller is timeout or crashed
		ctx.TaskPhase++
		c.Lock.Lock()
		c.Processing--
		c.ReduceID++
		logger.Debug(logger.DDebug, "mark done task context [%v], processing [%d], ReduceID [%d], coordinator phase [%d]", ctx, c.Processing, c.ReduceID, c.Phase)
		if c.ReduceID == c.NMap && c.Processing == 0 { // all map tasks done
			c.Phase = MAPPED
			logger.Debug(logger.DDebug, "coordinator enter MAPPED phase, processing [%d], ReduceID [%d]", c.Processing, c.ReduceID)
		} else if c.ReduceID == c.NMap+c.NReduce-1 && c.Processing == 0 { // all reduce tasks done
			c.Phase = REDUCED
			logger.Debug(logger.DDebug, "coordinator enter REDUCED phase, processing [%d], ReduceID [%d]", c.Processing, c.ReduceID)
		}
		c.Lock.Unlock()

		// todo: new a reduce task and add to c.assignCh
	}
	//*reply = *(ctx.Task)
	return nil
}

func (c *Coordinator) AssignTask(args *Task, reply *Task) error {
	c.Lock.RLock()
	logger.Debug(logger.DDebug, "assign task, coordinator phase [%d]", c.Phase)
	switch c.Phase {
	case ORIGIN:
		//todo
		c.Lock.RUnlock()
		c.Lock.Lock()
		if c.Phase == ORIGIN {
			c.assignMapTask(args, reply)
		}
		c.Lock.Unlock()
		return nil
	case MAPPING, REDUCING:
		//todo
		c.assignWaitingTask(args, reply)

	case MAPPED:
		//todo
		//c.assignReduceTask(args, reply)
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
	reply.Id = -1
	reply.Type = WAITING

	logger.Debug(logger.DDebug, "assign waiting task")
}

func (c *Coordinator) assignMapTask(args *Task, reply *Task) {
	// assign map task
	ctx := <-c.assignCh
	ctx.Lock.Lock()
	ctx.TaskPhase = MAPPING
	*reply = *(ctx.Task)
	logger.Debug(logger.DDebug, "assign map task context [%+v], reply [%+v]", ctx, reply)
	ctx.Lock.Unlock()

	c.Processing++
	if c.Processing > 0 && c.Processing+c.ReduceID == c.NMap {
		c.Phase = MAPPING
		logger.Debug(logger.DDebug, "coordinator enter MAPPING phase, processing [%d], ReduceID [%d]", c.Processing, c.ReduceID)
	}

	// crash test callback for crash handler
	c.callbackCh <- func() {
		timer := time.NewTimer(time.Second * EXPIRE)
		<-timer.C
		ctx.Lock.RLock()
		if ctx.TaskPhase != MAPPED {
			ctx.Lock.RUnlock()

			ctx.Lock.Lock()
			if ctx.TaskPhase != MAPPED {
				c.Lock.Lock() // redo when AssignTask return
				if c.Processing < 1 {
					logger.Debug(logger.DError, "task callback minus c.Processing error, processing [%d], ReduceID [%d]", c.Processing, c.ReduceID)
				}
				c.Processing--
				c.Phase = ORIGIN // must not in MAPPING phase because at least one task need to assign
				ctx.TaskPhase = ORIGIN
				ctx.Task.Version += 1
				c.assignCh <- ctx
				logger.Debug(logger.DWarn, "task crashed! map task context [%+v], processing [%d], ReduceID [%d]", ctx, c.Processing, c.ReduceID)
				c.Lock.Unlock()
			}
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
		NMap:       len(files),
		Processing: 0,
		ReduceID:   0, // Phase
		callbackCh: make(chan func(), len(files)+nReduce),
		assignCh:   make(chan *TaskContext, len(files)+nReduce),
	}

	for idx, file := range files {
		ctx := NewTaskContext(TaskID(idx), file, nReduce) // create map task contexts
		contextMap.Add(TaskID(idx), ctx)
		c.assignCh <- ctx //  push unassigned mapping tasks to channel
	}

	logger.Debug(logger.DInfo, "create Coordinator [%+v], contextMap [%+v]", c, *c.ContextMap)

	c.server()
	c.crashHandler()
	return &c
}
