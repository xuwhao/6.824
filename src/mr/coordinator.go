package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	"6.824/logger"
)

/* ------------------------------- Data Structures START ------------------------------- */

const EXPIRE = 10 // reassign tasks after EXPIRE seconds
const PREFIX = "mr-"

var TMPDIR string = "./"
var OUTDIR string = "./"

type TaskID int

type TaskType int

const (
	UNDEFIDED TaskType = iota
	MAP
	REDUCE
	WAITING
	EXIT
)

type Phase int

const (
	ORIGIN   Phase = iota // assign map tasks
	MAPPING               // all map tasks assigned but not done
	MAPPED                // all map tasks have been done, assign reducing tasks
	REDUCING              //all reduce tasks assigned but not done
	REDUCED               // all reduce tasks have been done
	DONE
)

type TaskContext struct {
	Lock      *sync.RWMutex
	Task      *Task
	TaskPhase Phase
}

func (ctx *TaskContext) String() string {
	return fmt.Sprintf("{%p, {Task: %v, TaskPhase: %d}", ctx, ctx.Task, ctx.TaskPhase)
}

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

type ConcurrentMap[T comparable, E any] struct {
	Lock *sync.RWMutex
	Map  map[T]E
}

func (c *ConcurrentMap[T, E]) Add(key T, value E) {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	c.Map[key] = value
}

func (c *ConcurrentMap[T, E]) Remove(key T) {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	delete(c.Map, key)
}
func (c *ConcurrentMap[T, E]) Get(key T) (E, bool) {
	c.Lock.RLock()
	defer c.Lock.RUnlock()
	v, ok := c.Map[key]
	return v, ok
}

func (c *ConcurrentMap[T, E]) String() string {
	var build strings.Builder
	build.WriteString("{")
	c.Lock.RLock()
	for k, v := range c.Map {
		build.WriteString(fmt.Sprintf("{%+v: %+v}, ", k, v))
	}
	c.Lock.RUnlock()
	build.WriteString("}")
	return build.String()
}

/* ------------------------------ Data Structures END ------------------------------ */

/* ------------------------------- Coordinator START ------------------------------- */

type Coordinator struct {
	ContextMap      *ConcurrentMap[TaskID, *TaskContext]
	Lock            *sync.RWMutex
	Phase           Phase
	NReduce         int
	NMap            int
	ReduceID        int
	DoneCnt         int
	Processing      int
	callbackChannel chan func()       // task callback to check if it is timeout or crashed
	taskChannel     chan *TaskContext // unassigned map tasks
	//reduceTaskChannel chan *TaskContext // unassigned reduce tasks
}

func (c *Coordinator) MarkDone(args Task, reply *Task) error {
	ctx, _ := c.ContextMap.Get(args.Id)

	ctx.Lock.Lock() // use X lock because of the lower probability of worker crashed
	defer ctx.Lock.Unlock()

	logger.Debug(logger.DDebug, "try to mark task context done, args %+v, ctx %+v", args, ctx)
	if args.Version == ctx.Task.Version { // if not match, task args is assigned to other worker because caller is timeout or crashed
		ctx.TaskPhase++
		c.Lock.Lock()
		c.Processing--
		if args.Type == MAP {
			c.ReduceID++
			logger.Debug(logger.DDebug, "mark done map task context [%v], processing %d, ReduceID %d, coordinator phase %d", ctx, c.Processing, c.ReduceID, c.Phase)
			if c.ReduceID == c.NMap && c.Processing == 0 { // all map tasks done
				c.Phase = MAPPED
				logger.Debug(logger.DDebug, "coordinator enter MAPPED phase, processing %d, ReduceID %d", c.Processing, c.ReduceID)

				// make reduce tasks that added to c.taskChannel and contextMap
				for i := 0; i < c.NReduce; i++ {
					files := []string{}
					for j := 0; j < c.NMap; j++ {
						file := fmt.Sprintf(TMPDIR+PREFIX+"%d-%d", j, i)
						files = append(files, file)
					}
					tk := NewTaskContext(REDUCE, TaskID(c.ReduceID), files, c.NReduce, MAPPED)
					c.ContextMap.Add(TaskID(c.ReduceID), tk)
					c.taskChannel <- tk
					c.ReduceID++
				}
			}
		} else {
			c.DoneCnt++
			logger.Debug(logger.DDebug, "mark done reduce task context [%v], processing %d, DoneCnt %d, coordinator phase %d", ctx, c.Processing, c.DoneCnt, c.Phase)
			if c.DoneCnt == c.NReduce && c.Processing == 0 { // all reduce tasks done
				c.Phase = REDUCED
				logger.Debug(logger.DDebug, "coordinator enter REDUCED phase, processing %d, ReduceID %d", c.Processing, c.ReduceID)
			}
		}
		c.Lock.Unlock()
	}
	*reply = *(ctx.Task)
	return nil
}

func (c *Coordinator) AssignTask(_ Task, reply *Task) error {
	c.Lock.RLock()
	logger.Debug(logger.DDebug, "assign task, coordinator phase %d", c.Phase)

	switch c.Phase {
	case ORIGIN:
		c.Lock.RUnlock()

		c.Lock.Lock()
		if c.Phase == ORIGIN {
			c.assignMRTask(reply, MAPPING, "map")
			if c.Processing > 0 && c.Processing+c.ReduceID == c.NMap { // All map tasks were assigned but not all were completed
				c.Phase = MAPPING
				logger.Debug(logger.DDebug, "coordinator enter MAPPING phase, processing %d, ReduceID %d", c.Processing, c.ReduceID)
			}
		}
		c.Lock.Unlock()
		return nil

	case MAPPED:
		c.Lock.RUnlock()

		c.Lock.Lock()
		if c.Phase == MAPPED {
			c.assignMRTask(reply, REDUCING, "reduce")
			if c.Processing > 0 && c.Processing+c.DoneCnt == c.NReduce { // All reduce tasks were assigned but not all were completed
				c.Phase = REDUCING
				logger.Debug(logger.DDebug, "coordinator enter REDUCING phase, processing %d, DoneCnt %d", c.Processing, c.DoneCnt)
			}
		}
		c.Lock.Unlock()
		return nil

	case MAPPING, REDUCING:
		reply.Type = WAITING
		reply.Id = -1
		logger.Debug(logger.DDebug, "assign WAITING task, %+v", reply)
	case REDUCED:
		reply.Type = EXIT
		reply.Id = -2
		logger.Debug(logger.DDebug, "assign EXIT task, %+v", reply)
	}

	c.Lock.RUnlock()
	return nil
}

func (c *Coordinator) assignMRTask(reply *Task, taskPhase Phase, prompt string) {

	ctx := <-c.taskChannel

	ctx.Lock.Lock()
	ctx.TaskPhase = taskPhase
	*reply = *(ctx.Task)
	logger.Debug(logger.DDebug, "assign "+prompt+" task context %+v, reply %+v", ctx, reply)
	ctx.Lock.Unlock()

	c.Processing++

	// crash test callback for crash handler
	c.callbackChannel <- func() {
		timer := time.NewTimer(time.Second * EXPIRE)
		<-timer.C
		ctx.Lock.RLock()
		if ctx.TaskPhase != taskPhase+1 {
			ctx.Lock.RUnlock()

			ctx.Lock.Lock()
			if ctx.TaskPhase != taskPhase+1 { // task is not done
				c.Lock.Lock() // redo when AssignTask return
				c.Processing--
				if c.Processing < 0 {
					logger.Debug(logger.DError, "task callback "+prompt+", c.Processing less than 0, processing %d, ReduceID %d, DoneCnt %d", c.Processing, c.ReduceID, c.DoneCnt)
				}
				c.Phase = taskPhase - 1 // MAPPING - 1 = ORIGIN, REDUCEING -1 = MAPPED
				ctx.TaskPhase = taskPhase - 1
				ctx.Task.Version += 1
				c.taskChannel <- ctx
				logger.Debug(logger.DWarn, "task crashed! "+prompt+" task context %+v, processing %d, ReduceID %d, DoneCnt %d", ctx, c.Processing, c.ReduceID, c.DoneCnt)
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
	c.Lock.RLock()
	defer c.Lock.RUnlock()

	if c.Phase == REDUCED {
		ret = true
		logger.Debug(logger.DInfo, "MapReduce finished")
	}

	return ret
}

// crashHandler start another goroutine to detect a crash or tiemout worker
func (c *Coordinator) crashHandler() {
	go func() {
		for callback := range c.callbackChannel {
			go callback()
		}
	}()
}

/* ------------------------------- Coordinator END ------------------------------- */

/* ------------------------------ Util Functions START --------------------------- */

func NewTaskContext(taskType TaskType, id TaskID, files []string, nReduce int, taskPhase Phase) *TaskContext {
	return &TaskContext{
		Lock: &sync.RWMutex{},
		Task: &Task{
			Type:       taskType,
			Id:         id,
			NReduce:    nReduce,
			InputFiles: files,
			Version:    0,
		},
		TaskPhase: taskPhase,
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
		ContextMap:      contextMap,
		Lock:            &sync.RWMutex{},
		Phase:           ORIGIN,
		NReduce:         nReduce,
		NMap:            len(files),
		Processing:      0,
		ReduceID:        0, // when all map task finished, ReduceID becomes the beginning id of the first reduce task
		DoneCnt:         0,
		callbackChannel: make(chan func(), len(files)+nReduce),
		taskChannel:     make(chan *TaskContext, len(files)+nReduce),
	}

	for idx, file := range files {
		ctx := NewTaskContext(MAP, TaskID(idx), []string{file}, nReduce, ORIGIN)
		contextMap.Add(TaskID(idx), ctx)
		c.taskChannel <- ctx //  push unassigned mapping tasks to channel
	}

	logger.Debug(logger.DInfo, "create Coordinator %+v, contextMap %+v", c, c.ContextMap)

	c.server()
	c.crashHandler()
	return &c
}

/* ----------------------------- Util Functions END --------------------------- */
