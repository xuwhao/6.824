package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"time"

	"6.824/logger"
)

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func init() {
	TMPDIR = os.Getenv("TMPDIR")
	if TMPDIR == "" {
		TMPDIR = "./"
	}

	OUTDIR = os.Getenv("OUTDIR")
	if OUTDIR == "" {
		OUTDIR = "./"
	}
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	var err error

	caller := RPCCaller[Task, *Task]{RPCName: GetTask, DebugFmt: "got Task"}
	DoneCaller := RPCCaller[Task, *Task]{RPCName: MarkDone, DebugFmt: "mark done"}

	crash := false // for debuging
	rand.Seed(time.Now().UnixNano())

	doing := true
	for doing {
		emptyArgs, task := Task{}, Task{}
		err = caller.remoteCall(emptyArgs, &task)
		if err != nil {
			continue
		}

		switch task.Type {
		case MAP:
			if crash { // just for testing
				delay := rand.Intn(6) + 1
				logger.Debug(logger.DInfo, "sleep %d s", delay)
				time.Sleep(time.Second * time.Duration(delay))
			}

			err = ExecuteMapTask(mapf, &task)
			if err != nil {
				logger.Debug(logger.DError, "execute map task failed, task %+v, err %w", &task, err)
				continue
			}

			doneTask := Task{}
			err = DoneCaller.remoteCall(task, &doneTask)
			if err != nil {
				continue
			}
		case REDUCE:
			if crash { // just for testing
				delay := rand.Intn(6) + 1
				logger.Debug(logger.DInfo, "sleep %d s", delay)
				time.Sleep(time.Second * time.Duration(delay))
			}

			err = ExecuteReduceTask(reducef, &task)
			if err != nil {
				logger.Debug(logger.DError, "execute reduce task failed, task %+v, err %w", &task, err)
				continue
			}

			doneTask := Task{}
			err = DoneCaller.remoteCall(task, &doneTask)
			if err != nil {
				continue
			}
		case WAITING:
			// todo
			time.Sleep(time.Second * 3)
		case EXIT:
			doing = false
		case UNDEFIDED:
			logger.Debug(logger.DError, "got UNDEFIDED task %+v", &task)
		}
		time.Sleep(time.Second)
	}
	logger.Debug(logger.DInfo, "bye bye!")
}

func ExecuteMapTask(mapf func(string, string) []KeyValue, task *Task) error {

	intermediate := []KeyValue{}

	for _, filename := range task.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			logger.Debug(logger.DError, "map can not open file %+v", file)
			return err
		}
		content, err := io.ReadAll(file)
		if err != nil {
			logger.Debug(logger.DError, "map can not read %+v", file)
			return err
		}
		file.Close()
		kvPairs := mapf(filename, string(content))
		intermediate = append(intermediate, kvPairs...)
	}

	sort.Sort(ByKey(intermediate))

	// buckets[i] contains KeyValues of map-task.Id-i
	buckets := make([][]KeyValue, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		buckets[i] = []KeyValue{}
	}

	// hash KeyValues into corresponding bucket
	for _, kv := range intermediate {
		reduceKey := ihash(kv.Key) % task.NReduce
		buckets[reduceKey] = append(buckets[reduceKey], kv)
	}

	// write buckets into file
	for i := 0; i < task.NReduce; i++ {
		tmpFile, err := ioutil.TempFile(TMPDIR, "mr-tmp-map-")
		if err != nil {
			logger.Debug(logger.DError, "map create temp file failed, i %d", i)
			removePreviousFile(task.Id, i)
			return err
		}

		encoder := json.NewEncoder(tmpFile)
		err = encoder.Encode(&buckets[i])
		if err != nil {
			logger.Debug(logger.DError, "map encode data failed, i %d, data %+v", i, buckets[i])
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			removePreviousFile(task.Id, i)
			return err
		}

		tmpFile.Close()
		err = os.Rename(tmpFile.Name(), fmt.Sprintf(TMPDIR+PREFIX+"%d-%d", task.Id, i))
		if err != nil {
			logger.Debug(logger.DError, "map rename temp file failed, i %d", i)
			os.Remove(tmpFile.Name())
			removePreviousFile(task.Id, i)
			return err
		}
	}

	return nil
}

func removePreviousFile(id TaskID, n int) {
	for i := 0; i < n; i++ {
		os.Remove(fmt.Sprintf(TMPDIR+PREFIX+"%d-%d", id, i))
	}
}

func ExecuteReduceTask(reducef func(string, []string) string, task *Task) error {

	// shuffle
	intermediate := []KeyValue{}
	for i := 0; i < len(task.InputFiles); i++ {
		inputFile, err := os.Open(task.InputFiles[i])
		if err != nil {
			logger.Debug(logger.DError, "reduce can not open file %+v", inputFile)
			return err
		}

		decoder := json.NewDecoder(inputFile)
		for {
			content := []KeyValue{}
			if err := decoder.Decode(&content); err != nil {
				break
			}
			intermediate = append(intermediate, content...)
		}
		inputFile.Close()
	}

	// make intermediate ordered in global scope
	sort.Sort(ByKey(intermediate))

	// call reducef for each key
	outputs := []byte{}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		outputs = append(outputs, []byte(fmt.Sprintf("%v %v\n", intermediate[i].Key, output))...)
		i = j
	}

	// write to file
	ofile, err := ioutil.TempFile(TMPDIR, "mr-tmp-reduce-")
	if err != nil {
		logger.Debug(logger.DError, "reduce create output temp file failed")
		return err
	}

	_, err = ofile.Write(outputs)
	if err != nil {
		logger.Debug(logger.DError, "reduce write output failed")
		return err
	}
	ofile.Close()
	os.Rename(ofile.Name(), fmt.Sprintf(OUTDIR+"mr-out-%d", int(task.Id)-len(task.InputFiles)))

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
